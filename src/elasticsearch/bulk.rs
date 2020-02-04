use crate::elasticsearch::Elasticsearch;
use crate::json::builder::JsonBuilder;
use pgx::*;
use serde::Deserialize;
use serde_json::{json, Value};
use std::any::Any;
use std::io::{Error, ErrorKind, Read, Write};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

#[derive(Debug)]
pub enum BulkRequestCommand {
    Insert {
        ctid: u64,
        cmin: pg_sys::CommandId,
        cmax: pg_sys::CommandId,
        xmin: u64,
        xmax: u64,
        builder: JsonBuilder,
    },
    Update {
        ctid: u64,
        cmax: pg_sys::CommandId,
        xmax: u64,
        builder: JsonBuilder,
    },
    DeleteByXmin {
        ctid: u64,
        xmin: u64,
    },
    DeleteByXmax {
        ctid: u64,
        xmax: u64,
    },
    Interrupt,
    Done,
}

#[derive(Debug)]
pub enum BulkRequestError {
    IndexingError(String),
    NoError,
}

pub struct ElasticsearchBulkRequest<'a> {
    handler: Handler<'a>,
    error_receiver: crossbeam::channel::Receiver<BulkRequestError>,
}

impl<'a> ElasticsearchBulkRequest<'a> {
    pub fn new(elasticsearch: Elasticsearch<'a>, queue_size: usize, concurrency: usize) -> Self {
        let (etx, erx) = crossbeam::channel::bounded(queue_size * concurrency);

        ElasticsearchBulkRequest {
            handler: Handler::new(elasticsearch, concurrency, etx),
            error_receiver: erx,
        }
    }

    pub fn wait_for_completion(self) -> Result<usize, BulkRequestError> {
        // wait for the bulk requests to finish
        self.handler.wait_for_completion()
    }

    pub fn terminate(
        &self,
    ) -> impl Fn() + std::panic::UnwindSafe + std::panic::RefUnwindSafe + 'static {
        let terminate = self.handler.terminatd.clone();
        move || {
            terminate.store(true, Ordering::SeqCst);
            info!("terminating process");
        }
    }

    pub fn insert(
        &mut self,
        ctid: pg_sys::ItemPointerData,
        cmin: pg_sys::CommandId,
        cmax: pg_sys::CommandId,
        xmin: u64,
        xmax: u64,
        builder: JsonBuilder,
    ) -> Result<(), crossbeam::SendError<BulkRequestCommand>> {
        self.check_for_error();

        self.handler.queue_command(BulkRequestCommand::Insert {
            ctid: item_pointer_to_u64(ctid),
            cmin,
            cmax,
            xmin,
            xmax,
            builder,
        })
    }

    pub fn update(
        &mut self,
        ctid: pg_sys::ItemPointerData,
        cmax: pg_sys::CommandId,
        xmax: u64,
        builder: JsonBuilder,
    ) -> Result<(), crossbeam::SendError<BulkRequestCommand>> {
        self.check_for_error();

        self.handler.queue_command(BulkRequestCommand::Update {
            ctid: item_pointer_to_u64(ctid),
            cmax,
            xmax,
            builder,
        })
    }

    pub fn delete_by_xmin(
        &mut self,
        ctid: pg_sys::ItemPointerData,
        xmin: u64,
    ) -> Result<(), crossbeam::SendError<BulkRequestCommand>> {
        self.check_for_error();

        self.handler
            .queue_command(BulkRequestCommand::DeleteByXmin {
                ctid: item_pointer_to_u64(ctid),
                xmin,
            })
    }

    pub fn delete_by_xmax(
        &mut self,
        ctid: pg_sys::ItemPointerData,
        xmax: u64,
    ) -> Result<(), crossbeam::SendError<BulkRequestCommand>> {
        self.check_for_error();

        self.handler
            .queue_command(BulkRequestCommand::DeleteByXmax {
                ctid: item_pointer_to_u64(ctid),
                xmax,
            })
    }

    #[inline]
    fn check_for_error(&mut self) {
        // do we have an error queued up?
        match self
            .error_receiver
            .try_recv()
            .unwrap_or(BulkRequestError::NoError)
        {
            BulkRequestError::IndexingError(err_string) => {
                self.handler.terminate();
                panic!("{}", err_string);
            }
            BulkRequestError::NoError => {}
        }

        if interrupt_pending() {
            self.handler.terminate();
            check_for_interrupts!();
        }
    }
}

const BULK_FILTER_PATH: &str = "errors,items.index.error.caused_by.reason";

pub(crate) struct Handler<'a> {
    pub(crate) terminatd: Arc<AtomicBool>,
    threads: Vec<JoinHandle<usize>>,
    active_thread_cnt: Arc<AtomicUsize>,
    in_flight: Arc<AtomicUsize>,
    total_docs: usize,
    elasticsearch: Elasticsearch<'a>,
    concurrency: usize,
    bulk_sender: crossbeam::channel::Sender<BulkRequestCommand>,
    bulk_receiver: crossbeam::channel::Receiver<BulkRequestCommand>,
    error_sender: crossbeam::channel::Sender<BulkRequestError>,
}

struct BulkReceiver {
    terminated: Arc<AtomicBool>,
    first: Option<BulkRequestCommand>,
    in_flight: Arc<AtomicUsize>,
    receiver: crossbeam::channel::Receiver<BulkRequestCommand>,
    bytes_out: usize,
    docs_out: Arc<AtomicUsize>,
    buffer: Vec<u8>,
}

impl std::io::Read for BulkReceiver {
    fn read(&mut self, mut buf: &mut [u8]) -> Result<usize, Error> {
        // were we asked to terminate?
        if self.terminated.load(Ordering::SeqCst) {
            return Err(Error::new(ErrorKind::Interrupted, "terminated"));
        }

        // if we have a first value, we need to send it out first
        if let Some(command) = self.first.take() {
            self.serialize_command(command);
        }

        // otherwise we'll wait to receive a command
        if self.docs_out.load(Ordering::SeqCst) < 10_000 && self.bytes_out < 8 * 1024 * 1024 {
            // but only if we haven't exceeded the max _bulk docs limit
            match self.receiver.recv_timeout(Duration::from_millis(333)) {
                Ok(command) => self.serialize_command(command),
                Err(_) => {}
            }
        }

        let amt = buf.write(&self.buffer)?;
        if amt > 0 {
            // move our bytes forward the amount we wrote above
            let (_, right) = self.buffer.split_at(amt);
            self.buffer = Vec::from(right);
            self.bytes_out += amt;
        }

        Ok(amt)
    }
}

impl BulkReceiver {
    fn serialize_command(&mut self, command: BulkRequestCommand) {
        self.in_flight.fetch_add(1, Ordering::SeqCst);
        self.docs_out.fetch_add(1, Ordering::SeqCst);

        // build json of this entire command and store in self.bytes
        match command {
            BulkRequestCommand::Insert {
                ctid,
                cmin,
                cmax,
                xmin,
                xmax,
                builder: mut doc,
            } => {
                serde_json::to_writer(
                    &mut self.buffer,
                    &json! {
                        {"index": {"_id": ctid } }
                    },
                )
                .expect("failed to serialize index line");
                self.buffer.push(b'\n');

                doc.add_u32("zdb_cmin", cmin);
                doc.add_u32("zdb_cmax", cmax);
                doc.add_u64("zdb_xmin", xmin);
                doc.add_u64("zdb_xmax", xmax);

                let doc_as_json = doc.build();
                self.buffer.append(&mut doc_as_json.into_bytes());
                self.buffer.push(b'\n');
            }
            BulkRequestCommand::Update { .. } => panic!("unsupported"),
            BulkRequestCommand::DeleteByXmin { .. } => panic!("unsupported"),
            BulkRequestCommand::DeleteByXmax { .. } => panic!("unsupported"),
            BulkRequestCommand::Interrupt => panic!("unsupported"),
            BulkRequestCommand::Done => panic!("unsupported"),
        }
    }
}

impl From<BulkReceiver> for reqwest::Body {
    fn from(reader: BulkReceiver) -> Self {
        reqwest::Body::new(reader)
    }
}

impl<'a> Handler<'a> {
    pub(crate) fn new(
        elasticsearch: Elasticsearch<'a>,
        concurrency: usize,
        error_sender: crossbeam::channel::Sender<BulkRequestError>,
    ) -> Self {
        let (tx, rx) = crossbeam::channel::bounded(10_000);

        Handler {
            terminatd: Arc::new(AtomicBool::new(false)),
            threads: Vec::new(),
            active_thread_cnt: Arc::new(AtomicUsize::new(0)),
            in_flight: Arc::new(AtomicUsize::new(0)),
            total_docs: 0,
            elasticsearch,
            concurrency,
            bulk_sender: tx,
            bulk_receiver: rx,
            error_sender,
        }
    }

    pub fn queue_command(
        &mut self,
        command: BulkRequestCommand,
    ) -> Result<(), crossbeam::SendError<BulkRequestCommand>> {
        if self.total_docs % 10000 == 0 {
            info!(
                "total={}, in_flight={}, active_threads={}",
                self.total_docs,
                self.in_flight.load(Ordering::SeqCst),
                self.active_thread_cnt.load(Ordering::SeqCst)
            );
        }

        self.total_docs += 1;

        let nthreads = self.active_thread_cnt.load(Ordering::SeqCst);
        if nthreads < self.concurrency {
            self.threads
                .push(self.create_thread(self.threads.len(), Some(command)));

            Ok(())
        } else {
            self.bulk_sender.send(command)
        }
    }

    fn create_thread(
        &self,
        thread_id: usize,
        mut initial_command: Option<BulkRequestCommand>,
    ) -> JoinHandle<usize> {
        //        let es = self.elasticsearch.clone();
        let url = self.elasticsearch.options.url().to_owned();
        let index_name = self
            .elasticsearch
            .options
            .index_name(&self.elasticsearch.heaprel, &self.elasticsearch.indexrel);
        let rx = self.bulk_receiver.clone();
        let in_flight = self.in_flight.clone();
        let active_thread_cnt = self.active_thread_cnt.clone();
        let error = self.error_sender.clone();
        let terminated = self.terminatd.clone();

        info!("spawning thread #{}", thread_id + 1);
        self.active_thread_cnt.fetch_add(1, Ordering::SeqCst);
        std::thread::spawn(move || {
            let mut total_docs_out = 0;
            loop {
                if terminated.load(Ordering::SeqCst) {
                    eprintln!("thread #{} existing b/c of termination", thread_id);
                    break;
                }
                let initial_command = initial_command.take();
                let first;

                if initial_command.is_some() {
                    first = initial_command;
                } else {
                    first = Some(match rx.recv() {
                        Ok(command) => command,
                        Err(_) => {
                            // we don't have a first command to deal with on this iteration b/c
                            // the channel has been shutdown.  we're simply out of records
                            // and can safely break out
                            eprintln!("thread #{} exiting b/c channel is closed", thread_id);
                            break;
                        }
                    })
                }

                let docs_out = Arc::new(AtomicUsize::new(0));
                let rx = rx.clone();
                let reader = BulkReceiver {
                    terminated: terminated.clone(),
                    first,
                    in_flight: in_flight.clone(),
                    receiver: rx.clone(),
                    bytes_out: 0,
                    docs_out: docs_out.clone(),
                    buffer: Vec::new(),
                };

                let url = &format!(
                    "{}{}/_bulk?filter_path={}",
                    url, index_name, BULK_FILTER_PATH
                );
                let client = reqwest::Client::new();
                let response = client
                    .post(url)
                    .header("content-type", "application/json")
                    .body(reader)
                    .send();

                let docs_out = docs_out.load(Ordering::SeqCst);

                in_flight.fetch_sub(docs_out, Ordering::SeqCst);

                total_docs_out += docs_out;

                match response {
                    // we got a valid response from ES
                    Ok(mut response) => {
                        let code = response.status().as_u16();
                        let mut resp_string = String::new();
                        response
                            .read_to_string(&mut resp_string)
                            .expect("unable to convert HTTP response to a string");

                        if code != 200 {
                            // it wasn't a valid response code
                            return Handler::send_error(error, code, resp_string, total_docs_out);
                        } else {
                            // it was a valid response code, but does it contain errors?
                            #[derive(Deserialize)]
                            struct BulkResponse {
                                errors: bool,
                                items: Option<Vec<Value>>,
                            }

                            // NB:  this is stupid that ES forces us to parse the response for requests
                            // that contain an error, but here we are
                            let response: BulkResponse = match serde_json::from_str(&resp_string) {
                                Ok(json) => json,
                                Err(_) => {
                                    // it didn't parse as json, but we don't care as we just return
                                    // the entire response string anyway
                                    return Handler::send_error(
                                        error,
                                        code,
                                        resp_string,
                                        total_docs_out,
                                    );
                                }
                            };

                            if response.errors {
                                // yup, the response contains an error
                                return Handler::send_error(
                                    error,
                                    code,
                                    resp_string,
                                    total_docs_out,
                                );
                            }
                        }
                    }

                    // this is likely a general reqwest/network communication error
                    Err(e) => {
                        return Handler::send_error(error, 0, format!("{:?}", e), total_docs_out);
                    }
                }

                if docs_out == 0 {
                    eprintln!("thread #{} exiting b/x docs_out == 0", thread_id);
                    break;
                }
            }

            active_thread_cnt.fetch_sub(1, Ordering::SeqCst);
            total_docs_out
        })
    }

    fn send_error(
        sender: crossbeam::Sender<BulkRequestError>,
        code: u16,
        message: String,
        total_docs_out: usize,
    ) -> usize {
        sender
            .send(BulkRequestError::IndexingError(format!(
                "code={}, {}",
                code, message
            )))
            .expect("failed to send error over channel");
        total_docs_out
    }

    pub fn wait_for_completion(self) -> Result<usize, BulkRequestError> {
        // drop the sender side of the channel since we're done
        // this will signal the receivers that once their queues are empty
        // there's nothing left for them to do
        std::mem::drop(self.bulk_sender);

        info!("thead count={}", self.threads.len());
        let mut cnt = 0;
        for (i, jh) in self.threads.into_iter().enumerate() {
            match jh.join() {
                Ok(many) => {
                    info!(
                        "thread #{}: total_docs_out={}, in_flight={}",
                        i + 1,
                        many,
                        self.in_flight.load(Ordering::SeqCst)
                    );
                    cnt += many;
                }
                Err(e) => panic!("Got an error joining on a thread: {}", downcast_err(e)),
            }
        }

        Ok(cnt)
    }

    pub(crate) fn terminate(&mut self) {
        self.terminatd.store(true, Ordering::SeqCst);
    }
}

fn downcast_err(e: Box<dyn Any + Send>) -> String {
    if let Some(s) = e.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = e.downcast_ref::<String>() {
        s.to_string()
    } else {
        // not a type we understand, so use a generic string
        "Box<Any>".to_string()
    }
}
