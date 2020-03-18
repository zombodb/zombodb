use crate::access_method::options::RefreshInterval;
use crate::elasticsearch::{Elasticsearch, ElasticsearchError};
use crate::executor_manager::get_executor_manager;
use crate::gucs::ZDB_LOG_LEVEL;
use crate::json::builder::JsonBuilder;
use pgx::*;
use serde::Deserialize;
use serde_json::{json, Value};
use std::any::Any;
use std::io::{Error, ErrorKind, Write};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

#[derive(Debug)]
pub enum BulkRequestCommand<'a> {
    Insert {
        ctid: u64,
        cmin: pg_sys::CommandId,
        cmax: pg_sys::CommandId,
        xmin: u64,
        xmax: u64,
        builder: JsonBuilder<'a>,
    },
    Update {
        ctid: u64,
        cmax: pg_sys::CommandId,
        xmax: u64,
    },
    TransactionInProgress {
        xid: u64,
    },
    TransactionCommitted {
        xid: u64,
    },
    DeleteByXmin {
        ctid: u64,
        xmin: u64,
    },
    DeleteByXmax {
        ctid: u64,
        xmax: u64,
    },
    VacuumXmax {
        ctid: u64,
        xmax: u64,
    },
    RemoveAbortedTransactions {
        xids: Vec<u64>,
    },
}

#[derive(Debug)]
pub enum BulkRequestError {
    IndexingError(String),
    RefreshError(String),
    NoError,
}

pub struct ElasticsearchBulkRequest {
    handler: Handler,
    error_receiver: crossbeam_channel::Receiver<BulkRequestError>,
}

impl ElasticsearchBulkRequest {
    pub fn new(
        elasticsearch: &Elasticsearch,
        queue_size: usize,
        concurrency: usize,
        batch_size: usize,
    ) -> Self {
        let (etx, erx) = crossbeam_channel::bounded(concurrency);

        ElasticsearchBulkRequest {
            handler: Handler::new(
                elasticsearch.clone(),
                queue_size,
                concurrency,
                batch_size,
                etx,
                &erx,
            ),
            error_receiver: erx,
        }
    }

    pub fn finish(self) -> Result<(usize, usize), BulkRequestError> {
        self.handler.check_for_error();

        // need to clone the successful_requests counter so we can get an accourate
        // count after we've called .wait_for_completion()
        let successful_requests = self.handler.successful_requests.clone();
        let elasticsearch = self.handler.elasticsearch.clone();

        // wait for the bulk requests to finish
        let total_docs = self.handler.wait_for_completion()?;
        let nrequests = successful_requests.load(Ordering::SeqCst);

        // now refresh the index
        match elasticsearch.options.refresh_interval {
            RefreshInterval::Immediate => ElasticsearchBulkRequest::refresh_index(elasticsearch)?,
            RefreshInterval::ImmediateAsync => {
                std::thread::spawn(|| ElasticsearchBulkRequest::refresh_index(elasticsearch).ok());
            }
            RefreshInterval::Background(_) => {
                // Elasticsearch will do it for us in the future
            }
        }

        Ok((total_docs, nrequests))
    }

    fn refresh_index(elasticsearch: Elasticsearch) -> Result<(), BulkRequestError> {
        if let Err(e) = elasticsearch.refresh_index().execute() {
            Err(BulkRequestError::RefreshError(e.message().to_string()))
        } else {
            Ok(())
        }
    }

    pub fn terminate(
        &self,
    ) -> impl Fn() + std::panic::UnwindSafe + std::panic::RefUnwindSafe + 'static {
        let terminate = self.handler.terminatd.clone();
        move || {
            terminate.store(true, Ordering::SeqCst);
        }
    }

    pub fn terminate_now(&self) {
        (self.terminate())();
    }

    pub fn totals(&self) -> (usize, usize) {
        (
            self.handler.total_docs,
            self.handler.successful_requests.load(Ordering::SeqCst),
        )
    }

    pub fn insert(
        &mut self,
        ctid: pg_sys::ItemPointerData,
        cmin: pg_sys::CommandId,
        cmax: pg_sys::CommandId,
        xmin: u64,
        xmax: u64,
        builder: JsonBuilder<'static>,
    ) -> Result<(), crossbeam_channel::SendError<BulkRequestCommand>> {
        self.handler.check_for_error();

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
    ) -> Result<(), crossbeam_channel::SendError<BulkRequestCommand>> {
        self.handler.check_for_error();

        self.handler.queue_command(BulkRequestCommand::Update {
            ctid: item_pointer_to_u64(ctid),
            cmax,
            xmax,
        })
    }

    pub fn transaction_in_progress(
        &mut self,
        xid: pg_sys::TransactionId,
    ) -> Result<(), crossbeam_channel::SendError<BulkRequestCommand>> {
        self.handler.check_for_error();

        self.handler
            .queue_command(BulkRequestCommand::TransactionInProgress {
                xid: xid_to_64bit(xid),
            })
    }

    pub fn transaction_committed(
        &mut self,
        xid: pg_sys::TransactionId,
    ) -> Result<(), crossbeam_channel::SendError<BulkRequestCommand>> {
        self.handler.check_for_error();

        // the transaction committed command needs to be the last command we send
        // so we busy-loop wait until there's only 1 (or zero) active threads
        // and then we send the command so it'll be the last command
        while self.handler.active_threads.load(Ordering::SeqCst) > 1 {
            check_for_interrupts!();
            std::thread::yield_now();
        }

        info!("send commit");
        self.handler
            .queue_command(BulkRequestCommand::TransactionCommitted {
                xid: xid_to_64bit(xid),
            })
    }

    pub fn delete_by_xmin(
        &mut self,
        ctid: u64,
        xmin: u64,
    ) -> Result<(), crossbeam_channel::SendError<BulkRequestCommand>> {
        self.handler.check_for_error();

        self.handler
            .queue_command(BulkRequestCommand::DeleteByXmin { ctid, xmin })
    }

    pub fn delete_by_xmax(
        &mut self,
        ctid: u64,
        xmax: u64,
    ) -> Result<(), crossbeam_channel::SendError<BulkRequestCommand>> {
        self.handler.check_for_error();

        self.handler
            .queue_command(BulkRequestCommand::DeleteByXmax { ctid, xmax })
    }

    pub fn vacuum_xmax(
        &mut self,
        ctid: u64,
        xmax: u64,
    ) -> Result<(), crossbeam_channel::SendError<BulkRequestCommand>> {
        self.handler.check_for_error();

        self.handler
            .queue_command(BulkRequestCommand::VacuumXmax { ctid, xmax })
    }

    pub fn remove_aborted_xids(
        &mut self,
        xids: Vec<u64>,
    ) -> Result<(), crossbeam_channel::SendError<BulkRequestCommand>> {
        self.handler.check_for_error();

        if xids.is_empty() {
            // nothing to do
            Ok(())
        } else {
            self.handler
                .queue_command(BulkRequestCommand::RemoveAbortedTransactions { xids })
        }
    }
}

const BULK_FILTER_PATH: &str = "errors,items.*.error";

pub(crate) struct Handler {
    pub(crate) terminatd: Arc<AtomicBool>,
    threads: Vec<Option<JoinHandle<usize>>>,
    in_flight: Arc<AtomicUsize>,
    total_docs: usize,
    active_threads: Arc<AtomicUsize>,
    successful_requests: Arc<AtomicUsize>,
    elasticsearch: Elasticsearch,
    concurrency: usize,
    batch_size: usize,
    bulk_sender: Option<crossbeam_channel::Sender<BulkRequestCommand<'static>>>,
    bulk_receiver: crossbeam_channel::Receiver<BulkRequestCommand<'static>>,
    error_sender: crossbeam_channel::Sender<BulkRequestError>,
    error_receiver: crossbeam_channel::Receiver<BulkRequestError>,
}

struct BulkReceiver<'a> {
    terminated: Arc<AtomicBool>,
    first: Option<BulkRequestCommand<'a>>,
    in_flight: Arc<AtomicUsize>,
    receiver: crossbeam_channel::Receiver<BulkRequestCommand<'a>>,
    bytes_out: usize,
    docs_out: Arc<AtomicUsize>,
    active_threads: Arc<AtomicUsize>,
    buffer: Vec<u8>,
    batch_size: usize,
}

impl<'a> std::io::Read for BulkReceiver<'a> {
    fn read(&mut self, mut buf: &mut [u8]) -> Result<usize, Error> {
        // were we asked to terminate?
        if self.terminated.load(Ordering::SeqCst) {
            return Err(Error::new(ErrorKind::Interrupted, "terminated"));
        }

        let command = if self.first.is_some() {
            // take our first command
            self.first.take()
        } else if self.docs_out.load(Ordering::SeqCst) < 10_000 && self.bytes_out < self.batch_size
        {
            // take a command from the receiver
            match self.receiver.recv_timeout(Duration::from_millis(333)) {
                Ok(command) => Some(command),
                Err(_) => None,
            }
        } else {
            None
        };

        if let Some(command) = command {
            self.serialize_command(command);
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

impl<'a> BulkReceiver<'a> {
    fn serialize_command(&mut self, command: BulkRequestCommand<'a>) {
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

                doc.add_u64("zdb_ctid", ctid);
                doc.add_u32("zdb_cmin", cmin);
                if cmax as pg_sys::CommandId != pg_sys::InvalidCommandId {
                    doc.add_u32("zdb_cmax", cmax);
                }
                doc.add_u64("zdb_xmin", xmin);
                if xmax as pg_sys::TransactionId != pg_sys::InvalidTransactionId {
                    doc.add_u64("zdb_xmax", xmax);
                }

                let doc_as_json = doc.build();
                self.buffer.append(&mut doc_as_json.into_bytes());
                self.buffer.push(b'\n');
            }
            BulkRequestCommand::Update { ctid, cmax, xmax } => {
                serde_json::to_writer(
                    &mut self.buffer,
                    &json! {
                        {
                            "update": {
                                "_id": ctid,
                                "retry_on_conflict": 1
                            }
                        }
                    },
                )
                .expect("failed to serialize update line");
                self.buffer.push(b'\n');

                serde_json::to_writer(
                    &mut self.buffer,
                    &json! {
                        {
                            "script": {
                                "source": "ctx._source.zdb_cmax=params.CMAX;ctx._source.zdb_xmax=params.XMAX;",
                                "lang": "painless",
                                "params": {
                                    "CMAX": cmax,
                                    "XMAX": xmax
                                }
                            }
                        }
                    },
                )
                .expect("failed to serialize update command");
                self.buffer.push(b'\n');
            }
            BulkRequestCommand::TransactionInProgress { xid } => {
                serde_json::to_writer(
                    &mut self.buffer,
                    &json! {
                        {
                            "update": {
                                "_id": "zdb_aborted_xids",
                                "retry_on_conflict": 128
                            }
                        }
                    },
                )
                .expect("failed to serialize update line for transaction in progress");
                self.buffer.push(b'\n');

                serde_json::to_writer(
                    &mut self.buffer,
                    &json! {
                        {
                            "upsert": {
                                "zdb_aborted_xids": [xid]
                            },
                            "script": {
                                "source": "ctx._source.zdb_aborted_xids.add(params.XID);",
                                "lang": "painless",
                                "params": { "XID": xid }
                            }
                        }
                    },
                )
                .expect("failed to serialize upsert line for transaction in progress");
                self.buffer.push(b'\n');
            }
            BulkRequestCommand::TransactionCommitted { xid } => {
                serde_json::to_writer(
                    &mut self.buffer,
                    &json! {
                        {
                            "update": {
                                "_id": "zdb_aborted_xids",
                                "retry_on_conflict": 128
                            }
                        }
                    },
                )
                .expect("failed to serialize update line for transaction committed");
                self.buffer.push(b'\n');

                serde_json::to_writer(
                    &mut self.buffer,
                    &json! {
                        {
                            "script": {
                                "source": "ctx._source.zdb_aborted_xids.remove(ctx._source.zdb_aborted_xids.indexOf(params.XID));",
                                "lang": "painless",
                                "params": { "XID": xid }
                            }
                        }
                    },
                )
                .expect("failed to serialize update line for transaction committed");
                self.buffer.push(b'\n');
            }
            BulkRequestCommand::DeleteByXmin { ctid, xmin } => {
                serde_json::to_writer(
                    &mut self.buffer,
                    &json! {
                        {
                            "update": { "_id": ctid }
                        }
                    },
                )
                .expect("failed to serialize update line for delete by xmin");
                self.buffer.push(b'\n');

                serde_json::to_writer(&mut self.buffer,
                &json! {
                    {
                        "script": {
                            "source": "if (ctx._source.zdb_xmin == params.EXPECTED_XMIN) { ctx.op='delete'; } else { ctx.op='none'; }",
                            "lang": "painless",
                            "params": { "EXPECTED_XMIN": xmin }
                        }
                    }
                }).expect("failed to serialize script line for delete by xmin");
                self.buffer.push(b'\n');
            }
            BulkRequestCommand::DeleteByXmax { ctid, xmax } => {
                serde_json::to_writer(
                    &mut self.buffer,
                    &json! {
                        {
                            "update": { "_id": ctid }
                        }
                    },
                )
                .expect("failed to serialize update line for delete by xmax");
                self.buffer.push(b'\n');

                serde_json::to_writer(&mut self.buffer,
                                      &json! {
                    {
                        "script": {
                            "source": "if (ctx._source.zdb_xmax == params.EXPECTED_XMAX) { ctx.op='delete'; } else { ctx.op='none'; }",
                            "lang": "painless",
                            "params": { "EXPECTED_XMAX": xmax }
                        }
                    }
                }).expect("failed to serialize script line for delete by xmax");
                self.buffer.push(b'\n');
            }
            BulkRequestCommand::VacuumXmax { ctid, xmax } => {
                serde_json::to_writer(
                    &mut self.buffer,
                    &json! {
                        {
                            "update": { "_id": ctid, "retry_on_conflict": 0 }
                        }
                    },
                )
                .expect("failed to serialize update line for vacuum xmax");
                self.buffer.push(b'\n');

                serde_json::to_writer(&mut self.buffer,
                                      &json! {
                    {
                        "script": {
                            "source": "if (ctx._source.zdb_xmax != params.EXPECTED_XMAX) { ctx.op='none'; } else { ctx._source.zdb_xmax=null; }",
                            "lang": "painless",
                            "params": { "EXPECTED_XMAX": xmax }
                        }
                    }
                }).expect("failed to serialize script line for vacuum xmax");
                self.buffer.push(b'\n');
            }
            BulkRequestCommand::RemoveAbortedTransactions { xids } => {
                serde_json::to_writer(
                    &mut self.buffer,
                    &json! {
                        {
                            "update": { "_id": "zdb_aborted_xids", "retry_on_conflict": 128 }
                        }
                    },
                )
                .expect("failed to serialize update line for remove aborted transactions");
                self.buffer.push(b'\n');

                serde_json::to_writer(
                    &mut self.buffer,
                    &json! {
                        {
                            "script": {
                                "source": "ctx._source.zdb_aborted_xids.removeAll(params.XIDS);",
                                "lang": "painless",
                                "params": { "XIDS": xids }
                            }
                        }
                    },
                )
                .expect("failed to serialize script line for remove aborted transactions");
                self.buffer.push(b'\n');
            }
        }
    }
}

impl From<BulkReceiver<'static>> for reqwest::Body {
    fn from(reader: BulkReceiver<'static>) -> Self {
        reqwest::Body::new(reader)
    }
}

impl Handler {
    pub(crate) fn new(
        elasticsearch: Elasticsearch,
        _queue_size: usize,
        concurrency: usize,
        batch_size: usize,
        error_sender: crossbeam_channel::Sender<BulkRequestError>,
        error_receiver: &crossbeam_channel::Receiver<BulkRequestError>,
    ) -> Self {
        // NB:  creating a large (queue_size * concurrency) bounded channel
        // is quite slow.  Going with 2 * concurrency, which should be twice the number of index shards
        let (tx, rx) = crossbeam_channel::bounded((2 * concurrency).max(4));

        Handler {
            terminatd: Arc::new(AtomicBool::new(false)),
            threads: Vec::new(),
            in_flight: Arc::new(AtomicUsize::new(0)),
            total_docs: 0,
            active_threads: Arc::new(AtomicUsize::new(0)),
            successful_requests: Arc::new(AtomicUsize::new(0)),
            elasticsearch,
            batch_size,
            concurrency,
            bulk_sender: Some(tx),
            bulk_receiver: rx,
            error_sender,
            error_receiver: error_receiver.clone(),
        }
    }

    pub fn queue_command(
        &mut self,
        command: BulkRequestCommand<'static>,
    ) -> Result<(), crossbeam_channel::SendError<BulkRequestCommand<'static>>> {
        match &command {
            BulkRequestCommand::Insert { .. } | BulkRequestCommand::Update { .. } => {
                let current_xid = unsafe { pg_sys::GetCurrentTransactionIdIfAny() };
                if current_xid != pg_sys::InvalidTransactionId {
                    get_executor_manager().push_xid(current_xid);
                }
            }
            _ => {}
        }

        // send the command
        self.bulk_sender.as_ref().unwrap().send(command)?;

        // now determine if we need to start a new thread to handle what's in the queue
        let nthreads = self.active_threads.load(Ordering::SeqCst);
        if self.total_docs > 0 && self.total_docs % 10_000 == 0 {
            elog(
                ZDB_LOG_LEVEL.get().log_level(),
                &format!(
                    "[zombodb] total={}, in_flight={}, queued={}, active_threads={}, index={}",
                    self.total_docs,
                    self.in_flight.load(Ordering::SeqCst),
                    self.bulk_receiver.len(),
                    nthreads,
                    self.elasticsearch.base_url()
                ),
            );
        }
        if nthreads == 0
            || (nthreads < self.concurrency && self.total_docs % (10_000 / self.concurrency) == 0)
        {
            info!(
                "creating thread:  queue={}, total_docs={}, nthreads={}",
                self.bulk_receiver.len(),
                self.total_docs,
                nthreads
            );
            self.threads.push(Some(self.create_thread(nthreads)));
        }

        self.total_docs += 1;
        Ok(())
    }

    fn create_thread(&self, _thread_id: usize) -> JoinHandle<usize> {
        let base_url = self.elasticsearch.base_url();
        let rx = self.bulk_receiver.clone();
        let in_flight = self.in_flight.clone();
        let error = self.error_sender.clone();
        let terminated = self.terminatd.clone();
        let batch_size = self.batch_size;
        let active_threads = self.active_threads.clone();
        let successful_requests = self.successful_requests.clone();

        self.active_threads.fetch_add(1, Ordering::SeqCst);
        std::thread::spawn(move || {
            let mut total_docs_out = 0;
            loop {
                if terminated.load(Ordering::SeqCst) {
                    // we've been signaled to terminate, so get out now
                    break;
                }

                let first = Some(match rx.recv_timeout(Duration::from_millis(333)) {
                    Ok(command) => command,
                    Err(_) => {
                        // we don't have a first command to deal with on this iteration b/c
                        // the channel has been shutdown.  we're simply out of records
                        // and can safely break out
                        break;
                    }
                });

                let docs_out = Arc::new(AtomicUsize::new(0));
                let reader = BulkReceiver {
                    terminated: terminated.clone(),
                    first,
                    in_flight: in_flight.clone(),
                    receiver: rx.clone(),
                    batch_size,
                    bytes_out: 0,
                    docs_out: docs_out.clone(),
                    active_threads: active_threads.clone(),
                    buffer: Vec::new(),
                };

                let url = format!("{}/_bulk?filter_path={}", base_url, BULK_FILTER_PATH);
                if let Err(e) = Elasticsearch::execute_request(
                    reqwest::Client::builder()
                        .timeout(Duration::from_secs(60 * 60))
                        .gzip(true)
                        .build()
                        .expect("failed to build client")
                        .post(&url)
                        .header("content-type", "application/json")
                        .body(reader),
                    |code, resp_string| {
                        #[derive(Deserialize, Debug)]
                        struct ErrorObject {
                            reason: String,
                        }

                        #[derive(Deserialize, Debug)]
                        struct BulkResponse {
                            error: Option<ErrorObject>,
                            errors: Option<bool>,
                            items: Option<Vec<Value>>,
                        }

                        // NB:  this is stupid that ES forces us to parse the response for requests
                        // that contain an error, but here we are
                        let response: BulkResponse = match serde_json::from_str(&resp_string) {
                            Ok(response) => response,

                            // it didn't parse as json, but we don't care as we just return
                            // the entire response string anyway
                            Err(_) => {
                                return Err(ElasticsearchError(Some(code), resp_string));
                            }
                        };

                        if !response.errors.unwrap_or(false) && response.error.is_none() {
                            successful_requests.fetch_add(1, Ordering::SeqCst);
                            Ok(())
                        } else {
                            // yup, the response contains an error
                            Err(ElasticsearchError(Some(code), resp_string))
                        }
                    },
                ) {
                    return Handler::send_error(error, e.status(), e.message(), total_docs_out);
                }

                let docs_out = docs_out.load(Ordering::SeqCst);
                in_flight.fetch_sub(docs_out, Ordering::SeqCst);
                total_docs_out += docs_out;

                if docs_out == 0 {
                    // we didn't output any docs, which likely means there's no more in the channel
                    // to process, so get out.
                    break;
                }
            }

            active_threads.fetch_sub(1, Ordering::SeqCst);
            total_docs_out
        })
    }

    fn send_error(
        sender: crossbeam_channel::Sender<BulkRequestError>,
        code: Option<reqwest::StatusCode>,
        message: &str,
        total_docs_out: usize,
    ) -> usize {
        sender
            .send(BulkRequestError::IndexingError(format!(
                "code={:?}, {}",
                code, message
            )))
            .expect("failed to send error over channel");
        total_docs_out
    }

    pub fn wait_for_completion(mut self) -> Result<usize, BulkRequestError> {
        // drop the sender side of the channel since we're done
        // this will signal the receivers that once their queues are empty
        // there's nothing left for them to do
        std::mem::drop(self.bulk_sender.take());

        let mut cnt = 0;
        for i in 0..self.threads.len() {
            let jh = self.threads.get_mut(i).unwrap().take().unwrap();
            match jh.join() {
                Ok(many) => {
                    info!("thread finished");
                    self.check_for_error();
                    cnt += many;
                }
                Err(e) => panic!("Got an error joining on a thread: {}", downcast_err(e)),
            }
        }

        Ok(cnt)
    }

    pub(crate) fn terminate(&self) {
        self.terminatd.store(true, Ordering::SeqCst);
    }

    #[inline]
    pub(crate) fn check_for_error(&self) {
        // do we have an error queued up?
        match self
            .error_receiver
            .try_recv()
            .unwrap_or(BulkRequestError::NoError)
        {
            BulkRequestError::IndexingError(err_string)
            | BulkRequestError::RefreshError(err_string) => {
                self.terminate();
                panic!("{}", err_string);
            }
            BulkRequestError::NoError => {}
        }

        if interrupt_pending() {
            self.terminate();
            check_for_interrupts!();
        }
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
