use crate::elasticsearch::{BulkRequestCommand, BulkRequestError, Elasticsearch};
use pgx::*;
use serde_json::json;
use std::io::{Error, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;

pub(crate) struct Handler {
    threads: Vec<JoinHandle<usize>>,
}

struct BulkReceiver {
    receiver: crossbeam::channel::Receiver<BulkRequestCommand>,
    bytes_out: usize,
    docs_out: Arc<AtomicUsize>,
    backlog: Vec<u8>,
}

impl std::io::Read for BulkReceiver {
    fn read(&mut self, mut buf: &mut [u8]) -> Result<usize, Error> {
        let mut bytes = &mut self.backlog;

        if self.docs_out.load(Ordering::SeqCst) < 10_000 && self.bytes_out < 8 * 1024 * 1024 {
            // we haven't exceeded the max _bulk docs limit

            for command in self.receiver.iter() {
                self.docs_out.fetch_add(1, Ordering::SeqCst);

                // build json of this entire command and store in self.bytes
                match command {
                    BulkRequestCommand::Insert {
                        ctid,
                        cmin: _,
                        cmax: _,
                        xmin: _,
                        xmax: _,
                        doc,
                    } => {
                        serde_json::to_writer(
                            &mut bytes,
                            &json! {
                                {"index": {"_id": item_pointer_to_u64(ctid) } }
                            },
                        )
                        .expect("failed to serialize index line");
                        bytes.push(b'\n');

                        serde_json::to_writer(&mut bytes, &doc).expect("failed to serialize doc");
                        bytes.push(b'\n');
                    }
                    BulkRequestCommand::Update { .. } => panic!("unsupported"),
                    BulkRequestCommand::DeleteByXmin { .. } => panic!("unsupported"),
                    BulkRequestCommand::DeleteByXmax { .. } => panic!("unsupported"),
                    BulkRequestCommand::Interrupt => panic!("unsupported"),
                    BulkRequestCommand::Done => panic!("unsupported"),
                }

                break;
            }
        }

        let amt = buf.write(&bytes)?;
        if amt > 0 {
            // move our bytes forward the amount we wrote above
            let (_, right) = bytes.split_at(amt);
            self.backlog = Vec::from(right);
            self.bytes_out += amt;
        }

        Ok(amt)
    }
}

impl From<BulkReceiver> for reqwest::Body {
    fn from(reader: BulkReceiver) -> Self {
        reqwest::Body::new(reader)
    }
}

impl Handler {
    pub(crate) fn run(
        elasticsearch: Elasticsearch,
        concurrency: usize,
        bulk_receiver: crossbeam::channel::Receiver<BulkRequestCommand>,
        _error_sender: crossbeam::channel::Sender<BulkRequestError>,
    ) -> Self {
        let mut threads = Vec::new();
        for i in 0..concurrency {
            let es = elasticsearch.clone();
            let rx = bulk_receiver.clone();

            let jh = std::thread::spawn(move || {
                let mut total_docs_out = 0;

                loop {
                    let docs_out = Arc::new(AtomicUsize::new(0));

                    let rx = rx.clone();
                    let reader = BulkReceiver {
                        receiver: rx.clone(),
                        bytes_out: 0,
                        docs_out: docs_out.clone(),
                        backlog: Vec::new(),
                    };

                    let url = &format!("{}{}/_bulk", es.url, es.index_name);
                    let client = reqwest::Client::new();
                    let response = client
                        .post(url)
                        .header("content-type", "application/json")
                        .body(reader)
                        .send();

                    let docs_out = docs_out.load(Ordering::SeqCst);
                    total_docs_out += docs_out;

                    eprintln!("thread#{}: docs_out={}", i, docs_out);
                    match response {
                        Ok(mut _response) => {
                            // TODO:  parse the response into json and inspect for errors
                            //                        let mut resp_string = String::new();
                            //                        response.read_to_string(&mut resp_string);
                            //                        eprintln!("response={}", resp_string);
                        }

                        Err(e) => eprintln!("error {:?}", e),
                    }

                    if docs_out == 0 {
                        break;
                    }
                }

                eprintln!("thread#{}: total_docs_out={}", i, total_docs_out);
                total_docs_out
            });

            threads.push(jh)
        }

        Handler { threads }
    }

    pub(crate) fn wait_for_completion(self) -> Result<usize, BulkRequestError> {
        let mut cnt = 0;

        for jh in self.threads {
            match jh.join() {
                Ok(many) => {
                    cnt += many;
                }
                Err(_) => panic!("Got an error joining on a thread"),
            }
        }

        Ok(cnt)
    }

    pub(crate) fn terminate(&mut self) {}
}
