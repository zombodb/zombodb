use crate::elasticsearch::{BulkRequestCommand, BulkRequestError, Elasticsearch};
use pgx::*;
use serde_json::json;
use std::io::{Error, Write};
use std::thread::JoinHandle;

pub(crate) struct Handler {
    threads: Vec<JoinHandle<usize>>,
}

struct BulkReceiver {
    receiver: crossbeam::channel::Receiver<BulkRequestCommand>,
    bytes_out: usize,
    docs_out: *mut usize,
    bytes: Vec<u8>,
}

impl std::io::Read for BulkReceiver {
    fn read(&mut self, mut buf: &mut [u8]) -> Result<usize, Error> {
        let mut total = 0;
        let mut
        eprintln!("enter");
        for command in self.receiver.iter() {
            eprintln!("got command");
            if self.bytes.is_empty() {
                // build json of this entire command and store in self.bytes

                match command {
                    BulkRequestCommand::Insert {
                        ctid,
                        cmin,
                        cmax,
                        xmin,
                        xmax,
                        doc,
                    } => {
                        serde_json::to_writer(
                            &mut self.bytes,
                            &json! {
                                {"index": {"_id": item_pointer_to_u64(ctid), "foo":format!("{:?}", ctid)}}
                            },
                        )
                        .expect("failed to serialize index line");
                        self.bytes.push(b'\n');
                        serde_json::to_writer(&mut self.bytes, &doc)
                            .expect("failed to serialize doc");
                        self.bytes.push(b'\n');
                    }
                    BulkRequestCommand::Update { .. } => panic!("unsupported"),
                    BulkRequestCommand::DeleteByXmin { .. } => panic!("unsupported"),
                    BulkRequestCommand::DeleteByXmax { .. } => panic!("unsupported"),
                    BulkRequestCommand::Interrupt => panic!("unsupported"),
                    BulkRequestCommand::Done => panic!("unsupported"),
                }
            }

            let amt = buf.write(&self.bytes)?;
            if amt == 0 {
                break;
            }

            // move our bytes forward the amount we wrote above
            let (_, right) = self.bytes.split_at(amt);
            self.bytes = Vec::from(right);

            unsafe {
                *self.docs_out += 1;
            }
            total += amt;
            eprintln!("looping");
        }

        eprintln!("returning: {}", total);
        self.bytes_out += total;
        Ok(total)
    }
}

impl Handler {
    pub(crate) fn run(
        elasticsearch: Elasticsearch,
        concurrency: usize,
        bulk_receiver: crossbeam::channel::Receiver<BulkRequestCommand>,
        error_sender: crossbeam::channel::Sender<BulkRequestError>,
    ) -> Self {
        let mut threads = Vec::new();
        for i in 0..1 {
            // concurrency {
            let es = elasticsearch.clone();
            let rx = bulk_receiver.clone();

            let jh = std::thread::spawn(move || {
                let mut docs_out = 0;
                let reader = BulkReceiver {
                    receiver: rx,
                    bytes_out: 0,
                    docs_out: &mut docs_out,
                    bytes: vec![],
                };

                ureq::post(es.url.as_str()).send(reader);
                eprintln!("thead#{}: docs_out={}", i, docs_out);
                docs_out
            });

            threads.push(jh)
        }

        Handler { threads }
    }

    pub(crate) fn wait_for_completion(mut self) -> Result<usize, BulkRequestError> {
        let mut cnt = 0;
        let mut error = BulkRequestError::NoError;

        for jh in self.threads {
            match jh.join() {
                Ok(many) => {
                    cnt += many;
                }
                Err(e) => panic!("Got an error joining on a thread"),
            }
        }

        Ok(cnt)
    }

    pub(crate) fn terminate(&mut self) {}
}
