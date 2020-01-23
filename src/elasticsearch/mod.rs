use pgx::*;
use serde::*;
use serde_json::Value;

mod bulk;

#[derive(Debug, Clone, Serialize)]
pub enum BulkRequestCommand {
    Insert {
        #[serde(skip_serializing)]
        ctid: pg_sys::ItemPointerData,
        cmin: pg_sys::CommandId,
        cmax: pg_sys::CommandId,
        xmin: u64,
        xmax: u64,
        doc: Value,
    },
    Update {
        #[serde(skip_serializing)]
        ctid: pg_sys::ItemPointerData,
        cmax: pg_sys::CommandId,
        xmax: u64,
        doc: Value,
    },
    DeleteByXmin {
        #[serde(skip_serializing)]
        ctid: pg_sys::ItemPointerData,
        xmin: u64,
    },
    DeleteByXmax {
        #[serde(skip_serializing)]
        ctid: pg_sys::ItemPointerData,
        xmax: u64,
    },
    Interrupt,
    Done,
}

#[derive(Debug)]
pub enum BulkRequestError {
    CommError,
    NoError,
}

#[derive(Debug, Clone)]
pub struct Elasticsearch {
    url: url::Url,
    index_name: String,
}

pub struct ElasticsearchBulkRequest {
    handler: bulk::Handler,
    bulk_sender: crossbeam::channel::Sender<BulkRequestCommand>,
    error_receiver: crossbeam::channel::Receiver<BulkRequestError>,
}

impl Elasticsearch {
    pub fn new(url: &str, index_name: &str) -> Self {
        Elasticsearch {
            url: url::Url::parse(url).expect("malformed url"),
            index_name: index_name.to_string(),
        }
    }

    pub fn start_bulk(self) -> ElasticsearchBulkRequest {
        ElasticsearchBulkRequest::new(self, 10_000, num_cpus::get())
    }
}

impl ElasticsearchBulkRequest {
    fn new(elasticsearch: Elasticsearch, queue_size: usize, concurrency: usize) -> Self {
        let (btx, brx) = crossbeam::channel::bounded(queue_size * concurrency);
        let (etx, erx) = crossbeam::channel::bounded(queue_size * concurrency);

        ElasticsearchBulkRequest {
            handler: bulk::Handler::run(elasticsearch, concurrency, brx, etx),
            bulk_sender: btx,
            error_receiver: erx,
        }
    }

    pub fn wait_for_completion(mut self) -> Result<usize, BulkRequestError> {
        // drop the sender side of the channel since we're done
        // this will signal the receivers that once their queues are empty
        // there's nothing left for them to do
        std::mem::drop(self.bulk_sender);

        // wait for the bulk requests to finish
        self.handler.wait_for_completion()
    }

    pub fn insert(
        &mut self,
        ctid: pg_sys::ItemPointerData,
        cmin: pg_sys::CommandId,
        cmax: pg_sys::CommandId,
        xmin: u64,
        xmax: u64,
        doc: Value,
    ) -> Result<(), crossbeam::SendError<BulkRequestCommand>> {
        self.check_for_error();

        self.bulk_sender.send(BulkRequestCommand::Insert {
            ctid,
            cmin,
            cmax,
            xmin,
            xmax,
            doc,
        })
    }

    pub fn update(
        &mut self,
        ctid: pg_sys::ItemPointerData,
        cmax: pg_sys::CommandId,
        xmax: u64,
        doc: Value,
    ) -> Result<(), crossbeam::SendError<BulkRequestCommand>> {
        self.check_for_error();

        self.bulk_sender.send(BulkRequestCommand::Update {
            ctid,
            cmax,
            xmax,
            doc,
        })
    }

    pub fn delete_by_xmin(
        &mut self,
        ctid: pg_sys::ItemPointerData,
        xmin: u64,
    ) -> Result<(), crossbeam::SendError<BulkRequestCommand>> {
        self.check_for_error();

        self.bulk_sender
            .send(BulkRequestCommand::DeleteByXmin { ctid, xmin })
    }

    pub fn delete_by_xmax(
        &mut self,
        ctid: pg_sys::ItemPointerData,
        xmax: u64,
    ) -> Result<(), crossbeam::SendError<BulkRequestCommand>> {
        self.check_for_error();

        self.bulk_sender
            .send(BulkRequestCommand::DeleteByXmax { ctid, xmax })
    }

    #[inline]
    fn check_for_error(&mut self) {
        // do we have an error queued up?
        match self
            .error_receiver
            .try_recv()
            .unwrap_or(BulkRequestError::NoError)
        {
            BulkRequestError::CommError => {
                self.handler.terminate();
                panic!("Elasticsearch Communication Error");
            }
            BulkRequestError::NoError => {}
        }

        if interrupt_pending() {
            self.handler.terminate();
            check_for_interrupts!();
        }
    }
}
