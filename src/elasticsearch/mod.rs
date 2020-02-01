#![allow(dead_code)]
use crate::json::builder::JsonBuilder;
use pgx::*;

mod bulk;

#[derive(Debug)]
pub enum BulkRequestCommand {
    Insert {
        ctid: pg_sys::ItemPointerData,
        cmin: pg_sys::CommandId,
        cmax: pg_sys::CommandId,
        xmin: u64,
        xmax: u64,
        builder: JsonBuilder,
    },
    Update {
        ctid: pg_sys::ItemPointerData,
        cmax: pg_sys::CommandId,
        xmax: u64,
        builder: JsonBuilder,
    },
    DeleteByXmin {
        ctid: pg_sys::ItemPointerData,
        xmin: u64,
    },
    DeleteByXmax {
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
        let (etx, erx) = crossbeam::channel::bounded(queue_size * concurrency);

        ElasticsearchBulkRequest {
            handler: bulk::Handler::new(elasticsearch, concurrency, etx),
            error_receiver: erx,
        }
    }

    pub fn wait_for_completion(self) -> Result<usize, BulkRequestError> {
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
        builder: JsonBuilder,
    ) -> Result<(), crossbeam::SendError<BulkRequestCommand>> {
        self.check_for_error();

        self.handler.queue_command(BulkRequestCommand::Insert {
            ctid,
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
            ctid,
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
            .queue_command(BulkRequestCommand::DeleteByXmin { ctid, xmin })
    }

    pub fn delete_by_xmax(
        &mut self,
        ctid: pg_sys::ItemPointerData,
        xmax: u64,
    ) -> Result<(), crossbeam::SendError<BulkRequestCommand>> {
        self.check_for_error();

        self.handler
            .queue_command(BulkRequestCommand::DeleteByXmax { ctid, xmax })
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
