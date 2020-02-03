#![allow(dead_code)]
use crate::json::builder::JsonBuilder;
use pgx::*;
use std::sync::atomic::Ordering;

mod bulk;

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
