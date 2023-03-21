use crate::access_method::options::RefreshInterval;
use crate::elasticsearch::{Elasticsearch, ElasticsearchError};
use crate::executor_manager::get_executor_manager;
use crate::gucs::ZDB_LOG_LEVEL;
use crate::json::builder::JsonBuilder;
use crossbeam::channel::{RecvTimeoutError, SendTimeoutError};
use dashmap::DashSet;
use pgx::pg_sys::elog::interrupt_pending;
use pgx::*;
use rustc_hash::FxHasher;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::any::Any;
use std::collections::HashSet;
use std::hash::BuildHasherDefault;
use std::io::{Error, ErrorKind, Write};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

#[derive(Debug)]
pub enum BulkRequestCommand<'a> {
    Insert {
        prior_update: Option<Box<BulkRequestCommand<'a>>>,
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
    elasticsearch: Elasticsearch,
    do_refresh: bool,
    queue_size: usize,
    concurrency: usize,
    batch_size: usize,
    error_receiver: crossbeam::channel::Receiver<BulkRequestError>,
}

impl Clone for ElasticsearchBulkRequest {
    fn clone(&self) -> Self {
        ElasticsearchBulkRequest::new(
            &self.elasticsearch,
            self.queue_size,
            self.concurrency,
            self.batch_size,
        )
    }
}

impl ElasticsearchBulkRequest {
    pub fn new(
        elasticsearch: &Elasticsearch,
        queue_size: usize,
        concurrency: usize,
        batch_size: usize,
    ) -> Self {
        let (etx, erx) = crossbeam::channel::bounded(concurrency);

        ElasticsearchBulkRequest {
            handler: Handler::new(
                elasticsearch.clone(),
                queue_size,
                concurrency,
                batch_size,
                etx,
                &erx,
            ),
            elasticsearch: elasticsearch.clone(),
            do_refresh: true,
            queue_size,
            concurrency,
            batch_size,
            error_receiver: erx,
        }
    }

    pub fn finish(mut self) -> Result<(usize, usize), BulkRequestError> {
        self.handler.check_for_error();

        // do we have any deferred commands we need to process again?
        let deferred_commands = std::mem::take(&mut self.handler.deferred);
        let mut deferred_request = None;
        if !deferred_commands.is_empty() {
            deferred_request = Some(self.clone());
        }

        // need to clone the successful_requests counter so we can get an accurate
        // count after we've called .wait_for_completion()
        let successful_requests = self.handler.successful_requests.clone();
        let elasticsearch = self.handler.elasticsearch.clone();

        // wait for the bulk requests to finish
        let mut total_docs = self.handler.wait_for_completion()?;
        let mut nrequests = successful_requests.load(Ordering::SeqCst);

        // requeue any deferred commands
        if deferred_request.is_some() {
            ZDB_LOG_LEVEL.get().log(&format!(
                "[zombodb] requeuing={} deferred commands, index={}",
                deferred_commands.len(),
                self.elasticsearch.base_url()
            ));

            let mut bulk = deferred_request.unwrap();
            bulk.do_refresh = false; // we don't need to do a refresh for this bulk as we'll take care of it below
            deferred_commands.into_iter().for_each(|command| {
                bulk.handler
                    .queue_command_ex(command, true)
                    .expect("failed to queue leftover command")
            });
            let (t, nr) = bulk.finish()?;
            total_docs += t;
            nrequests += nr;
        }

        // now refresh the index if we actually modified it
        if self.do_refresh && total_docs != 0 && nrequests != 0 {
            match elasticsearch.options.refresh_interval() {
                RefreshInterval::Immediate => {
                    ElasticsearchBulkRequest::refresh_index(elasticsearch)?
                }
                RefreshInterval::ImmediateAsync => {
                    std::thread::spawn(|| {
                        ElasticsearchBulkRequest::refresh_index(elasticsearch).ok()
                    });
                }
                RefreshInterval::Background(_) => {
                    // Elasticsearch will do it for us in the future
                }
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
        let terminate = self.handler.terminated.clone();
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
    ) -> Result<(), crossbeam::channel::SendError<BulkRequestCommand>> {
        self.handler.check_for_error();

        let prior_update = self.handler.prior_update.take();
        self.handler.queue_command(BulkRequestCommand::Insert {
            prior_update: prior_update.map(|c| Box::new(c)),
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
    ) -> Result<(), crossbeam::channel::SendError<BulkRequestCommand>> {
        self.handler.check_for_error();

        if self.handler.prior_update.is_some() {
            panic!("Bulk Handler already has a queued prior update tuple")
        }

        // hold onto this, we'll use it during self.insert()
        self.handler.prior_update = Some(BulkRequestCommand::Update {
            ctid: item_pointer_to_u64(ctid),
            cmax: cmax,
            xmax: xmax,
        });

        Ok(())
    }

    pub fn delete(
        &mut self,
        ctid: pg_sys::ItemPointerData,
        cmax: pg_sys::CommandId,
        xmax: u64,
    ) -> Result<(), crossbeam::channel::SendError<BulkRequestCommand>> {
        self.handler.check_for_error();

        let ctid = item_pointer_to_u64(ctid);
        let command = BulkRequestCommand::Update { ctid, cmax, xmax };
        if self.handler.in_flight.contains(&ctid) {
            self.handler.deferred.push(command);
            Ok(())
        } else {
            self.handler.queue_command(command)
        }
    }

    pub fn transaction_in_progress(
        &mut self,
        xid: pg_sys::TransactionId,
    ) -> Result<(), crossbeam::channel::SendError<BulkRequestCommand>> {
        self.handler.check_for_error();

        self.handler
            .queue_command(BulkRequestCommand::TransactionInProgress {
                xid: xid_to_64bit(xid),
            })
    }

    pub fn transaction_committed(
        &mut self,
        xid: pg_sys::TransactionId,
    ) -> Result<(), crossbeam::channel::SendError<BulkRequestCommand>> {
        self.handler.check_for_error();

        // the transaction committed command needs to be the last command we send
        // so we busy-loop wait until there's only 1 (or zero) active threads
        // and then we send the command so it'll be the last command
        while self.handler.active_threads.load(Ordering::SeqCst) > 1 {
            check_for_interrupts!();
            std::thread::yield_now();
        }

        self.handler
            .queue_command(BulkRequestCommand::TransactionCommitted {
                xid: xid_to_64bit(xid),
            })
    }

    pub fn delete_by_xmin(
        &mut self,
        ctid: u64,
        xmin: u64,
    ) -> Result<(), crossbeam::channel::SendError<BulkRequestCommand>> {
        self.handler.check_for_error();

        self.handler
            .queue_command(BulkRequestCommand::DeleteByXmin { ctid, xmin })
    }

    pub fn delete_by_xmax(
        &mut self,
        ctid: u64,
        xmax: u64,
    ) -> Result<(), crossbeam::channel::SendError<BulkRequestCommand>> {
        self.handler.check_for_error();

        self.handler
            .queue_command(BulkRequestCommand::DeleteByXmax { ctid, xmax })
    }

    pub fn vacuum_xmax(
        &mut self,
        ctid: u64,
        xmax: u64,
    ) -> Result<(), crossbeam::channel::SendError<BulkRequestCommand>> {
        self.handler.check_for_error();

        self.handler
            .queue_command(BulkRequestCommand::VacuumXmax { ctid, xmax })
    }

    pub fn remove_aborted_xids(
        &mut self,
        xids: Vec<u64>,
    ) -> Result<(), crossbeam::channel::SendError<BulkRequestCommand>> {
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
    when_started: Instant,
    terminated: Arc<AtomicBool>,
    threads: Vec<Option<JoinHandle<usize>>>,
    prior_update: Option<BulkRequestCommand<'static>>,
    in_flight: Arc<DashSet<u64, BuildHasherDefault<FxHasher>>>,
    deferred: Vec<BulkRequestCommand<'static>>,
    total_docs: usize,
    active_threads: Arc<AtomicUsize>,
    successful_requests: Arc<AtomicUsize>,
    elasticsearch: Elasticsearch,
    concurrency: usize,
    batch_size: usize,
    queue_size: usize,
    bulk_sender: Option<crossbeam::channel::Sender<BulkRequestCommand<'static>>>,
    bulk_receiver: crossbeam::channel::Receiver<BulkRequestCommand<'static>>,
    error_sender: crossbeam::channel::Sender<BulkRequestError>,
    error_receiver: crossbeam::channel::Receiver<BulkRequestError>,
    current_xid: Option<pg_sys::TransactionId>,
}

struct BulkReceiver<'a> {
    terminated: Arc<AtomicBool>,
    first: Option<BulkRequestCommand<'a>>,
    consumed: HashSet<u64>,
    receiver: crossbeam::channel::Receiver<BulkRequestCommand<'a>>,
    bytes_out: usize,
    docs_out: usize,
    buffer: Vec<u8>,
    buffer_offset: usize,
    batch_size: usize,
    queue_size: usize,
}

impl<'a> std::io::Read for BulkReceiver<'a> {
    fn read(&mut self, mut buf: &mut [u8]) -> Result<usize, Error> {
        // were we asked to terminate?
        if self.terminated.load(Ordering::SeqCst) {
            // indicate that our reader "pipe" has been broken and there's nothing else we can do
            return Err(Error::new(ErrorKind::BrokenPipe, "terminated"));
        }

        // write out whatever might be remaining in our internal buffer
        if self.buffer_offset < self.buffer.len() {
            let amt = buf.write(&self.buffer[self.buffer_offset..])?;
            self.buffer_offset += amt;
            self.bytes_out += amt;

            // and we'll keep doing that before we take something else off the queue
            return Ok(amt);
        } else {
            // we've written the entirety of our buffer so clear it out and reset our offset
            self.buffer_offset = 0;
            self.buffer.clear();
        }

        let command = match self.first.take() {
            // take our first command
            Some(command) => Some(command),

            // otherwise if we have room, try to pull one from the receiver
            None if self.docs_out < self.queue_size && self.bytes_out < self.batch_size => {
                // we don't care if there's an error trying to receive
                self.receiver
                    .recv_timeout(Duration::from_millis(333))
                    .map_or(None, |command| Some(command))
            }

            // we got nothing!
            None => None,
        };

        match command {
            Some(command) => {
                // serialize this comment (into `self.buffer`)
                self.serialize_command(command);

                // and start to write out what we can
                // our buffer_offset should be zero here as the prior contents
                // of the buffer should have been written above
                assert_eq!(self.buffer_offset, 0);
                let amt = buf.write(&self.buffer)?;
                if amt > 0 {
                    self.buffer_offset += amt;
                    self.bytes_out += amt;
                } else {
                    self.buffer.clear();
                    self.buffer_offset = 0;
                }

                Ok(amt)
            }
            None => Ok(0),
        }
    }
}

impl<'a> BulkReceiver<'a> {
    fn serialize_command(&mut self, command: BulkRequestCommand<'a>) {
        self.docs_out += 1;

        // build json of this entire command and store in self.bytes
        match command {
            BulkRequestCommand::Insert {
                prior_update,
                ctid,
                cmin,
                cmax,
                xmin,
                xmax,
                builder: mut doc,
            } => {
                // remember that we've processed this ctid
                self.consumed.insert(ctid);

                // do the prior ::Update that might correspond to this
                // ::Insert if we have one
                if prior_update.is_some() {
                    self.serialize_command(*prior_update.unwrap());
                }

                serde_json::to_writer(
                    &mut self.buffer,
                    &json! {
                        {"index": {"_id": ctid } }
                    },
                )
                .expect("failed to serialize index line");
                self.buffer.push(b'\n');

                doc.add_u64("\"zdb_ctid\"", ctid);
                doc.add_u32("\"zdb_cmin\"", cmin);
                if cmax as pg_sys::CommandId != pg_sys::InvalidCommandId {
                    doc.add_u32("\"zdb_cmax\"", cmax);
                }
                doc.add_u64("\"zdb_xmin\"", xmin);
                if xmax as pg_sys::TransactionId != pg_sys::InvalidTransactionId {
                    doc.add_u64("\"zdb_xmax\"", xmax);
                }

                doc.build(&mut self.buffer);
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

impl Handler {
    pub(crate) fn new(
        elasticsearch: Elasticsearch,
        queue_size: usize,
        concurrency: usize,
        batch_size: usize,
        error_sender: crossbeam::channel::Sender<BulkRequestError>,
        error_receiver: &crossbeam::channel::Receiver<BulkRequestError>,
    ) -> Self {
        let (tx, rx) = crossbeam::channel::bounded(concurrency);

        Handler {
            when_started: Instant::now(),
            terminated: Arc::new(AtomicBool::new(false)),
            threads: Vec::new(),
            prior_update: None,
            in_flight: Arc::new(DashSet::default()),
            deferred: Default::default(),
            total_docs: 0,
            active_threads: Arc::new(AtomicUsize::new(0)),
            successful_requests: Arc::new(AtomicUsize::new(0)),
            elasticsearch,
            batch_size,
            queue_size,
            concurrency,
            bulk_sender: Some(tx),
            bulk_receiver: rx,
            error_sender,
            error_receiver: error_receiver.clone(),
            current_xid: None,
        }
    }

    pub fn queue_command(
        &mut self,
        command: BulkRequestCommand<'static>,
    ) -> Result<(), crossbeam::channel::SendError<BulkRequestCommand<'static>>> {
        self.queue_command_ex(command, false)
    }

    pub fn queue_command_ex(
        &mut self,
        mut command: BulkRequestCommand<'static>,
        is_deferred: bool,
    ) -> Result<(), crossbeam::channel::SendError<BulkRequestCommand<'static>>> {
        if is_deferred == false && self.current_xid.is_none() {
            match &command {
                BulkRequestCommand::Insert { .. } | BulkRequestCommand::Update { .. } => {
                    let current_xid = unsafe { pg_sys::GetCurrentTransactionId() };
                    if current_xid != pg_sys::InvalidTransactionId {
                        get_executor_manager().push_xid(current_xid);
                    }
                    self.current_xid.replace(current_xid);
                }
                _ => {}
            }
        }

        if let BulkRequestCommand::Insert {
            prior_update, ctid, ..
        } = &command
        {
            // record that this insert is now "in flight"
            // so that future updates to it will get deferred
            // instead of queued
            self.in_flight.insert(*ctid);

            if let Some(prior_update) = prior_update.as_ref() {
                if let BulkRequestCommand::Update { ctid, .. } = prior_update.as_ref() {
                    if self.in_flight.contains(ctid) {
                        // trying to update a ctid that's currently in flight,
                        // so we defer the entire command
                        self.deferred.push(command);
                        return Ok(());
                    }
                }
            }
        }

        // try to send the command to a background thread
        while let Err(e) = self
            .bulk_sender
            .as_ref()
            .unwrap()
            .send_timeout(command, std::time::Duration::from_secs(10))
        {
            match e {
                // the channel is full, so lets see if we can figure out why?
                SendTimeoutError::Timeout(full_command) => {
                    if self.terminated.load(Ordering::SeqCst) {
                        // We're all done because we've been asked to terminate
                        //
                        // Drop the sender and ultimately return an error
                        //
                        // The reason could be an error from a background thread or it could
                        // be through a Postgres command interrupt (^C)
                        //
                        // We drop the bulk_sender to ensure that if there's a subsequent call
                        // to .queue_command(), it'll just fail with a TrySendError::Disconnected.
                        // In practice this shouldn't happen as callers to .queue_command() should
                        // be properly handling the Result
                        drop(self.bulk_sender.take());

                        self.check_for_error(); // this will just panic if there's an error

                        // if there's not an error, we'll just return a generic error
                        // because we've been asked to terminate so there's no point in trying again
                        return Err(crossbeam::channel::SendError(full_command));
                    }

                    // we'll loop around and try again
                    command = full_command;
                    if interrupt_pending() {
                        panic!("detected interrupt from Postgres");
                    }
                }

                // the channel is disconnected, so return an error
                SendTimeoutError::Disconnected(disconnected_command) => {
                    return Err(crossbeam::channel::SendError(disconnected_command));
                }
            }
        }

        // now determine if we need to start a new thread to handle what's in the queue
        let nthreads = self.active_threads.load(Ordering::SeqCst);
        if self.total_docs > 0 && self.total_docs % self.queue_size == 0 {
            ZDB_LOG_LEVEL.get().log(&format!(
                "[zombodb] total={}, in_flight={}, queued={}, active_threads={}, index={}, elapsed={}",
                self.total_docs,
                self.in_flight.len(),
                self.bulk_receiver.len(),
                nthreads,
                self.elasticsearch.base_url(),
                humantime::Duration::from(self.when_started.elapsed())
            ));
        }
        if nthreads == 0
            || (nthreads < self.concurrency
                && self.total_docs % (self.queue_size / self.concurrency) == 0)
        {
            self.threads.push(Some(self.create_thread(nthreads)));
        }

        self.total_docs += 1;
        Ok(())
    }

    fn create_thread(&self, _thread_id: usize) -> JoinHandle<usize> {
        let base_url = self.elasticsearch.base_url();
        let bulk_receiver = self.bulk_receiver.clone();
        let in_flight = self.in_flight.clone();
        let error = self.error_sender.clone();
        let terminated = self.terminated.clone();
        let batch_size = self.batch_size;
        let queue_size = self.queue_size;
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

                let first = match bulk_receiver.recv_timeout(Duration::from_millis(333)) {
                    Ok(command) => Some(command),
                    Err(e) => {
                        match e {
                            // we timed out trying to receive the first command, so we'll end up
                            // looping back ground to try again
                            RecvTimeoutError::Timeout => None,

                            // we don't have a first command to deal with on this iteration b/c
                            // the channel has been shutdown.  we're simply out of records
                            // and can safely break out
                            RecvTimeoutError::Disconnected => {
                                break;
                            }
                        }
                    }
                };

                let mut docs_out = 0;
                if first.is_some() {
                    let mut reader = BulkReceiver {
                        terminated: terminated.clone(),
                        first,
                        consumed: Default::default(),
                        receiver: bulk_receiver.clone(),
                        batch_size,
                        queue_size,
                        bytes_out: 0,
                        docs_out: 0,
                        buffer: Vec::with_capacity(16384),
                        buffer_offset: 0,
                    };

                    let url = format!(
                        "{}/_bulk?format=cbor&filter_path={}",
                        base_url, BULK_FILTER_PATH
                    );

                    let response = catch_unwind(AssertUnwindSafe(|| {
                        Elasticsearch::execute_request(
                            Elasticsearch::client()
                                .post(&url)
                                .set("content-type", "application/json"),
                            &mut reader,
                            |body| {
                                #[derive(Serialize, Deserialize, Debug)]
                                struct ErrorObject {
                                    reason: String,
                                }

                                #[derive(Serialize, Deserialize, Debug)]
                                struct BulkResponse {
                                    error: Option<ErrorObject>,
                                    errors: Option<bool>,
                                    items: Option<Vec<Value>>,
                                }

                                // NB:  this is stupid that ES forces us to parse the response for requests
                                // that contain an error, but here we are
                                let result: serde_cbor::Result<BulkResponse> =
                                    serde_cbor::from_reader(body);
                                match result {
                                    // result deserialized okay, lets see if it's what we need
                                    Ok(response) => {
                                        if !response.errors.unwrap_or(false)
                                            && response.error.is_none()
                                        {
                                            successful_requests.fetch_add(1, Ordering::SeqCst);
                                            Ok(())
                                        } else {
                                            // yup, the response contains an error
                                            Err(ElasticsearchError(
                                                Some(200), // but it was given to us as a 200 OK, otherwise we wouldn't be here at all
                                                match serde_json::to_string(&response) {
                                                    Ok(s) => s,
                                                    Err(e) => format!("{:?}", e),
                                                },
                                            ))
                                        }
                                    }

                                    // couldn't deserialize the result
                                    Err(e) => {
                                        Err(ElasticsearchError(Some(200), format!("{:?}", e)))
                                    }
                                }
                            },
                        )
                    }));

                    let response = match response {
                        Ok(response) => response,
                        Err(e) => {
                            // the machinery behind `Elasticsearch::execute_request()` caused a panic, and
                            // we caught it.  So we're done.  Drop the receiver...
                            drop(bulk_receiver);

                            // ... and send the error back to the main thread
                            terminated.store(true, Ordering::SeqCst);
                            Handler::send_error(error, None, &format!("{:?}", downcast_err(e)));
                            break;
                        }
                    };

                    if let Err(e) = response {
                        // we received an error, so there's no need for any other active thread to expect
                        // to be able to use the receiver anymore
                        drop(bulk_receiver);

                        // send the error back to the main thread
                        terminated.store(true, Ordering::SeqCst);
                        Handler::send_error(error, e.status(), e.message());
                        break;
                    }

                    // remove from our set of "in flight" ctids those that we consumed
                    // during this request
                    in_flight.retain(|v| !reader.consumed.contains(v));

                    docs_out = reader.docs_out;
                    total_docs_out += docs_out;
                }

                if docs_out == 0 {
                    // we didn't output any docs, which likely means there's no more in the channel
                    // to process, so get out if the receiver is also empty
                    if bulk_receiver.is_empty() {
                        break;
                    }
                }
            }

            active_threads.fetch_sub(1, Ordering::SeqCst);
            total_docs_out
        })
    }

    fn send_error(
        sender: crossbeam::channel::Sender<BulkRequestError>,
        code: Option<u16>,
        message: &str,
    ) {
        sender
            .send(BulkRequestError::IndexingError(format!(
                "code={:?}, {}",
                code, message
            )))
            .ok(); // best attempt to send the error
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
                    self.check_for_error();
                    cnt += many;
                }
                Err(e) => panic!("Got an error joining on a thread: {}", downcast_err(e)),
            }
        }

        Ok(cnt)
    }

    pub(crate) fn terminate(&self) {
        self.terminated.store(true, Ordering::SeqCst);
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
            panic!("detected interrupt from Postgres");
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
