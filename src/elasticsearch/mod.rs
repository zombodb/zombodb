use pgx::*;
use serde_json::Value;

pub struct Elasticsearch {
    url: url::Url,
    index_name: String,
}

enum ElasticsearchBulkRequestType {
    Insert {
        ctid: pg_sys::ItemPointerData,
        cmin: pg_sys::CommandId,
        cmax: pg_sys::CommandId,
        xmin: u64,
        xmax: u64,
        doc: Value,
    },
    Update {
        ctid: pg_sys::ItemPointerData,
        cmax: pg_sys::CommandId,
        xmax: u64,
        doc: Value,
    },
    DeleteByXmin {
        ctid: pg_sys::ItemPointerData,
        xmin: u64,
    },
    DeleteByXmax {
        ctid: pg_sys::ItemPointerData,
        xmax: u64,
    },
}
pub struct ElasticsearchBulkRequest {
    elasticsearch: Elasticsearch,
    commands: Vec<ElasticsearchBulkRequestType>,
}

impl Elasticsearch {
    pub fn start_bulk(self) -> ElasticsearchBulkRequest {
        ElasticsearchBulkRequest {
            elasticsearch: self,
            commands: Vec::with_capacity(10_000),
        }
    }
}

impl ElasticsearchBulkRequest {
    pub fn insert(
        &mut self,
        ctid: pg_sys::ItemPointerData,
        cmin: pg_sys::CommandId,
        cmax: pg_sys::CommandId,
        xmin: u64,
        xmax: u64,
        doc: Value,
    ) {
        self.commands.push(ElasticsearchBulkRequestType::Insert {
            ctid,
            cmin,
            cmax,
            xmin,
            xmax,
            doc,
        });
    }

    pub fn update(
        &mut self,
        ctid: pg_sys::ItemPointerData,
        cmax: pg_sys::CommandId,
        xmax: u64,
        doc: Value,
    ) {
        self.commands.push(ElasticsearchBulkRequestType::Update {
            ctid,
            cmax,
            xmax,
            doc,
        });
    }

    pub fn delete_by_xmin(&mut self, ctid: pg_sys::ItemPointerData, xmin: u64) {
        self.commands
            .push(ElasticsearchBulkRequestType::DeleteByXmin { ctid, xmin });
    }

    pub fn delete_by_xmax(&mut self, ctid: pg_sys::ItemPointerData, xmax: u64) {
        self.commands
            .push(ElasticsearchBulkRequestType::DeleteByXmax { ctid, xmax });
    }
}
