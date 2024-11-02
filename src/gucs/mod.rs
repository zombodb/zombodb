use pgrx::prelude::*;
use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};
use std::ffi::CStr;

#[derive(PostgresGucEnum, Clone, Copy, PartialEq, Debug)]
pub enum ZDBLogLevel {
    #[hidden]
    Debug = PgLogLevel::DEBUG2 as isize,
    Debug1 = PgLogLevel::DEBUG1 as isize,
    Debug3 = PgLogLevel::DEBUG3 as isize,
    Debug4 = PgLogLevel::DEBUG4 as isize,
    Debug5 = PgLogLevel::DEBUG5 as isize,
    Info = PgLogLevel::INFO as isize,
    Notice = PgLogLevel::NOTICE as isize,
    Log = PgLogLevel::LOG as isize,
}

impl ZDBLogLevel {
    pub fn log_level(self) -> PgLogLevel {
        match self {
            ZDBLogLevel::Debug => PgLogLevel::DEBUG2,
            ZDBLogLevel::Debug1 => PgLogLevel::DEBUG1,
            ZDBLogLevel::Debug3 => PgLogLevel::DEBUG3,
            ZDBLogLevel::Debug4 => PgLogLevel::DEBUG4,
            ZDBLogLevel::Debug5 => PgLogLevel::DEBUG5,
            ZDBLogLevel::Info => PgLogLevel::INFO,
            ZDBLogLevel::Notice => PgLogLevel::NOTICE,
            ZDBLogLevel::Log => PgLogLevel::LOG,
        }
    }

    pub fn log(self, message: &str) {
        pgrx::ereport!(
            self.log_level(),
            pgrx::PgSqlErrorCode::ERRCODE_SUCCESSFUL_COMPLETION,
            message
        )
    }
}

pub static ZDB_IGNORE_VISIBILITY: GucSetting<bool> = GucSetting::<bool>::new(false);
pub static ZDB_DEFAULT_ROW_ESTIMATE: GucSetting<i32> = GucSetting::<i32>::new(2500);
pub static ZDB_DEFAULT_REPLICAS: GucSetting<i32> = GucSetting::<i32>::new(0);
pub static ZDB_DEFAULT_ELASTICSEARCH_URL: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);
pub static ZDB_LOG_LEVEL: GucSetting<ZDBLogLevel> =
    GucSetting::<ZDBLogLevel>::new(ZDBLogLevel::Debug);
pub static ZDB_ACCELERATOR: GucSetting<bool> = GucSetting::<bool>::new(false);

pub fn init() {
    GucRegistry::define_bool_guc("zdb.ignore_visibility",
                                 "Should queries honor visibility rules?",
                                 "By default, all Elasticsearch search requests apply a MVCC snapshot visibility filter.  Disabling this might provide a slight performance boost at the expense of correct results",
                                 &ZDB_IGNORE_VISIBILITY,
                                 GucContext::Userset, GucFlags::default());

    GucRegistry::define_int_guc("zdb.default_row_estimate",
                                "The default row estimate ZDB should use",
        "ZomboDB needs to provide Postgres with an estimate of the number of rows Elasticsearch will return for any given query. 2500 is a sensible default estimate that generally convinces Postgres to use an IndexScan plan. Setting this to -1 will cause ZomboDB to execute an Elasticsearch _count request for every query to return the exact number.",
        &ZDB_DEFAULT_ROW_ESTIMATE,
        -1, std::i32::MAX, GucContext::Userset, GucFlags::default());

    GucRegistry::define_int_guc(
        "zdb.default_replicas",
        "The default number of index replicas",
        "Defines the number of replicas all new indices should have. Changing this value does not propogate to existing indices.",
        &ZDB_DEFAULT_REPLICAS,
        0,
        32768,
        GucContext::Suset, GucFlags::default());

    GucRegistry::define_string_guc(
        "zdb.default_elasticsearch_url",
        "The default Elasticsearch URL ZomboDB should use if not specified on the index",
        "Defines the default URL for your Elasticsearch cluster so you can elite setting it on every index during CREATE INDEX. The value used must end with a forward slash (/).",
        &ZDB_DEFAULT_ELASTICSEARCH_URL,
        GucContext::Suset, GucFlags::default());

    GucRegistry::define_enum_guc(
        "zdb.log_level",
        "ZomboDB's logging level",
        "The Postgres log level to which ZomboDB emits all of its (non-vacuum) log messages.",
        &ZDB_LOG_LEVEL,
        GucContext::Userset,
        GucFlags::default(),
    );

    GucRegistry::define_bool_guc(
        "zdb.enable_search_accelerator",
        "Do you have ZomboDB's search accelerator installed on your Elasticsearch cluster?",
        "ZomboDB's search accelerator improves the performance of certain types of queries.",
        &ZDB_ACCELERATOR,
        GucContext::Userset,
        GucFlags::default(),
    );
}

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use crate::gucs::{
        ZDBLogLevel, ZDB_DEFAULT_ELASTICSEARCH_URL, ZDB_DEFAULT_REPLICAS, ZDB_DEFAULT_ROW_ESTIMATE,
        ZDB_IGNORE_VISIBILITY, ZDB_LOG_LEVEL,
    };
    use pgrx::*;

    #[pg_test]
    fn test_default_url() {
        assert!(ZDB_DEFAULT_ELASTICSEARCH_URL.get().is_some());
        assert_eq!(
            ZDB_DEFAULT_ELASTICSEARCH_URL.get().unwrap(),
            c"http://localhost:19200/"
        );
    }

    #[pg_test]
    fn test_default_replicas() {
        assert_eq!(ZDB_DEFAULT_REPLICAS.get(), 0);
    }

    #[pg_test]
    fn test_default_row_estimate() -> spi::Result<()> {
        assert_eq!(ZDB_DEFAULT_ROW_ESTIMATE.get(), 2500);
        Spi::run("SET zdb.default_row_estimate TO 42")?;
        assert_eq!(ZDB_DEFAULT_ROW_ESTIMATE.get(), 42);
        Ok(())
    }

    #[pg_test]
    fn test_ignore_visibility() -> spi::Result<()> {
        assert_eq!(ZDB_IGNORE_VISIBILITY.get(), false);
        Spi::run("SET zdb.ignore_visibility TO true")?;
        assert_eq!(ZDB_IGNORE_VISIBILITY.get(), true);
        Ok(())
    }

    #[pg_test]
    fn test_log_level() -> spi::Result<()> {
        assert_eq!(ZDB_LOG_LEVEL.get(), ZDBLogLevel::Debug);
        Spi::run("SET zdb.log_level to 'info'")?;
        assert_eq!(ZDB_LOG_LEVEL.get(), ZDBLogLevel::Info);
        Ok(())
    }
}
