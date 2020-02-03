use memoffset::*;
use pgx::*;
use std::ffi::CStr;

#[repr(C)]
pub struct ZDBIndexOptions {
    /* varlena header (do not touch directly!) */
    #[allow(dead_code)]
    vl_len_: i32,

    url_offset: i32,
    type_name_offset: i32,
    refresh_interval_offset: i32,
    alias_offset: i32,
    uuid_offset: i32,

    optimize_after: i32,
    compression_level: i32,
    shards: i32,
    replicas: i32,
    bulk_concurrency: i32,
    batch_size: i32,
    llapi: bool,
}

#[allow(dead_code)]
impl ZDBIndexOptions {
    pub unsafe fn from(relation: &pg_sys::RelationData) -> &Self {
        if relation.rd_index.is_null() {
            panic!("relation doesn't represent an index")
        } else if relation.rd_options.is_null() {
            panic!("no rd_options on index");
        }

        let ops = relation.rd_options;
        (ops as *mut ZDBIndexOptions).as_ref().unwrap()
    }

    pub fn optimize_after(&self) -> i32 {
        self.optimize_after
    }

    pub fn compression_level(&self) -> i32 {
        self.compression_level
    }

    pub fn shards(&self) -> i32 {
        self.shards
    }

    pub fn replicas(&self) -> i32 {
        self.replicas
    }

    pub fn bulk_concurrency(&self) -> i32 {
        self.bulk_concurrency
    }

    pub fn batch_size(&self) -> i32 {
        self.batch_size
    }

    pub fn llapi(&self) -> bool {
        self.llapi
    }

    pub fn url(&self) -> Option<&str> {
        self.get_str(self.url_offset)
    }

    pub fn type_name(&self) -> Option<&str> {
        self.get_str(self.type_name_offset)
    }

    pub fn refresh_interval(&self) -> Option<&str> {
        self.get_str(self.refresh_interval_offset)
    }

    pub fn alias(&self) -> Option<&str> {
        self.get_str(self.alias_offset)
    }

    pub fn uuid(&self) -> Option<&str> {
        self.get_str(self.uuid_offset)
    }

    fn get_str(&self, offset: i32) -> Option<&str> {
        if offset <= 0 {
            None
        } else {
            let opts = self as *const _ as void_ptr as usize;
            let value =
                unsafe { CStr::from_ptr((opts + offset as usize) as *const std::os::raw::c_char) };

            Some(value.to_str().unwrap())
        }
    }
}

static ZDB_DEFAULT_REPLICAS_GUC: i32 = 0;
static mut RELOPT_KIND_ZDB: pg_sys::relopt_kind = 0;

extern "C" fn validate_url(url: *const std::os::raw::c_char) {
    let url = unsafe { CStr::from_ptr(url) }
        .to_str()
        .expect("failed to convert url to utf8");

    if url == "default" {
        // "default" is a fine value
        return;
    }

    if !url.ends_with('/') {
        panic!("url must end with a forward slash");
    }
}

#[pg_guard]
pub unsafe extern "C" fn amoptions(
    reloptions: pg_sys::Datum,
    validate: bool,
) -> *mut pg_sys::bytea {
    // TODO:  how to make this const?  we can't use offset_of!() macro in const definitions, apparently
    let tab: [pg_sys::relopt_parse_elt; 12] = [
        pg_sys::relopt_parse_elt {
            optname: CStr::from_bytes_with_nul_unchecked(b"url\0").as_ptr(),
            opttype: pg_sys::relopt_type_RELOPT_TYPE_STRING,
            offset: offset_of!(ZDBIndexOptions, url_offset) as i32,
        },
        pg_sys::relopt_parse_elt {
            optname: CStr::from_bytes_with_nul_unchecked(b"type_name\0").as_ptr(),
            opttype: pg_sys::relopt_type_RELOPT_TYPE_STRING,
            offset: offset_of!(ZDBIndexOptions, type_name_offset) as i32,
        },
        pg_sys::relopt_parse_elt {
            optname: CStr::from_bytes_with_nul_unchecked(b"refresh_interval\0").as_ptr(),
            opttype: pg_sys::relopt_type_RELOPT_TYPE_STRING,
            offset: offset_of!(ZDBIndexOptions, refresh_interval_offset) as i32,
        },
        pg_sys::relopt_parse_elt {
            optname: CStr::from_bytes_with_nul_unchecked(b"shards\0").as_ptr(),
            opttype: pg_sys::relopt_type_RELOPT_TYPE_INT,
            offset: offset_of!(ZDBIndexOptions, shards) as i32,
        },
        pg_sys::relopt_parse_elt {
            optname: CStr::from_bytes_with_nul_unchecked(b"replicas\0").as_ptr(),
            opttype: pg_sys::relopt_type_RELOPT_TYPE_INT,
            offset: offset_of!(ZDBIndexOptions, replicas) as i32,
        },
        pg_sys::relopt_parse_elt {
            optname: CStr::from_bytes_with_nul_unchecked(b"bulk_concurrency\0").as_ptr(),
            opttype: pg_sys::relopt_type_RELOPT_TYPE_INT,
            offset: offset_of!(ZDBIndexOptions, bulk_concurrency) as i32,
        },
        pg_sys::relopt_parse_elt {
            optname: CStr::from_bytes_with_nul_unchecked(b"batch_size\0").as_ptr(),
            opttype: pg_sys::relopt_type_RELOPT_TYPE_INT,
            offset: offset_of!(ZDBIndexOptions, batch_size) as i32,
        },
        pg_sys::relopt_parse_elt {
            optname: CStr::from_bytes_with_nul_unchecked(b"compression_level\0").as_ptr(),
            opttype: pg_sys::relopt_type_RELOPT_TYPE_INT,
            offset: offset_of!(ZDBIndexOptions, compression_level) as i32,
        },
        pg_sys::relopt_parse_elt {
            optname: CStr::from_bytes_with_nul_unchecked(b"alias\0").as_ptr(),
            opttype: pg_sys::relopt_type_RELOPT_TYPE_STRING,
            offset: offset_of!(ZDBIndexOptions, alias_offset) as i32,
        },
        pg_sys::relopt_parse_elt {
            optname: CStr::from_bytes_with_nul_unchecked(b"optimize_after\0").as_ptr(),
            opttype: pg_sys::relopt_type_RELOPT_TYPE_INT,
            offset: offset_of!(ZDBIndexOptions, optimize_after) as i32,
        },
        pg_sys::relopt_parse_elt {
            optname: CStr::from_bytes_with_nul_unchecked(b"llapi\0").as_ptr(),
            opttype: pg_sys::relopt_type_RELOPT_TYPE_BOOL,
            offset: offset_of!(ZDBIndexOptions, llapi) as i32,
        },
        pg_sys::relopt_parse_elt {
            optname: CStr::from_bytes_with_nul_unchecked(b"uuid\0").as_ptr(),
            opttype: pg_sys::relopt_type_RELOPT_TYPE_STRING,
            offset: offset_of!(ZDBIndexOptions, uuid_offset) as i32,
        },
    ];

    let mut noptions = 0;
    let options = pg_sys::parseRelOptions(reloptions, validate, RELOPT_KIND_ZDB, &mut noptions);
    if noptions == 0 {
        return std::ptr::null_mut();
    }

    for relopt in std::slice::from_raw_parts_mut(options, noptions as usize) {
        relopt.gen.as_mut().unwrap().lockmode = pg_sys::AccessShareLock as pg_sys::LOCKMODE;
    }

    let rdopts =
        pg_sys::allocateReloptStruct(std::mem::size_of::<ZDBIndexOptions>(), options, noptions);
    pg_sys::fillRelOptions(
        rdopts,
        std::mem::size_of::<ZDBIndexOptions>(),
        options,
        noptions,
        validate,
        tab.as_ptr(),
        tab.len() as i32,
    );
    pg_sys::pfree(options as void_mut_ptr);

    rdopts as *mut pg_sys::bytea
}

pub unsafe fn init() {
    RELOPT_KIND_ZDB = pg_sys::add_reloption_kind();
    pg_sys::add_string_reloption(
        RELOPT_KIND_ZDB,
        CStr::from_bytes_with_nul_unchecked(b"url\0").as_ptr(),
        CStr::from_bytes_with_nul_unchecked(b"Server URL and port\0").as_ptr(),
        CStr::from_bytes_with_nul_unchecked(b"default\0").as_ptr(),
        Some(validate_url),
    );
    pg_sys::add_string_reloption(
        RELOPT_KIND_ZDB,
        CStr::from_bytes_with_nul_unchecked(b"type_name\0").as_ptr(),
        CStr::from_bytes_with_nul_unchecked(
            b"What Elasticsearch index type name should ZDB use?  Default is 'doc'\0",
        )
        .as_ptr(),
        CStr::from_bytes_with_nul_unchecked(b"doc\0").as_ptr(),
        None,
    );
    pg_sys::add_string_reloption(RELOPT_KIND_ZDB, CStr::from_bytes_with_nul_unchecked(b"refresh_interval\0").as_ptr(),
                                 CStr::from_bytes_with_nul_unchecked(b"Frequency in which Elasticsearch indexes are refreshed.  Related to ES' index.refresh_interval setting\0").as_ptr(),
                                 CStr::from_bytes_with_nul_unchecked(b"-1\0").as_ptr(), None);
    pg_sys::add_int_reloption(
        RELOPT_KIND_ZDB,
        CStr::from_bytes_with_nul_unchecked(b"shards\0").as_ptr(),
        CStr::from_bytes_with_nul_unchecked(b"The number of shards for the index\0").as_ptr(),
        5,
        1,
        32768,
    );
    pg_sys::add_int_reloption(
        RELOPT_KIND_ZDB,
        CStr::from_bytes_with_nul_unchecked(b"replicas\0").as_ptr(),
        CStr::from_bytes_with_nul_unchecked(b"The number of replicas for the index\0").as_ptr(),
        ZDB_DEFAULT_REPLICAS_GUC,
        0,
        32768,
    );
    pg_sys::add_int_reloption(
        RELOPT_KIND_ZDB,
        CStr::from_bytes_with_nul_unchecked(b"bulk_concurrency\0").as_ptr(),
        CStr::from_bytes_with_nul_unchecked(
            b"The maximum number of concurrent _bulk API requests\0",
        )
        .as_ptr(),
        12,
        1,
        num_cpus::get().max(1) as i32,
    );
    pg_sys::add_int_reloption(
        RELOPT_KIND_ZDB,
        CStr::from_bytes_with_nul_unchecked(b"batch_size\0").as_ptr(),
        CStr::from_bytes_with_nul_unchecked(b"The size in bytes of batch calls to the _bulk API\0")
            .as_ptr(),
        1024 * 1024 * 8,
        1024,
        (std::i32::MAX / 2) - 1,
    );
    pg_sys::add_int_reloption(
        RELOPT_KIND_ZDB,
        CStr::from_bytes_with_nul_unchecked(b"compression_level\0").as_ptr(),
        CStr::from_bytes_with_nul_unchecked(
            b"0-9 value to indicate the level of HTTP compression\0",
        )
        .as_ptr(),
        1,
        0,
        9,
    );
    pg_sys::add_string_reloption(
        RELOPT_KIND_ZDB,
        CStr::from_bytes_with_nul_unchecked(b"alias\0").as_ptr(),
        CStr::from_bytes_with_nul_unchecked(
            b"The Elasticsearch Alias to which this index should belong\0",
        )
        .as_ptr(),
        std::ptr::null(),
        None,
    );
    pg_sys::add_string_reloption(
        RELOPT_KIND_ZDB,
        CStr::from_bytes_with_nul_unchecked(b"uuid\0").as_ptr(),
        CStr::from_bytes_with_nul_unchecked(b"The Elasticsearch index name, as a UUID\0").as_ptr(),
        std::ptr::null(),
        None,
    );
    pg_sys::add_int_reloption(
        RELOPT_KIND_ZDB,
        CStr::from_bytes_with_nul_unchecked(b"optimize_after\0").as_ptr(),
        CStr::from_bytes_with_nul_unchecked(
            b"After how many deleted docs should ZDB _optimize the ES index during VACUUM?\0",
        )
        .as_ptr(),
        0,
        0,
        std::i32::MAX,
    );
    pg_sys::add_bool_reloption(
        RELOPT_KIND_ZDB,
        CStr::from_bytes_with_nul_unchecked(b"llapi\0").as_ptr(),
        CStr::from_bytes_with_nul_unchecked(
            b"Will this index be used by ZomboDB's low-level API?\0",
        )
        .as_ptr(),
        false,
    );
}

#[cfg(any(test, feature = "pg_test"))]
mod tests {
    use crate::access_method::options::{validate_url, ZDBIndexOptions};
    use pgx::*;
    use std::ffi::CString;

    #[test]
    fn make_idea_happy() {}

    #[pg_test]
    fn test_validate_url() {
        validate_url(CString::new("http://localhost:9200/").unwrap().as_ptr());
    }

    #[pg_test(error = "url must end with a forward slash")]
    fn test_validate_invalid_url() {
        validate_url(CString::new("http://localhost:9200").unwrap().as_ptr());
    }

    #[pg_test]
    unsafe fn test_index_options() {
        Spi::run(
            "CREATE TABLE test();  
        CREATE INDEX idxtest 
                  ON test 
               USING zombodb ((test.*)) 
                WITH (url='localhost:9200/', 
                      type_name='test_type_name', 
                      alias='test_alias', 
                      uuid='test_uuid', 
                      refresh_interval='5s'); ",
        );

        let index_oid = Spi::get_one::<pg_sys::Oid>("SELECT 'idxtest'::regclass::oid")
            .expect("failed to get SPI result");
        let indexrel = pg_sys::RelationIdGetRelation(index_oid);
        let options = ZDBIndexOptions::from(&indexrel.as_ref().unwrap());
        assert_eq!(options.url().unwrap(), "localhost:9200/");
        assert_eq!(options.type_name().unwrap(), "test_type_name");
        assert_eq!(options.alias().unwrap(), "test_alias");
        assert_eq!(options.uuid().unwrap(), "test_uuid");
        assert_eq!(options.refresh_interval().unwrap(), "5s");
        pg_sys::RelationClose(indexrel);
    }
}
