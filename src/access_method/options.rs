use crate::elasticsearch::Elasticsearch;
use crate::gucs::{ZDB_DEFAULT_ELASTICSEARCH_URL, ZDB_DEFAULT_REPLICAS};
use crate::utils::find_zdb_index;
use crate::zql::ast::{IndexLink, QualifiedField};
use crate::zql::transformations::field_finder::find_link_for_field;
use crate::zql::{parse_field_lists, INDEX_LINK_PARSER};
use lazy_static::*;
use memoffset::*;
use pgx::pg_sys::AsPgCStr;
use pgx::*;
use std::collections::{HashMap, HashSet};
use std::ffi::CStr;
use std::fmt::Debug;

const DEFAULT_BATCH_SIZE: i32 = 8 * 1024 * 1024;
const DEFAULT_COMPRESSION_LEVEL: i32 = 1;
const DEFAULT_SHARDS: i32 = 5;
const DEFAULT_OPTIMIZE_AFTER: i32 = 0;
const DEFAULT_MAX_RESULT_WINDOW: i32 = 10000;
const DEFAULT_NESTED_FIELDS_LIMIT: i32 = 1000;
const DEFAULT_NESTED_OBJECTS_LIMIT: i32 = 10000;
const DEFAULT_TOTAL_FIELDS_LIMIT: i32 = 1000;
const DEFAULT_MAX_TERMS_COUNT: i32 = 65535;
const DEFAULT_MAX_ANALYZE_TOKEN_COUNT: i32 = 10000;
const DEFAULT_URL: &str = "default";
const DEFAULT_TYPE_NAME: &str = "doc";
const DEFAULT_REFRESH_INTERVAL: &str = "-1";
const DEFAULT_TRANSLOG_DURABILITY: &str = "request";

lazy_static! {
    static ref DEFAULT_BULK_CONCURRENCY: i32 = num_cpus::get() as i32;
}

#[derive(Eq, PartialEq, Debug, Clone)]
pub enum RefreshInterval {
    Immediate,
    ImmediateAsync,
    Background(String),
}

impl RefreshInterval {
    pub fn as_str(&self) -> &str {
        match self {
            RefreshInterval::Immediate => "-1",
            RefreshInterval::ImmediateAsync => "-1",
            RefreshInterval::Background(s) => s.as_str(),
        }
    }
}

#[repr(C)]
struct ZDBIndexOptionsInternal {
    /* varlena header (do not touch directly!) */
    #[allow(dead_code)]
    vl_len_: i32,

    url_offset: i32,
    type_name_offset: i32,
    refresh_interval_offset: i32,
    nested_fields_limit: i32,
    nested_objects_limit: i32,
    total_fields_limit: i32,
    max_terms_count: i32,
    max_analyze_token_count: i32,
    alias_offset: i32,
    uuid_offset: i32,
    translog_durability_offset: i32,
    options_offset: i32,
    field_lists_offset: i32,
    shadow_index: bool,

    max_result_window: i32,
    optimize_after: i32,
    compression_level: i32,
    shards: i32,
    replicas: i32,
    bulk_concurrency: i32,
    batch_size: i32,
    llapi: bool,

    nested_object_date_detection: bool,
    nested_object_numeric_detection: bool,
    nested_object_text_mapping_offset: i32,

    include_source: bool,
}

#[allow(dead_code)]
impl ZDBIndexOptionsInternal {
    fn from_relation(relation: &PgRelation) -> PgBox<ZDBIndexOptionsInternal> {
        if relation.rd_index.is_null() {
            panic!("'{}' is not a ZomboDB index", relation.name())
        } else if relation.rd_options.is_null() {
            // use defaults
            let mut ops = PgBox::<ZDBIndexOptionsInternal>::alloc0();
            ops.compression_level = DEFAULT_COMPRESSION_LEVEL;
            ops.shards = DEFAULT_SHARDS;
            ops.replicas = ZDB_DEFAULT_REPLICAS.get();
            ops.bulk_concurrency = *DEFAULT_BULK_CONCURRENCY;
            ops.batch_size = DEFAULT_BATCH_SIZE;
            ops.optimize_after = DEFAULT_OPTIMIZE_AFTER;
            ops.max_result_window = DEFAULT_MAX_RESULT_WINDOW;
            ops.nested_fields_limit = DEFAULT_NESTED_FIELDS_LIMIT;
            ops.nested_objects_limit = DEFAULT_NESTED_OBJECTS_LIMIT;
            ops.total_fields_limit = DEFAULT_TOTAL_FIELDS_LIMIT;
            ops.max_terms_count = DEFAULT_MAX_TERMS_COUNT;
            ops.max_analyze_token_count = DEFAULT_MAX_ANALYZE_TOKEN_COUNT;
            ops.nested_object_date_detection = false;
            ops.nested_object_numeric_detection = false;
            ops.include_source = true;
            ops.into_pg_boxed()
        } else {
            unsafe { PgBox::from_pg(relation.rd_options as *mut ZDBIndexOptionsInternal) }
        }
    }

    fn url(&self) -> String {
        let url = self.get_str(self.url_offset, || DEFAULT_URL.to_owned());

        if url == DEFAULT_URL {
            // the url option on the index could also be the string 'default', so
            // in either case above, lets use the setting from postgresql.conf
            if ZDB_DEFAULT_ELASTICSEARCH_URL.get().is_some() {
                ZDB_DEFAULT_ELASTICSEARCH_URL.get().unwrap()
            } else {
                // the user hasn't provided one
                panic!("Must set zdb.default_elasticsearch_url");
            }
        } else {
            // the index itself has a valid url
            url
        }
    }

    fn type_name(&self) -> String {
        self.get_str(self.type_name_offset, || DEFAULT_TYPE_NAME.to_owned())
    }

    fn refresh_interval(&self) -> RefreshInterval {
        match self
            .get_str(self.refresh_interval_offset, || {
                DEFAULT_REFRESH_INTERVAL.to_owned()
            })
            .as_str()
        {
            "-1" | "immediate" => RefreshInterval::Immediate,
            "async" => RefreshInterval::ImmediateAsync,
            other => RefreshInterval::Background(other.to_owned()),
        }
    }

    fn alias(&self, heaprel: &PgRelation, indexrel: &PgRelation) -> String {
        self.get_str(self.alias_offset, || {
            format!(
                "{}.{}.{}.{}-{}",
                unsafe {
                    std::ffi::CStr::from_ptr(pg_sys::get_database_name(pg_sys::MyDatabaseId))
                }
                .to_str()
                .unwrap(),
                unsafe {
                    std::ffi::CStr::from_ptr(pg_sys::get_namespace_name(indexrel.namespace_oid()))
                }
                .to_str()
                .unwrap(),
                heaprel.name(),
                indexrel.name(),
                indexrel.oid()
            )
        })
    }

    fn uuid(&self, heaprel: &PgRelation, indexrel: &PgRelation) -> String {
        self.get_str(self.uuid_offset, || {
            format!(
                "{}.{}.{}.{}",
                unsafe { pg_sys::MyDatabaseId },
                indexrel.namespace_oid(),
                heaprel.oid(),
                indexrel.oid(),
            )
        })
    }

    fn index_name(&self, heaprel: &PgRelation, indexrel: &PgRelation) -> String {
        self.uuid(heaprel, indexrel)
    }

    fn translog_durability(&self) -> String {
        self.get_str(self.translog_durability_offset, || {
            DEFAULT_TRANSLOG_DURABILITY.to_owned()
        })
    }

    fn links(&self) -> Option<Vec<String>> {
        let options = self.get_str(self.options_offset, || "".to_owned());
        if options.is_empty() {
            None
        } else {
            Some(options.split(',').map(|s| s.trim().to_owned()).collect())
        }
    }

    fn field_lists(&self) -> Option<HashMap<String, Vec<QualifiedField>>> {
        let value = self.get_str(self.field_lists_offset, || "".to_owned());
        if value.is_empty() {
            None
        } else {
            Some(parse_field_lists(&value))
        }
    }

    fn nested_object_text_mapping(&self) -> serde_json::Value {
        let value = self.get_str(self.nested_object_text_mapping_offset, || "".to_owned());
        if value.is_empty() {
            serde_json::json! {
                {
                   "type": "keyword",
                   "ignore_above": 10922,
                   "normalizer": "lowercase",
                   "copy_to": "zdb_all"
                 }
            }
        } else {
            serde_json::from_str(&value).expect("invalid 'nested_object_text_mapping' value")
        }
    }

    fn get_str<F: FnOnce() -> String>(&self, offset: i32, default: F) -> String {
        if offset == 0 {
            default()
        } else {
            let opts = self as *const _ as void_ptr as usize;
            let value =
                unsafe { CStr::from_ptr((opts + offset as usize) as *const std::os::raw::c_char) };

            value.to_str().unwrap().to_owned()
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ZDBIndexOptions {
    oid: pg_sys::Oid,
    url: String,
    type_name: String,
    refresh_interval: RefreshInterval,
    max_result_window: i32,
    nested_fields_limit: i32,
    nested_objects_limit: i32,
    total_field_limit: i32,
    max_terms_count: i32,
    max_analyze_token_count: i32,
    alias: String,
    uuid: String,
    translog_durability: String,
    links: Option<Vec<String>>,
    field_lists: Option<HashMap<String, Vec<QualifiedField>>>,
    shadow_index: bool,

    optimize_after: i32,
    compression_level: i32,
    shards: i32,
    replicas: i32,
    bulk_concurrency: i32,
    batch_size: i32,
    llapi: bool,

    nested_object_date_detection: bool,
    nested_object_numeric_detection: bool,
    nested_object_text_mapping: serde_json::Value,

    include_source: bool,
}

#[allow(dead_code)]
impl ZDBIndexOptions {
    pub fn from_relation(relation: &PgRelation) -> ZDBIndexOptions {
        let (relation, options) = find_zdb_index(relation).unwrap();
        ZDBIndexOptions::from_relation_no_lookup(&relation, options)
    }

    pub fn from_relation_no_lookup(
        relation: &PgRelation,
        options: Option<Vec<String>>,
    ) -> ZDBIndexOptions {
        let internal = ZDBIndexOptionsInternal::from_relation(&relation);
        let heap_relation = relation.heap_relation().expect("not an index");
        ZDBIndexOptions {
            oid: relation.oid(),
            url: internal.url(),
            type_name: internal.type_name(),
            refresh_interval: internal.refresh_interval(),
            max_result_window: internal.max_result_window,
            nested_fields_limit: internal.nested_fields_limit,
            nested_objects_limit: internal.nested_objects_limit,
            total_field_limit: internal.total_fields_limit,
            max_terms_count: internal.max_terms_count,
            max_analyze_token_count: internal.max_analyze_token_count,
            alias: internal.alias(&heap_relation, &relation),
            uuid: internal.uuid(&heap_relation, &relation),
            links: options.map_or_else(|| internal.links(), |v| Some(v)),
            field_lists: internal.field_lists(),
            shadow_index: internal.shadow_index,
            compression_level: internal.compression_level,
            shards: internal.shards,
            replicas: internal.replicas,
            bulk_concurrency: internal.bulk_concurrency,
            batch_size: internal.batch_size,
            optimize_after: internal.optimize_after,
            translog_durability: internal.translog_durability(),
            llapi: internal.llapi,
            nested_object_date_detection: internal.nested_object_date_detection,
            nested_object_numeric_detection: internal.nested_object_numeric_detection,
            nested_object_text_mapping: internal.nested_object_text_mapping(),
            include_source: internal.include_source,
        }
    }

    pub fn index_relation(&self) -> PgRelation {
        PgRelation::with_lock(self.oid(), pg_sys::AccessShareLock as pg_sys::LOCKMODE)
    }

    pub fn heap_relation(&self) -> PgRelation {
        self.index_relation().heap_relation().expect("not an index")
    }

    pub fn oid(&self) -> pg_sys::Oid {
        self.oid
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

    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn type_name(&self) -> &str {
        &self.type_name
    }

    pub fn refresh_interval(&self) -> RefreshInterval {
        self.refresh_interval.clone()
    }

    pub fn max_result_window(&self) -> i32 {
        self.max_result_window
    }

    pub fn nested_fields_limit(&self) -> i32 {
        self.nested_fields_limit
    }

    pub fn nested_objects_limit(&self) -> i32 {
        self.nested_objects_limit
    }

    pub fn total_fields_limit(&self) -> i32 {
        self.total_field_limit
    }

    pub fn max_terms_count(&self) -> i32 {
        self.max_terms_count
    }

    pub fn max_analyze_token_count(&self) -> i32 {
        self.max_analyze_token_count
    }

    pub fn alias(&self) -> &str {
        &self.alias
    }

    pub fn uuid(&self) -> &str {
        &self.uuid
    }

    pub fn index_name(&self) -> &str {
        &self.uuid
    }

    pub fn translog_durability(&self) -> &str {
        &self.translog_durability
    }

    pub fn links(&self) -> &Option<Vec<String>> {
        &self.links
    }

    pub fn field_lists(&self) -> HashMap<String, Vec<QualifiedField>> {
        match &self.field_lists {
            Some(field_lists) => field_lists.clone(),
            None => HashMap::new(),
        }
    }

    pub fn is_shadow_index(&self) -> bool {
        self.shadow_index
    }

    pub fn nested_object_date_detection(&self) -> bool {
        self.nested_object_date_detection
    }

    pub fn nested_object_numeric_detection(&self) -> bool {
        self.nested_object_numeric_detection
    }

    pub fn nested_object_text_mapping(&self) -> &serde_json::Value {
        &self.nested_object_text_mapping
    }

    pub fn include_source(&self) -> bool {
        self.include_source
    }
}

#[pg_extern(
    immutable,
    parallel_safe,
    raw,
    no_guard,
    sql = r#"
        -- we don't want any SQL generated for the "shadow" function, but we do want its '_wrapper' symbol
        -- exported so that shadow indexes can reference it using whatever argument type they want    
    "#
)]
fn shadow(fcinfo: pg_sys::FunctionCallInfo) -> pg_sys::Datum {
    pg_getarg_datum_raw(fcinfo, 0)
}

#[pg_extern(volatile, parallel_safe)]
fn determine_index(relation: PgRelation) -> Option<PgRelation> {
    match find_zdb_index(&relation) {
        Ok((relation, _)) => Some(relation),

        // we don't want to raise an error if we couldn't find the index for the relation
        Err(_) => None,
    }
}

#[pg_extern(volatile, parallel_safe)]
fn index_links(relation: PgRelation) -> Option<Vec<String>> {
    let (_, options) = find_zdb_index(&relation).expect("failed to lookup index link options");
    options
}

#[pg_extern(volatile, parallel_safe)]
fn index_name(index_relation: PgRelation) -> String {
    ZDBIndexOptions::from_relation(&index_relation)
        .index_name()
        .to_owned()
}

#[pg_extern(volatile, parallel_safe)]
fn index_alias(index_relation: PgRelation) -> String {
    ZDBIndexOptions::from_relation(&index_relation)
        .alias()
        .to_owned()
}

#[pg_extern(volatile, parallel_safe)]
fn index_url(index_relation: PgRelation) -> String {
    ZDBIndexOptions::from_relation(&index_relation)
        .url()
        .to_owned()
}

#[pg_extern(volatile, parallel_safe)]
fn index_type_name(index_relation: PgRelation) -> String {
    ZDBIndexOptions::from_relation(&index_relation)
        .type_name()
        .to_owned()
}

#[pg_extern(volatile, parallel_safe)]
fn index_mapping(index_relation: PgRelation) -> JsonB {
    JsonB(
        Elasticsearch::new(&index_relation)
            .get_mapping()
            .execute()
            .expect("failed to get index mapping"),
    )
}

#[pg_extern(volatile, parallel_safe)]
fn index_settings(index_relation: PgRelation) -> JsonB {
    JsonB(
        Elasticsearch::new(&index_relation)
            .get_settings()
            .execute()
            .expect("failed to get index settings"),
    )
}

#[pg_extern(volatile, parallel_safe)]
pub(crate) fn index_options(index_relation: PgRelation) -> Option<Vec<String>> {
    ZDBIndexOptions::from_relation(&index_relation)
        .links()
        .clone()
}

#[pg_extern(volatile, parallel_safe)]
fn index_field_lists(
    index_relation: PgRelation,
) -> impl std::iter::Iterator<Item = (name!(fieldname, String), name!(fields, Vec<String>))> {
    ZDBIndexOptions::from_relation(&index_relation)
        .field_lists()
        .into_iter()
        .map(|(k, v)| (k, v.into_iter().map(|f| f.field_name()).collect()))
}

#[pg_extern(volatile, parallel_safe)]
fn field_mapping(index_relation: PgRelation, field: &str) -> Option<JsonB> {
    let root_index = IndexLink::from_relation(&index_relation);
    let index_links = IndexLink::from_zdb(&index_relation);
    let link = find_link_for_field(
        &QualifiedField {
            index: None,
            field: field.into(),
        },
        &root_index,
        &index_links,
    );

    link.map_or(None, |link| {
        let index = link.open_index().expect("failed to open index");
        let options = ZDBIndexOptions::from_relation(&index);
        let mapping = index_mapping(index);

        let mut as_map: HashMap<String, serde_json::Value> =
            serde_json::from_value(mapping.0).unwrap();

        as_map = serde_json::from_value(
            as_map
                .remove(options.index_name())
                .expect("no index object in mapping"),
        )
        .unwrap();
        as_map = serde_json::from_value(
            as_map
                .remove("mappings")
                .expect("no mappings object in mapping"),
        )
        .unwrap();
        as_map = serde_json::from_value(
            as_map
                .remove("properties")
                .expect("no properties object in mapping"),
        )
        .unwrap();

        match as_map.remove(field) {
            Some(field_mapping) => Some(JsonB(field_mapping)),
            None => None,
        }
    })
}

static mut RELOPT_KIND_ZDB: pg_sys::relopt_kind = 0;

#[pg_guard]
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

    if let Err(e) = url::Url::parse(url) {
        panic!("{}", e.to_string())
    }
}

#[pg_guard]
extern "C" fn validate_translog_durability(value: *const std::os::raw::c_char) {
    if value.is_null() {
        // null is fine -- we'll just use our default
        return;
    }

    let value = unsafe { CStr::from_ptr(value) }
        .to_str()
        .expect("failed to convert translog_durability to utf8");
    if value != "request" && value != "async" {
        panic!(
            "invalid translog_durability setting.  Must be one of 'request' or 'async': {}",
            value
        )
    }
}

#[pg_guard]
extern "C" fn validate_options(value: *const std::os::raw::c_char) {
    if value.is_null() {
        // null is fine
        return;
    }

    let mut used_fields = HashSet::new();
    let mut fieldname_stack = Vec::new();
    let mut operator_stack = Vec::new();
    let input = unsafe { CStr::from_ptr(value) };
    let input = input.to_str().expect("options is not valid UTF8");

    for option in input.split(',') {
        INDEX_LINK_PARSER.with(|parser| {
            parser
                .parse(
                    None,
                    &mut used_fields,
                    &mut fieldname_stack,
                    &mut operator_stack,
                    option,
                )
                .expect(&format!("failed to parse index option: /{}/", option))
        });
    }

    return;
}

#[pg_guard]
extern "C" fn validate_field_lists(value: *const std::os::raw::c_char) {
    if value.is_null() {
        // null is fine
        return;
    }

    let input = unsafe { CStr::from_ptr(value) };
    let input = input.to_str().expect("field_lists is not valid UTF8");
    parse_field_lists(input);
}

#[pg_guard]
extern "C" fn validate_text_mapping(value: *const std::os::raw::c_char) {
    if value.is_null() {
        // null is fine
        return;
    }

    let input = unsafe { CStr::from_ptr(value) };
    serde_json::from_str::<serde_json::Value>(
        input
            .to_str()
            .expect("nested_object_text_mapping value is not valid UTF8"),
    )
    .expect("invalid nested_object_text_mapping");
}

const NUM_REL_OPTS: usize = 26;
#[allow(clippy::unneeded_field_pattern)] // b/c of offset_of!()
#[pg_guard]
pub unsafe extern "C" fn amoptions(
    reloptions: pg_sys::Datum,
    validate: bool,
) -> *mut pg_sys::bytea {
    // TODO:  how to make this const?  we can't use offset_of!() macro in const definitions, apparently
    let tab: [pg_sys::relopt_parse_elt; NUM_REL_OPTS] = [
        pg_sys::relopt_parse_elt {
            optname: "url".as_pg_cstr(),
            opttype: pg_sys::relopt_type_RELOPT_TYPE_STRING,
            offset: offset_of!(ZDBIndexOptionsInternal, url_offset) as i32,
        },
        pg_sys::relopt_parse_elt {
            optname: "type_name".as_pg_cstr(),
            opttype: pg_sys::relopt_type_RELOPT_TYPE_STRING,
            offset: offset_of!(ZDBIndexOptionsInternal, type_name_offset) as i32,
        },
        pg_sys::relopt_parse_elt {
            optname: "refresh_interval".as_pg_cstr(),
            opttype: pg_sys::relopt_type_RELOPT_TYPE_STRING,
            offset: offset_of!(ZDBIndexOptionsInternal, refresh_interval_offset) as i32,
        },
        pg_sys::relopt_parse_elt {
            optname: "shards".as_pg_cstr(),
            opttype: pg_sys::relopt_type_RELOPT_TYPE_INT,
            offset: offset_of!(ZDBIndexOptionsInternal, shards) as i32,
        },
        pg_sys::relopt_parse_elt {
            optname: "replicas".as_pg_cstr(),
            opttype: pg_sys::relopt_type_RELOPT_TYPE_INT,
            offset: offset_of!(ZDBIndexOptionsInternal, replicas) as i32,
        },
        pg_sys::relopt_parse_elt {
            optname: "bulk_concurrency".as_pg_cstr(),
            opttype: pg_sys::relopt_type_RELOPT_TYPE_INT,
            offset: offset_of!(ZDBIndexOptionsInternal, bulk_concurrency) as i32,
        },
        pg_sys::relopt_parse_elt {
            optname: "batch_size".as_pg_cstr(),
            opttype: pg_sys::relopt_type_RELOPT_TYPE_INT,
            offset: offset_of!(ZDBIndexOptionsInternal, batch_size) as i32,
        },
        pg_sys::relopt_parse_elt {
            optname: "compression_level".as_pg_cstr(),
            opttype: pg_sys::relopt_type_RELOPT_TYPE_INT,
            offset: offset_of!(ZDBIndexOptionsInternal, compression_level) as i32,
        },
        pg_sys::relopt_parse_elt {
            optname: "max_result_window".as_pg_cstr(),
            opttype: pg_sys::relopt_type_RELOPT_TYPE_INT,
            offset: offset_of!(ZDBIndexOptionsInternal, max_result_window) as i32,
        },
        pg_sys::relopt_parse_elt {
            optname: "nested_fields_limit".as_pg_cstr(),
            opttype: pg_sys::relopt_type_RELOPT_TYPE_INT,
            offset: offset_of!(ZDBIndexOptionsInternal, nested_fields_limit) as i32,
        },
        pg_sys::relopt_parse_elt {
            optname: "nested_objects_limit".as_pg_cstr(),
            opttype: pg_sys::relopt_type_RELOPT_TYPE_INT,
            offset: offset_of!(ZDBIndexOptionsInternal, nested_objects_limit) as i32,
        },
        pg_sys::relopt_parse_elt {
            optname: "total_fields_limit".as_pg_cstr(),
            opttype: pg_sys::relopt_type_RELOPT_TYPE_INT,
            offset: offset_of!(ZDBIndexOptionsInternal, total_fields_limit) as i32,
        },
        pg_sys::relopt_parse_elt {
            optname: "max_terms_count".as_pg_cstr(),
            opttype: pg_sys::relopt_type_RELOPT_TYPE_INT,
            offset: offset_of!(ZDBIndexOptionsInternal, max_terms_count) as i32,
        },
        pg_sys::relopt_parse_elt {
            optname: "max_analyze_token_count".as_pg_cstr(),
            opttype: pg_sys::relopt_type_RELOPT_TYPE_INT,
            offset: offset_of!(ZDBIndexOptionsInternal, max_analyze_token_count) as i32,
        },
        pg_sys::relopt_parse_elt {
            optname: "alias".as_pg_cstr(),
            opttype: pg_sys::relopt_type_RELOPT_TYPE_STRING,
            offset: offset_of!(ZDBIndexOptionsInternal, alias_offset) as i32,
        },
        pg_sys::relopt_parse_elt {
            optname: "optimize_after".as_pg_cstr(),
            opttype: pg_sys::relopt_type_RELOPT_TYPE_INT,
            offset: offset_of!(ZDBIndexOptionsInternal, optimize_after) as i32,
        },
        pg_sys::relopt_parse_elt {
            optname: "llapi".as_pg_cstr(),
            opttype: pg_sys::relopt_type_RELOPT_TYPE_BOOL,
            offset: offset_of!(ZDBIndexOptionsInternal, llapi) as i32,
        },
        pg_sys::relopt_parse_elt {
            optname: "uuid".as_pg_cstr(),
            opttype: pg_sys::relopt_type_RELOPT_TYPE_STRING,
            offset: offset_of!(ZDBIndexOptionsInternal, uuid_offset) as i32,
        },
        pg_sys::relopt_parse_elt {
            optname: "translog_durability".as_pg_cstr(),
            opttype: pg_sys::relopt_type_RELOPT_TYPE_STRING,
            offset: offset_of!(ZDBIndexOptionsInternal, translog_durability_offset) as i32,
        },
        pg_sys::relopt_parse_elt {
            optname: "options".as_pg_cstr(),
            opttype: pg_sys::relopt_type_RELOPT_TYPE_STRING,
            offset: offset_of!(ZDBIndexOptionsInternal, options_offset) as i32,
        },
        pg_sys::relopt_parse_elt {
            optname: "field_lists".as_pg_cstr(),
            opttype: pg_sys::relopt_type_RELOPT_TYPE_STRING,
            offset: offset_of!(ZDBIndexOptionsInternal, field_lists_offset) as i32,
        },
        pg_sys::relopt_parse_elt {
            optname: "shadow".as_pg_cstr(),
            opttype: pg_sys::relopt_type_RELOPT_TYPE_STRING,
            offset: offset_of!(ZDBIndexOptionsInternal, shadow_index) as i32,
        },
        pg_sys::relopt_parse_elt {
            optname: "nested_object_date_detection".as_pg_cstr(),
            opttype: pg_sys::relopt_type_RELOPT_TYPE_BOOL,
            offset: offset_of!(ZDBIndexOptionsInternal, nested_object_date_detection) as i32,
        },
        pg_sys::relopt_parse_elt {
            optname: "nested_object_numeric_detection".as_pg_cstr(),
            opttype: pg_sys::relopt_type_RELOPT_TYPE_BOOL,
            offset: offset_of!(ZDBIndexOptionsInternal, nested_object_numeric_detection) as i32,
        },
        pg_sys::relopt_parse_elt {
            optname: "nested_object_text_mapping".as_pg_cstr(),
            opttype: pg_sys::relopt_type_RELOPT_TYPE_STRING,
            offset: offset_of!(ZDBIndexOptionsInternal, nested_object_text_mapping_offset) as i32,
        },
        pg_sys::relopt_parse_elt {
            optname: "include_source".as_pg_cstr(),
            opttype: pg_sys::relopt_type_RELOPT_TYPE_BOOL,
            offset: offset_of!(ZDBIndexOptionsInternal, include_source) as i32,
        },
    ];

    build_relopts(reloptions, validate, tab)
}

#[cfg(any(feature = "pg13", feature = "pg14"))]
unsafe fn build_relopts(
    reloptions: pg_sys::Datum,
    validate: bool,
    tab: [pg_sys::relopt_parse_elt; NUM_REL_OPTS],
) -> *mut pg_sys::bytea {
    let rdopts;

    /* Parse the user-given reloptions */
    rdopts = pg_sys::build_reloptions(
        reloptions,
        validate,
        RELOPT_KIND_ZDB,
        std::mem::size_of::<ZDBIndexOptionsInternal>(),
        tab.as_ptr(),
        NUM_REL_OPTS as i32,
    );

    rdopts as *mut pg_sys::bytea
}

#[cfg(any(feature = "pg10", feature = "pg11", feature = "pg12"))]
unsafe fn build_relopts(
    reloptions: pg_sys::Datum,
    validate: bool,
    tab: [pg_sys::relopt_parse_elt; NUM_REL_OPTS],
) -> *mut pg_sys::bytea {
    let mut noptions = 0;
    let options = pg_sys::parseRelOptions(reloptions, validate, RELOPT_KIND_ZDB, &mut noptions);
    if noptions == 0 {
        return std::ptr::null_mut();
    }

    for relopt in std::slice::from_raw_parts_mut(options, noptions as usize) {
        relopt.gen.as_mut().unwrap().lockmode = pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE;
    }

    let rdopts = pg_sys::allocateReloptStruct(
        std::mem::size_of::<ZDBIndexOptionsInternal>(),
        options,
        noptions,
    );
    pg_sys::fillRelOptions(
        rdopts,
        std::mem::size_of::<ZDBIndexOptionsInternal>(),
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
        "url".as_pg_cstr(),
        "Server URL and port".as_pg_cstr(),
        "default".as_pg_cstr(),
        Some(validate_url),
        #[cfg(any(feature = "pg13", feature = "pg14"))]
        {
            pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE
        },
    );
    pg_sys::add_string_reloption(
        RELOPT_KIND_ZDB,
        "type_name".as_pg_cstr(),
        "What Elasticsearch index type name should ZDB use?  Default is 'doc'".as_pg_cstr(),
        "doc".as_pg_cstr(),
        None,
        #[cfg(any(feature = "pg13", feature = "pg14"))]
        {
            pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE
        },
    );
    pg_sys::add_string_reloption(
        RELOPT_KIND_ZDB,
        "refresh_interval".as_pg_cstr(),
        "Frequency in which Elasticsearch indexes are refreshed.  Related to ES' index.refresh_interval setting".as_pg_cstr(),
        DEFAULT_REFRESH_INTERVAL.as_pg_cstr(),
        None,
        #[cfg(any(feature = "pg13", feature = "pg14"))]
            { pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE },
    );
    pg_sys::add_int_reloption(
        RELOPT_KIND_ZDB,
        "shards".as_pg_cstr(),
        "The number of shards for the index".as_pg_cstr(),
        DEFAULT_SHARDS,
        1,
        32768,
        #[cfg(any(feature = "pg13", feature = "pg14"))]
        {
            pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE
        },
    );
    pg_sys::add_int_reloption(
        RELOPT_KIND_ZDB,
        "replicas".as_pg_cstr(),
        "The number of replicas for the index".as_pg_cstr(),
        ZDB_DEFAULT_REPLICAS.get(),
        0,
        32768,
        #[cfg(any(feature = "pg13", feature = "pg14"))]
        {
            pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE
        },
    );
    pg_sys::add_int_reloption(
        RELOPT_KIND_ZDB,
        "bulk_concurrency".as_pg_cstr(),
        "The maximum number of concurrent _bulk API requests".as_pg_cstr(),
        *DEFAULT_BULK_CONCURRENCY,
        1,
        num_cpus::get() as i32,
        #[cfg(any(feature = "pg13", feature = "pg14"))]
        {
            pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE
        },
    );
    pg_sys::add_int_reloption(
        RELOPT_KIND_ZDB,
        "batch_size".as_pg_cstr(),
        "The size in bytes of batch calls to the _bulk API".as_pg_cstr(),
        DEFAULT_BATCH_SIZE,
        1,
        (std::i32::MAX / 2) - 1,
        #[cfg(any(feature = "pg13", feature = "pg14"))]
        {
            pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE
        },
    );
    pg_sys::add_int_reloption(
        RELOPT_KIND_ZDB,
        "compression_level".as_pg_cstr(),
        "0-9 value to indicate the level of HTTP compression".as_pg_cstr(),
        DEFAULT_COMPRESSION_LEVEL,
        0,
        9,
        #[cfg(any(feature = "pg13", feature = "pg14"))]
        {
            pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE
        },
    );
    pg_sys::add_int_reloption(
        RELOPT_KIND_ZDB,
        "max_result_window".as_pg_cstr(),
        "The number of docs to page in from Elasticsearch at one time.  Default is 10,000"
            .as_pg_cstr(),
        DEFAULT_MAX_RESULT_WINDOW,
        1,
        std::i32::MAX,
        #[cfg(any(feature = "pg13", feature = "pg14"))]
        {
            pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE
        },
    );
    pg_sys::add_int_reloption(
        RELOPT_KIND_ZDB,
        "nested_fields_limit".as_pg_cstr(),
        "The maximum number of distinct nested mappings in an index.  Default is 1000".as_pg_cstr(),
        DEFAULT_NESTED_FIELDS_LIMIT,
        1,
        std::i32::MAX,
        #[cfg(any(feature = "pg13", feature = "pg14"))]
        {
            pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE
        },
    );
    pg_sys::add_int_reloption(
        RELOPT_KIND_ZDB,
        "nested_objects_limit".as_pg_cstr(),
        "The maximum number of nested JSON objects that a single document can contain across all nested types.  Default is 1000".as_pg_cstr(),
        DEFAULT_NESTED_OBJECTS_LIMIT,
        1,
        std::i32::MAX,
        #[cfg(any(feature = "pg13", feature = "pg14"))]
        {
            pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE
        },
    );
    pg_sys::add_int_reloption(
        RELOPT_KIND_ZDB,
        "total_fields_limit".as_pg_cstr(),
        "The maximum number of fields in an index. Field and object mappings, as well as field aliases count towards this limit. The default value is 1000.".as_pg_cstr(),
        DEFAULT_TOTAL_FIELDS_LIMIT,
        1,
        std::i32::MAX,
        #[cfg(any(feature = "pg13", feature = "pg14"))]
        {
            pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE
        },
    );
    pg_sys::add_int_reloption(
        RELOPT_KIND_ZDB,
        "max_terms_count".as_pg_cstr(),
        "The maximum number of terms that can be used in Terms Query.  The default value is 65535."
            .as_pg_cstr(),
        DEFAULT_MAX_TERMS_COUNT,
        1,
        std::i32::MAX,
        #[cfg(any(feature = "pg13", feature = "pg14"))]
        {
            pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE
        },
    );
    pg_sys::add_int_reloption(
        RELOPT_KIND_ZDB,
        "max_analyze_token_count".as_pg_cstr(),
        "The maximum number of tokens to be generated during text analysis.  Corresponds to the Elasticsearch 'index.analyze.max_token_count' setting.  The default value is 10000."
            .as_pg_cstr(),
        DEFAULT_MAX_ANALYZE_TOKEN_COUNT,
        1,
        std::i32::MAX,
        #[cfg(any(feature = "pg13", feature = "pg14"))]
        {
            pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE
        },
    );
    pg_sys::add_string_reloption(
        RELOPT_KIND_ZDB,
        "alias".as_pg_cstr(),
        "The Elasticsearch Alias to which this index should belong".as_pg_cstr(),
        std::ptr::null(),
        None,
        #[cfg(any(feature = "pg13", feature = "pg14"))]
        {
            pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE
        },
    );
    pg_sys::add_string_reloption(
        RELOPT_KIND_ZDB,
        "uuid".as_pg_cstr(),
        "The Elasticsearch index name, as a UUID".as_pg_cstr(),
        std::ptr::null(),
        None,
        #[cfg(any(feature = "pg13", feature = "pg14"))]
        {
            pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE
        },
    );
    pg_sys::add_string_reloption(
        RELOPT_KIND_ZDB,
        "translog_durability".as_pg_cstr(),
        "Elasticsearch index.translog.durability setting.  Defaults to 'request'".as_pg_cstr(),
        "request".as_pg_cstr(),
        Some(validate_translog_durability),
        #[cfg(any(feature = "pg13", feature = "pg14"))]
        {
            pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE
        },
    );
    pg_sys::add_int_reloption(
        RELOPT_KIND_ZDB,
        "optimize_after".as_pg_cstr(),
        "After how many deleted docs should ZDB _optimize the ES index during VACUUM?".as_pg_cstr(),
        DEFAULT_OPTIMIZE_AFTER,
        0,
        std::i32::MAX,
        #[cfg(any(feature = "pg13", feature = "pg14"))]
        {
            pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE
        },
    );
    pg_sys::add_bool_reloption(
        RELOPT_KIND_ZDB,
        "llapi".as_pg_cstr(),
        "Will this index be used by ZomboDB's low-level API?".as_pg_cstr(),
        false,
        #[cfg(any(feature = "pg13", feature = "pg14"))]
        {
            pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE
        },
    );
    pg_sys::add_string_reloption(
        RELOPT_KIND_ZDB,
        "options".as_pg_cstr(),
        "ZomboDB Index Linking options".as_pg_cstr(),
        std::ptr::null(),
        Some(validate_options),
        #[cfg(any(feature = "pg13", feature = "pg14"))]
        {
            pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE
        },
    );
    pg_sys::add_string_reloption(
        RELOPT_KIND_ZDB,
        "field_lists".as_pg_cstr(),
        "Combine fields into named lists during search".as_pg_cstr(),
        std::ptr::null(),
        Some(validate_field_lists),
        #[cfg(any(feature = "pg13", feature = "pg14"))]
        {
            pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE
        },
    );
    pg_sys::add_bool_reloption(
        RELOPT_KIND_ZDB,
        "shadow".as_pg_cstr(),
        "Is this index a shadow index, and if so, to which one".as_pg_cstr(),
        false,
        #[cfg(any(feature = "pg13", feature = "pg14"))]
        {
            pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE
        },
    );
    pg_sys::add_bool_reloption(
        RELOPT_KIND_ZDB,
        "nested_object_date_detection".as_pg_cstr(),
        "Should ES try to automatically detect dates in nested objects".as_pg_cstr(),
        false,
        #[cfg(any(feature = "pg13", feature = "pg14"))]
        {
            pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE
        },
    );
    pg_sys::add_bool_reloption(
        RELOPT_KIND_ZDB,
        "nested_object_numeric_detection".as_pg_cstr(),
        "Should ES try to automatically detect numbers in nested objects".as_pg_cstr(),
        false,
        #[cfg(any(feature = "pg13", feature = "pg14"))]
        {
            pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE
        },
    );
    pg_sys::add_string_reloption(
        RELOPT_KIND_ZDB,
        "nested_object_text_mapping".as_pg_cstr(),
        "As a JSON mapping definition, how should dynamic text values in JSON be mapped?".as_pg_cstr(),
        r#"{ "type": "keyword", "ignore_above": 10922, "normalizer": "lowercase", "copy_to": "zdb_all" }"#.as_pg_cstr(),
        Some(validate_text_mapping),
        #[cfg(any(feature = "pg13", feature = "pg14"))]
        {
            pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE
        },
    );
    pg_sys::add_bool_reloption(
        RELOPT_KIND_ZDB,
        "include_source".as_pg_cstr(),
        "Should the source of the document be included in the _source field?".as_pg_cstr(),
        true,
        #[cfg(any(feature = "pg13", feature = "pg14"))]
        {
            pg_sys::AccessExclusiveLock as pg_sys::LOCKMODE
        },
    );
}

#[cfg(any(test, feature = "pg_test"))]
#[pgx_macros::pg_schema]
mod tests {
    use crate::access_method::options::{
        validate_translog_durability, validate_url, RefreshInterval, ZDBIndexOptions,
        DEFAULT_BATCH_SIZE, DEFAULT_BULK_CONCURRENCY, DEFAULT_COMPRESSION_LEVEL,
        DEFAULT_OPTIMIZE_AFTER, DEFAULT_SHARDS, DEFAULT_TYPE_NAME,
    };
    use crate::gucs::ZDB_DEFAULT_REPLICAS;
    use crate::zql::ast::IndexLink;
    use pgx::pg_sys::AsPgCStr;
    use pgx::*;

    #[pg_test]
    fn test_validate_url() {
        validate_url("http://localhost:9200/".as_pg_cstr());
    }

    #[pg_test]
    fn test_validate_default_url() {
        validate_url("default".as_pg_cstr());
    }

    #[pg_test(error = "url must end with a forward slash")]
    fn test_validate_invalid_url() {
        validate_url("http://localhost:9200".as_pg_cstr());
    }

    #[pg_test(
        error = "invalid translog_durability setting.  Must be one of 'request' or 'async': foo"
    )]
    fn test_validate_invalid_translog_durability() {
        validate_translog_durability("foo".as_pg_cstr());
    }

    #[pg_test]
    fn test_valid_translog_durability_request() {
        validate_translog_durability("request".as_pg_cstr());
    }

    #[pg_test]
    fn test_valid_translog_durability_async() {
        validate_translog_durability("async".as_pg_cstr());
    }

    #[pg_test]
    #[initialize(es = true)]
    unsafe fn test_index_options() {
        let uuid = 42424242;
        Spi::run(&format!(
            "CREATE TABLE test();  
        CREATE INDEX idxtest 
                  ON test 
               USING zombodb ((test.*)) 
                WITH (url='http://localhost:19200/', 
                      type_name='test_type_name', 
                      alias='test_alias', 
                      uuid='{}', 
                      refresh_interval='5s',
                      translog_durability='async');",
            uuid
        ));

        let index_oid = Spi::get_one::<pg_sys::Oid>("SELECT 'idxtest'::regclass::oid")
            .expect("failed to get SPI result");
        let indexrel = PgRelation::from_pg(pg_sys::RelationIdGetRelation(index_oid));
        let options = ZDBIndexOptions::from_relation(&indexrel);
        assert_eq!(options.url(), "http://localhost:19200/");
        assert_eq!(options.type_name(), "test_type_name");
        assert_eq!(options.alias(), "test_alias");
        assert_eq!(options.uuid(), &uuid.to_string());
        assert_eq!(
            options.refresh_interval(),
            RefreshInterval::Background("5s".to_owned())
        );
        assert_eq!(options.compression_level(), 1);
        assert_eq!(options.shards(), 5);
        assert_eq!(options.replicas(), 0);
        assert_eq!(options.bulk_concurrency(), num_cpus::get() as i32);
        assert_eq!(options.batch_size(), 8 * 1024 * 1024);
        assert_eq!(options.optimize_after(), DEFAULT_OPTIMIZE_AFTER);
        assert_eq!(options.llapi(), false);
        assert_eq!(options.translog_durability(), "async");
        assert_eq!(options.links, None);
    }

    #[pg_test]
    #[initialize(es = true)]
    unsafe fn test_index_options_defaults() {
        Spi::run(
            "CREATE TABLE test();  
        CREATE INDEX idxtest 
                  ON test 
               USING zombodb ((test.*)) WITH (url='http://localhost:19200/');",
        );

        let heap_oid = Spi::get_one::<pg_sys::Oid>("SELECT 'test'::regclass::oid")
            .expect("failed to get SPI result");
        let index_oid = Spi::get_one::<pg_sys::Oid>("SELECT 'idxtest'::regclass::oid")
            .expect("failed to get SPI result");
        let heaprel = PgRelation::from_pg(pg_sys::RelationIdGetRelation(heap_oid));
        let indexrel = PgRelation::from_pg(pg_sys::RelationIdGetRelation(index_oid));
        let options = ZDBIndexOptions::from_relation(&indexrel);
        assert_eq!(options.type_name(), DEFAULT_TYPE_NAME);
        assert_eq!(
            &options.alias(),
            &format!("pgx_tests.public.test.idxtest-{}", indexrel.oid())
        );
        assert_eq!(
            &options.uuid(),
            &format!(
                "{}.{}.{}.{}",
                pg_sys::MyDatabaseId,
                indexrel.namespace_oid(),
                heaprel.oid(),
                indexrel.oid()
            )
        );
        assert_eq!(options.refresh_interval(), RefreshInterval::Immediate);
        assert_eq!(options.compression_level(), DEFAULT_COMPRESSION_LEVEL);
        assert_eq!(options.shards(), DEFAULT_SHARDS);
        assert_eq!(options.replicas(), ZDB_DEFAULT_REPLICAS.get());
        assert_eq!(options.bulk_concurrency(), *DEFAULT_BULK_CONCURRENCY);
        assert_eq!(options.batch_size(), DEFAULT_BATCH_SIZE);
        assert_eq!(options.optimize_after(), DEFAULT_OPTIMIZE_AFTER);
        assert_eq!(options.llapi(), false);
        assert_eq!(options.translog_durability(), "request")
    }

    #[pg_test]
    #[initialize(es = true)]
    unsafe fn test_index_name() {
        Spi::run(
            "CREATE TABLE test();  
        CREATE INDEX idxtest 
                  ON test 
               USING zombodb ((test.*)) WITH (url='http://localhost:19200/');",
        );

        let index_relation = PgRelation::open_with_name("idxtest").expect("no such relation");
        let options = ZDBIndexOptions::from_relation(&index_relation);

        assert_eq!(options.index_name(), options.uuid());
    }

    #[pg_test]
    #[initialize(es = true)]
    unsafe fn test_index_url() {
        Spi::run(
            "CREATE TABLE test();  
        CREATE INDEX idxtest 
                  ON test 
               USING zombodb ((test.*)) WITH (url='http://localhost:19200/');",
        );

        let index_relation = PgRelation::open_with_name("idxtest").expect("no such relation");
        let options = ZDBIndexOptions::from_relation(&index_relation);

        assert_eq!(options.url(), "http://localhost:19200/");
    }

    #[pg_test]
    #[initialize(es = true)]
    unsafe fn test_index_type_name() {
        Spi::run(
            "CREATE TABLE test();  
        CREATE INDEX idxtest 
                  ON test 
               USING zombodb ((test.*)) WITH (url='http://localhost:19200/');",
        );

        let index_relation = PgRelation::open_with_name("idxtest").expect("no such relation");
        let options = ZDBIndexOptions::from_relation(&index_relation);

        assert_eq!(options.type_name(), "doc");
    }

    #[pg_test]
    #[initialize(es = true)]
    unsafe fn test_index_link_options() {
        Spi::run(
            "CREATE TABLE test_link_options();  
        CREATE INDEX idxtest_link_options
                  ON test_link_options
               USING zombodb ((test_link_options.*)) WITH (options='id=<schema.table.index>other_id');",
        );

        let index_relation =
            PgRelation::open_with_name("idxtest_link_options").expect("no such relation");
        let options = ZDBIndexOptions::from_relation(&index_relation);

        assert_eq!(
            options.links(),
            &Some(vec!["id=<schema.table.index>other_id".to_string()])
        );
    }

    #[pg_test]
    #[initialize(es = true)]
    unsafe fn test_quoted_index_link_options_issue688() {
        Spi::run(
            "CREATE TABLE test_link_options();  
        CREATE INDEX idxtest_link_options
                  ON test_link_options
               USING zombodb ((test_link_options.*)) WITH (options='id=<`schema.table.index`>other_id');",
        );

        let index_relation =
            PgRelation::open_with_name("idxtest_link_options").expect("no such relation");
        let options = ZDBIndexOptions::from_relation(&index_relation);
        let links = options.links().as_ref().unwrap();

        assert_eq!(1, links.len());
        let link_definition = links.first().unwrap();
        let link = IndexLink::parse(&link_definition);
        assert_eq!(link, IndexLink::parse("id=<schema.table.index>other_id"));
        assert_eq!(link.qualified_index.schema, Some("schema".into()))
    }
}
