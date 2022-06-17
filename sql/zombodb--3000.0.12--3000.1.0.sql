DROP FUNCTION IF EXISTS zdb.schema_version() CASCADE;
DROP FUNCTION IF EXISTS zdb.highlight_document(index regclass, document jsonb, query_string text) CASCADE;
-- it's imperative for `update-versions.sh` that this function be formatted exactly this way
CREATE FUNCTION zdb.schema_version() RETURNS text LANGUAGE sql AS $$
SELECT '3000.1.0 (5cf2f0622cb6dae2c61531794aaa9a856c478ca8)'
$$;
-- src/highlighting/query_highlighter.rs:370
-- zombodb::highlighting::query_highlighter::highlight_document
CREATE FUNCTION zdb."highlight_document"(
	"index" regclass, /* pgx::rel::PgRelation */
	"document" jsonb, /* pgx::datum::json::JsonB */
	"query_string" text /* &str */
) RETURNS TABLE (
	"field_name" text,  /* alloc::string::String */
	"array_index" integer,  /* i32 */
	"term" text,  /* alloc::string::String */
	"type" text,  /* alloc::string::String */
	"position" integer,  /* i32 */
	"start_offset" bigint,  /* i64 */
	"end_offset" bigint,  /* i64 */
	"query_clause" text  /* alloc::string::String */
)
IMMUTABLE PARALLEL SAFE  STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'highlight_document_jsonb_wrapper';
-- src/highlighting/query_highlighter.rs:390
-- zombodb::highlighting::query_highlighter::highlight_document
CREATE FUNCTION zdb."highlight_document"(
	"index" regclass, /* pgx::rel::PgRelation */
	"document" json, /* pgx::datum::json::Json */
	"query_string" text /* &str */
) RETURNS TABLE (
	"field_name" text,  /* alloc::string::String */
	"array_index" integer,  /* i32 */
	"term" text,  /* alloc::string::String */
	"type" text,  /* alloc::string::String */
	"position" integer,  /* i32 */
	"start_offset" bigint,  /* i64 */
	"end_offset" bigint,  /* i64 */
	"query_clause" text  /* alloc::string::String */
)
IMMUTABLE PARALLEL SAFE  STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'highlight_document_json_wrapper';

