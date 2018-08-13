--
-- highlighting composition functions
--
CREATE TYPE esqdsl_highlight_type AS ENUM ('unified', 'plain', 'fvh');
CREATE TYPE esqdsl_fragmenter_type AS ENUM ('simple', 'span');
CREATE TYPE esqdsl_encoder_type AS ENUM ('default', 'html');
CREATE TYPE esqdsl_boundary_scanner_type AS ENUM ('chars', 'sentence', 'word');

CREATE TYPE esqdsl_highlight AS (
    type zdb.esqdsl_highlight_type,
    number_of_fragments int,
    pre_tags text[],
    post_tags text[],
    tags_schema text,
    require_field_match boolean,

    highlight_query zdbquery,

    no_match_size int,
    fragmenter zdb.esqdsl_fragmenter_type,
    fragment_size int,
    fragment_offset int,
    force_source boolean,
    encoder zdb.esqdsl_encoder_type,
    boundary_scanner_locale text,
    boundary_scan_max int,
    boundary_chars text,
    phrase_limit int,

    matched_fields boolean,
    "order" text
);
CREATE OR REPLACE FUNCTION highlight(
    type zdb.esqdsl_highlight_type DEFAULT NULL,
    require_field_match boolean DEFAULT false,
    number_of_fragments int DEFAULT NULL,
    highlight_query zdbquery DEFAULT NULL,
    pre_tags text[] DEFAULT NULL,
    post_tags text[] DEFAULT NULL,
    tags_schema text DEFAULT NULL,
    no_match_size int DEFAULT NULL,

    fragmenter zdb.esqdsl_fragmenter_type DEFAULT NULL,
    fragment_size int DEFAULT NULL,
    fragment_offset int DEFAULT NULL,
    force_source boolean DEFAULT true,
    encoder zdb.esqdsl_encoder_type DEFAULT NULL,
    boundary_scanner_locale text DEFAULT NULL,
    boundary_scan_max int DEFAULT NULL,
    boundary_chars text DEFAULT NULL,
    phrase_limit int DEFAULT NULL,

    matched_fields boolean DEFAULT NULL,
    "order" text DEFAULT NULL
) RETURNS json PARALLEL SAFE IMMUTABLE LANGUAGE sql AS $$
    SELECT json_strip_nulls(row_to_json(
             ROW(type, number_of_fragments, pre_tags, post_tags, tags_schema,
                require_field_match, highlight_query, no_match_size, fragmenter,
                fragment_size, fragment_offset, force_source, encoder, boundary_scanner_locale,
                boundary_scan_max, boundary_chars, phrase_limit,
                matched_fields, "order"
        )::zdb.esqdsl_highlight)
    );
$$;
CREATE OR REPLACE FUNCTION highlight(ctid tid, field name, highlight_definition json DEFAULT highlight()) RETURNS text[] PARALLEL SAFE STABLE STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'zdb_highlight';
