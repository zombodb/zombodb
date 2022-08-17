CREATE TABLE events
(
    id           bigint NOT NULL PRIMARY KEY,
    event_type   character varying(50),
    event_public boolean,
    repo_id      bigint,
    payload      jsonb,
    repo         jsonb,
    user_id      bigint,
    org          jsonb,
    created_at   timestamp without time zone
);

CREATE TABLE users
(
    id            bigint NOT NULL PRIMARY KEY,
    url           text,
    login         text,
    avatar_url    text,
    gravatar_id   text,
    display_login text
);

CREATE TABLE public.products
(
    id                bigint NOT NULL PRIMARY KEY,
    name              text   NOT NULL,
    keywords          character varying(64)[],
    short_summary     text,
    long_description  text,
    price             bigint,
    inventory_count   integer,
    discontinued      boolean DEFAULT false,
    availability_date date
);

CREATE TABLE public.so_comments
(
    creation_date     timestamp with time zone,
    id                bigint NOT NULL PRIMARY KEY,
    post_id           bigint,
    score             integer,
    comment_text      text,
    user_display_name character varying(512),
    user_id           bigint
);

CREATE TABLE public.so_posts
(
    accepted_answer_id       bigint,
    answer_count             integer,
    body                     text,
    closed_date              timestamp with time zone,
    comment_count            integer,
    community_owned_date     timestamp with time zone,
    creation_date            timestamp with time zone,
    favorite_count           integer,
    id                       bigint NOT NULL PRIMARY KEY,
    last_activity_date       timestamp with time zone,
    last_editor_date         timestamp with time zone,
    last_editor_display_name varchar,
    last_editor_user_id      bigint,
    owner_display_name       varchar,
    owner_user_id            bigint,
    parent_id                bigint,
    post_type_id             bigint,
    score                    double precision,
    tags                     text,
    title                    text,
    view_count               bigint
);

CREATE TABLE public.so_users
(
    about_me          text,
    account_id        bigint,
    age               integer,
    creation_date     timestamp with time zone,
    display_name      text,
    down_votes        integer,
    id                bigint NOT NULL PRIMARY KEY,
    last_access_date  timestamp with time zone,
    location          text,
    profile_image_url text,
    reputation        integer,
    up_votes          integer,
    views             bigint,
    website_url       text
);

CREATE TABLE public.words
(
    id   bigint NOT NULL PRIMARY KEY,
    word text   NOT NULL
);

CREATE TABLE public.data
(
    pk_data                  BIGINT,
    data_bigint_1            BIGINT,
    data_bigint_expand_group BIGINT,
    data_bigint_array_1      BIGINT[],
    data_bigint_array_2      BIGINT[],
    data_boolean             BOOLEAN,
    data_char_1              CHAR(2),
    data_char_2              CHAR(2),
    data_char_array_1        CHAR(2)[],
    data_char_array_2        CHAR(2)[],
    data_date_1              DATE,
    data_date_2              DATE,
    data_date_array_1        DATE[],
    data_date_array_2        DATE[],
    data_full_text           zdb.fulltext,
    data_full_text_shingles  zdb.fulltext_with_shingles,
    data_int_1               INT,
    data_int_2               INT,
    data_int_array_1         INT[],
    data_int_array_2         INT[],
    data_json                JSON,
    data_phrase_1            zdb.phrase,
    data_phrase_2            zdb.phrase,
    data_phrase_array_1      zdb.phrase_array,
    data_phrase_array_2      zdb.phrase_array,
    data_text_1              VARCHAR,
    data_text_filter         VARCHAR,
    data_text_array_1        VARCHAR[],
    data_text_array_2        VARCHAR[],
    data_timestamp           TIMESTAMP,
    data_varchar_1           VARCHAR(25),
    data_varchar_2           VARCHAR(25),
    data_varchar_array_1     VARCHAR(25)[],
    data_varchar_array_2     VARCHAR(25)[],
    CONSTRAINT idx_unit_tests_data_pkey PRIMARY KEY (pk_data)
);

CREATE TABLE public.var
(
    pk_var                  BIGINT,
    var_bigint_1            BIGINT,
    var_bigint_expand_group BIGINT,
    var_bigint_array_1      BIGINT[],
    var_bigint_array_2      BIGINT[],
    var_boolean             BOOLEAN,
    var_char_1              CHAR(2),
    var_char_2              CHAR(2),
    var_char_array_1        CHAR(2)[],
    var_char_array_2        CHAR(2)[],
    var_date_1              DATE,
    var_date_2              DATE,
    var_date_array_1        DATE[],
    var_date_array_2        DATE[],
    var_int_1               INT,
    var_int_2               INT,
    var_int_array_1         INT[],
    var_int_array_2         INT[],
    var_json                JSON,
    var_phrase_1            zdb.phrase,
    var_phrase_2            zdb.phrase,
    var_phrase_array_1      zdb.phrase_array,
    var_phrase_array_2      zdb.phrase_array,
    var_text_1              VARCHAR,
    var_text_filter         VARCHAR,
    var_text_array_1        VARCHAR[],
    var_text_array_2        VARCHAR[],
    var_timestamp           TIMESTAMP,
    var_varchar_1           VARCHAR(25),
    var_varchar_2           VARCHAR(25),
    var_varchar_array_1     VARCHAR(25)[],
    var_varchar_array_2     VARCHAR(25)[],
    CONSTRAINT idx_unit_tests_var_pkey PRIMARY KEY (pk_var)
);

CREATE TABLE public.vol
(
    pk_vol                  BIGINT,
    vol_bigint_1            BIGINT,
    vol_bigint_expand_group BIGINT,
    vol_bigint_array_1      BIGINT[],
    vol_bigint_array_2      BIGINT[],
    vol_boolean             BOOLEAN,
    vol_char_1              CHAR(2),
    vol_char_2              CHAR(2),
    vol_char_array_1        CHAR(2)[],
    vol_char_array_2        CHAR(2)[],
    vol_date_1              DATE,
    vol_date_2              DATE,
    vol_date_array_1        DATE[],
    vol_date_array_2        DATE[],
    vol_int_1               INT,
    vol_int_2               INT,
    vol_int_array_1         INT[],
    vol_int_array_2         INT[],
    vol_json                JSON,
    vol_phrase_1            zdb.phrase,
    vol_phrase_2            zdb.phrase,
    vol_phrase_array_1      zdb.phrase_array,
    vol_phrase_array_2      zdb.phrase_array,
    vol_text_1              VARCHAR,
    vol_text_filter         VARCHAR,
    vol_text_array_1        VARCHAR[],
    vol_text_array_2        VARCHAR[],
    vol_timestamp           TIMESTAMP,
    vol_varchar_1           VARCHAR(25),
    vol_varchar_2           VARCHAR(25),
    vol_varchar_array_1     VARCHAR(25)[],
    vol_varchar_array_2     VARCHAR(25)[],
    CONSTRAINT idx_unit_tests_vol_pkey PRIMARY KEY (pk_vol)
);

CREATE VIEW public.consolidated_record_view AS
SELECT pk_data
     , pk_var
     , pk_vol
     , data_bigint_1
     , data_bigint_expand_group
     , data_bigint_array_1
     , data_bigint_array_2
     , data_boolean
     , data_char_1
     , data_char_2
     , data_char_array_1
     , data_char_array_2
     , data_date_1
     , data_date_2
     , data_date_array_1
     , data_date_array_2
     , data_full_text
     , data_full_text_shingles
     , data_int_1
     , data_int_2
     , data_int_array_1
     , data_int_array_2
     , data_json
     , data_phrase_1
     , data_phrase_2
     , data_phrase_array_1
     , data_phrase_array_2
     , data_text_1
     , data_text_filter
     , data_text_array_1
     , data_text_array_2
     , data_timestamp
     , data_varchar_1
     , data_varchar_2
     , data_varchar_array_1
     , data_varchar_array_2
     , var_bigint_1
     , var_bigint_expand_group
     , var_bigint_array_1
     , var_bigint_array_2
     , var_boolean
     , var_char_1
     , var_char_2
     , var_char_array_1
     , var_char_array_2
     , var_date_1
     , var_date_2
     , var_date_array_1
     , var_date_array_2
     , var_int_1
     , var_int_2
     , var_int_array_1
     , var_int_array_2
     , var_json
     , var_phrase_1
     , var_phrase_2
     , var_phrase_array_1
     , var_phrase_array_2
     , var_text_1
     , var_text_filter
     , var_text_array_1
     , var_text_array_2
     , var_timestamp
     , var_varchar_1
     , var_varchar_2
     , var_varchar_array_1
     , var_varchar_array_2
     , vol_bigint_1
     , vol_bigint_expand_group
     , vol_bigint_array_1
     , vol_bigint_array_2
     , vol_boolean
     , vol_char_1
     , vol_char_2
     , vol_char_array_1
     , vol_char_array_2
     , vol_date_1
     , vol_date_2
     , vol_date_array_1
     , vol_date_array_2
     , vol_int_1
     , vol_int_2
     , vol_int_array_1
     , vol_int_array_2
     , vol_json
     , vol_phrase_1
     , vol_phrase_2
     , vol_phrase_array_1
     , vol_phrase_array_2
     , vol_text_1
     , vol_text_filter
     , vol_text_array_1
     , vol_text_array_2
     , vol_timestamp
     , vol_varchar_1
     , vol_varchar_2
     , vol_varchar_array_1
     , vol_varchar_array_2
     , data      AS zdb
     , data.ctid as data_ctid
FROM data
         LEFT JOIN var ON data.pk_data = var.pk_var
         LEFT JOIN vol ON data.pk_data = vol.pk_vol;