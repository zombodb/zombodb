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
