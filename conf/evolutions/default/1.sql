# --- !Ups

CREATE TABLE  IF NOT EXISTS topic (
    topic_id text DEFAULT uuid_generate_v4() PRIMARY KEY,
    topic text UNIQUE,
    partitions integer,
    replicas integer,
    retention_ms integer,
    cleanup_policy text,
    created_timestamp timestamp without time zone,
    cluster text,
    organization text,
    updated_timestamp timestamp without time zone DEFAULT now(),
    description text
);

# --- !Downs
DROP TABLE topic;
