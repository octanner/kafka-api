# --- !Ups

-- Attempt to install the extensions, but trap errors (in case in lower lanes without permission)
do language plpgsql $$
begin
  CREATE EXTENSION IF NOT EXISTS "uuid-ossp";;
  exception when others then
    raise notice 'Unable to create extensions for tag searching. Please verify you have database ';;
    raise notice '% %', SQLERRM, SQLSTATE;;
end;;
$$;

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
