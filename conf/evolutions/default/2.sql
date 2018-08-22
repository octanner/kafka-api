# --- !Ups

CREATE TABLE IF NOT EXISTS acl_source (
  user_id           TEXT                        DEFAULT uuid_generate_v4() PRIMARY KEY,
  username          TEXT UNIQUE,
  password          TEXT,
  cluster           TEXT,
  claimed           BOOLEAN,
  claimed_timestamp TIMESTAMP WITHOUT TIME ZONE,
  created_timestamp TIMESTAMP WITHOUT TIME ZONE DEFAULT now()
);

CREATE TABLE IF NOT EXISTS acl (
  acl_id            TEXT      DEFAULT uuid_generate_v4() PRIMARY KEY,
  user_id           TEXT NOT NULL,
  topic_id          TEXT NOT NULL,
  role              TEXT NOT NULL,
  cluster           TEXT,
  created_timestamp TIMESTAMP DEFAULT now(),
  CONSTRAINT acl_unique UNIQUE (user_id, topic_id, role)
);

# --- !Downs

DROP TABLE acl_source;
DROP TABLE acl;
