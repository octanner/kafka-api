# --- !Ups

ALTER TABLE TOPIC ADD COLUMN CONFIG_NAME TEXT DEFAULT 'ledger';
ALTER TABLE TOPIC DROP COLUMN ORGANIZATION;
ALTER TABLE TOPIC DROP COLUMN DESCRIPTION;
ALTER TABLE TOPIC ALTER COLUMN RETENTION_MS TYPE BIGINT;

CREATE TABLE TOPIC_CONFIG (
  TOPIC_CONFIG_ID TEXT DEFAULT uuid_generate_v4() PRIMARY KEY,
  NAME TEXT NOT NULL,
  CLUSTER TEXT NOT NULL,
  DESCRIPTION TEXT NOT NULL,
  CLEANUP_POLICY TEXT NOT NULL,
  PARTITIONS INT NOT NULL,
  RETENTION_MS BIGINT NOT NULL,
  REPLICAS INT NOT NULL,
  CREATED_TIMESTAMP TIMESTAMP,
  UPDATED_TIMESTAMP TIMESTAMP DEFAULT now(),
  CONSTRAINT topic_config_unique UNIQUE (NAME, CLUSTER)
);

INSERT INTO TOPIC_CONFIG(NAME, CLUSTER, DESCRIPTION, CLEANUP_POLICY, PARTITIONS, RETENTION_MS, REPLICAS, CREATED_TIMESTAMP)
    VALUES ('state', 'maru',
    'A compacted topic with infinite retention, for keeping state of one type. Topic Key Type cannot be NONE. Only one value schema mapping will be allowed.',
    'compact', 3, -1, 3, now());
INSERT INTO TOPIC_CONFIG(NAME, CLUSTER, DESCRIPTION, CLEANUP_POLICY, PARTITIONS, RETENTION_MS, REPLICAS, CREATED_TIMESTAMP)
    VALUES ('ledger', 'maru',
    'A non-compacted audit-log style topic for tracking changes in one value type. Only one value schema mapping will be allowed.',
    'delete', 3, 2629740000, 3, now());
INSERT INTO TOPIC_CONFIG(NAME, CLUSTER, DESCRIPTION, CLEANUP_POLICY, PARTITIONS, RETENTION_MS, REPLICAS, CREATED_TIMESTAMP)
    VALUES ('event', 'maru',
    'A non-compacted event-stream style topic which may contain multiple types of values. Multiple value schema mapping will be allowed.',
    'delete', 3, 2629740000, 3, now());