# --- !Ups
CREATE TABLE TOPIC_KEY_MAPPING (
  TOPIC_ID TEXT NOT NULL REFERENCES TOPIC(TOPIC_ID),
  KEY_TYPE TEXT NOT NULL,
  SCHEMA TEXT NULL,
  VERSION INT NULL,
  CLUSTER TEXT NOT NULL,
  CREATED_TIMESTAMP TIMESTAMP DEFAULT now(),
  PRIMARY KEY (TOPIC_ID)
);