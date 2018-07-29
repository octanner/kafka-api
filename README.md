# Broker for Kafka in Akkeris

## Synopis

Runs a REST API to offer management of Kafka topics, consumergroups, users, and ACLs

## Details

```
get /v1/kafka/cluster/:cluster/consumergroups
get /v1/kafka/cluster/:cluster/credentials/:username
get /v1/kafka/cluster/:cluster/topic/:topic
get /v1/kafka/cluster/:cluster/topic/:topic/consumergroups
get /v1/kafka/cluster/:cluster/topics
post /v1/kafka/cluster/:cluster/topic
post /v1/kafka/cluster/:cluster/user
post /v1/kafka/cluster/:cluster/user/:username/topic/:topic/consumergroup/rotate
put /v1/kafka/cluster/:cluster/acl/user/:user/topic/:topic/role/:role
put /v1/kafka/cluster/:cluster/topic/:topic/retentionms/:retentionms
delete /v1/kafka/cluster/:cluster/topic/:topic


JSON payload exmample for creation of a topic is

{
  "topic": {
    "name": "tt9",
    "description":"User.Friendly.Name",
    "organization":"testorg",
    "config": {
      "cleanup.policy": "delete",
      "partitions": 8,
      "retention.ms":8888
    }
  }
}


```


## Runtime Environment Variables
```
SANDBOX_KAFKA_PORT
SANDBOX_DEFAULT_PARTITIONS
SANDBOX_DEFAULT_REPLICAS
SANDBOX_DEFAULT_RETENTION
SANDBOX_KAFKA_ADMIN_USERNAME
SANDBOX_KAFKA_ADMIN_PASSWORD
SANDBOX_KAFKA_LOCATION
SANDBOX_KAFKA_HOSTNAME
SANDBOX_ZK
SANDBOX_KAFKA_AVRO_REGISTRY_LOCATION
SANDBOX_KAFKA_AVRO_REGISTRY_HOSTNAME
SANDBOX_KAFKA_AVRO_REGISTRY_PORT
DEV_KAFKA_PORT
DEV_DEFAULT_PARTITIONS
DEV_DEFAULT_REPLICAS
DEV_DEFAULT_RETENTION
DEV_KAFKA_ADMIN_USERNAME
DEV_KAFKA_ADMIN_PASSWORD
DEV_KAFKA_LOCATION
DEV_KAFKA_HOSTNAME
DEV_ZK
DEV_KAFKA_AVRO_REGISTRY_LOCATION
DEV_KAFKA_AVRO_REGISTRY_HOSTNAME
DEV_KAFKA_AVRO_REGISTRY_PORT
PRODUCTION_KAFKA_POST
PRODUCTION_DEFAULT_PARTITIONS
PRODUCTION_DEFAULT_REPLICAS
PRODUCTION_DEFAULT_RETENTION
PRODUCTION_KAFKA_ADMIN_USERNAME
PRODUCTION_KAFKA_ADMIN_PASSWORD
PRODUCTION_KAFKA_LOCATION
PRODUCTION_KAFKA_HOSTNAME
PRODUCTION_ZK
PRODUCTION_KAFKA_AVRO_REGISTRY_LOCATION
PRODUCTION_KAFKA_AVRO_REGISTRY_HOSTNAME
PRODUCTION_KAFKA_AVRO_REGISTRY_PORT
BROKERDB
BROKERDBUSER
BROKERDBPASS
PORT
```
## Build

```
mvn dependency:resolve
mvn verify
mvn package
```

## Run

```
java -jar target/kafka-api-jar-with-dependencies.jar
```



