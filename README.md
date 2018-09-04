# Broker for Kafka in Akkeris

## Synopis

Runs a REST API to offer management of Kafka topics, users, and ACLs, schema registry Avro schema mappings to topic

## Endpoints

```
get /v1/kafka/topics/:topic
get /v1/kafka/topics
get /v1/kafka/cluster/:cluster/credentials/:user
get /v1/kafka/cluster/:cluster/acls
get /v1/kafka/cluster/:cluster/schemas
get /v1/kafka/cluster/:cluster/schemas/:schema/versions
get /v1/kafka/cluster/:cluster/schemas/:schema/versions/:version
get /v1/kafka/cluster/:cluster/topics/:topic/topic-schema-mappings
post /v1/kafka/cluster/:cluster/topic
post /v1/kafka/cluster/:cluster/user
post /v1/kafka/cluster/:cluster/acls
delete /v1/kafka/acls/:id
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

## Run

```
source local_env.sh
sbt run
```

##DB setup
Insert all the the user names and passwords for all required kafka cluster(ex: dev, sandbox, nonprod, prod) to `acl_source` database table, with claimed being false. 

##Endpoints Description

### GET /v1/kafka/topics/:topic
#### Request Params 
None
#### Response
```json
{  
   "topic":{  
      "name":"test.some.topic",
      "description":"Test topic creation",
      "organization":"testOrg",
      "config":{  
         "cleanup.policy":"delete",
         "partitions":1,
         "retention.ms":888888,
         "replicas":1
      }
   }
}
```
#### Description
Get Topic info on a specific topic
#### Response Codes
200: Ok with response Json

404: Cannot find topic :topic

500: Internal Server Error
______

### GET /v1/kafka/topics
#### Request Params 
None
#### Response
```json
{  
   "topics":[  
      {  
         "name":"test.some.topic",
         "description":"Test topic creation",
         "organization":"testOrg",
         "config":{  
            "cleanup.policy":"delete",
            "partitions":1,
            "retention.ms":888888,
            "replicas":1
         }
      }
   ]
}
```
#### Description
Gets topics across all clusters known to the app. Gets this information from `topic` table
#### Response Codes
200: Ok with above response

500: Internal Service Error
______

### GET /v1/kafka/cluster/:cluster/credentials/:user
#### Request Params 
None
#### Response
```Json
{  
   "username":"testusername",
   "password":"testpassword"
}
```
#### Description
Endpoint to get credentials for a claimed user.
#### Response Codes
200: Ok with above response. 

400: User does not exist or is unclaimed.
```Json
{  
   "errors":[  
      {  
         "title":"Invalid User",
         "detail":"Either user :user does not exist in cluster test or the user is not claimed."
      }
   ]
}
```
______

### GET /v1/kafka/cluster/:cluster/acls
#### Request Params 
topic: Topic name. Required string.
#### Response
```Json
{  
   "acls":[  
      {  
         "id":"7ab2d06a-937d-4da4-8bc7-323e219c34f5",
         "user":"testusername",
         "topic":"test.some.topic",
         "cluster":"test",
         "role":"consumer"
      },
      {  
         "id":"76f06077-c50c-4242-8e08-0444e579a60e",
         "user":"testusername",
         "topic":"test.some.topic",
         "cluster":"test",
         "role":"producer"
      }
   ]
}
```
#### Description
Gets all Acls for the given topic.
#### Response Codes
200: Ok with above response

400: Invalid ACL role value in the database.

______

### GET /v1/kafka/cluster/:cluster/schemas
#### Request Params 
None
#### Response
```Json
{  
   "schemas":[  
      "schema1",
      "schema2"
   ]
}
```
#### Description
Lists all Schema subjects for the given cluster. Get this result from schema registry.
#### Response Codes
200: With above response

500: Internal Service Error

### GET /v1/kafka/cluster/:cluster/schemas/:schema/versions
#### Request Params 
None
#### Response
```Json
{  
   "versions":[  
      1,
      2,
      3
   ]
}
```
#### Description
Makes a rest call to cluster's schema registry to get all versions for the given schema subject. 
#### Response Codes
200: With above response

404: Schema with subject `$schema` not found

500: External service exception

________

### GET /v1/kafka/cluster/:cluster/schemas/:schema/versions/:version
#### Request Params 
None
#### Response
```Json
{  
   "subject":"test.schema1",
   "version":1,
   "schema":"{\"type\": \"string\"}"
}
```

#### Description
Get Schema Details and schema for the cluster, schema subject and version. Call Schema registry to get the same.
#### Response Codes
200: Ok with above response

404: Schema not found for `$schema` version `$version`

500: Internal Service Error
____

### GET /v1/kafka/cluster/:cluster/topics/:topic/topic-schema-mappings
#### Request Params 
None
#### Response
```Json
{  
   "mappings":[  
      {  
         "topic":"test.some.topic",
         "schema":{  
            "name":"testSchema",
            "version":1
         }
      }
   ]
}
```
#### Description
Get all the schema mappings for the topic from database.
#### Response Codes
200: Ok with above response

500: Internal Service Error
________

### POST /v1/kafka/cluster/:cluster/topic
#### Request Body 
```Json
{  
   "topic":{  
      "name":"test.some.topic",
      "description":"Test topic creation",
      "organization":"testOrg",
      "config":{  
         "cleanup.policy":"delete",
         "partitions":1,
         "retention.ms":888888,
         "replicas":1
      }
   }
}
```

#### Response
```json
{  
   "topic":{  
      "name":"test.some.topic",
      "description":"Test topic creation",
      "organization":"testOrg",
      "config":{  
         "cleanup.policy":"delete",
         "partitions":1,
         "retention.ms":888888,
         "replicas":1
      }
   }
}
```

#### Description
Create a topic with delete or compact topic type. 
#### Response Codes
200: With above response.

500: Internal Service Error
______

### POST /v1/kafka/cluster/:cluster/user
#### Request Body 
None
#### Response
```Json
{  
   "aclCredentials":{  
      "username":"testusername",
      "password":"testpassword"
   }
}
```
#### Description
Endpoint to Claim the user.
#### Response Codes
200: with above response

500: Internal Service Error
________

### POST /v1/kafka/cluster/:cluster/acls
#### Request Body 
```Json
{  
   "topic":"test.some.topic",
   "user":"testusername",
   "role":"Producer"
}
```
#### Response
{  
   "id":"901125d6-0e47-43c7-92d5-a9d8229c7f0f",
}
#### Description
Create Acl for a topic, user and role

#### Response Codes
200: Ok with above response

500: Internal Server Error

### delete /v1/kafka/acls/:id
#### Request Params 
None
#### Response

#### Description
Delete the Acl using Acl id.
 
#### Response Codes
200

_________
