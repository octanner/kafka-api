package e2e

import java.util.{ Properties, UUID }

import anorm._
import daos.TopicDao
import models.KeyType
import models.KeyType._
import models.Models.{ Topic, TopicConfiguration, TopicKeyType }
import models.http.HttpModels.{ SchemaRequest, SchemaResponse, TopicKeyMappingRequest, TopicRequest, TopicResponse, TopicSchemaMapping }
import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.kafka.clients.admin.{ AdminClient, AdminClientConfig }
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfterEach
import org.scalatest.mockito.MockitoSugar
import play.api.db.Database
import play.api.mvc.Results._
import play.api.inject.bind
import play.api.inject.guice.GuiceableModule
import play.api.libs.json.{ JsValue, Json }
import play.api.libs.ws.{ WSClient, WSRequest, WSResponse }
import play.api.{ Configuration, Logger }
import utils.AdminClientUtil
import utils.AdminClientUtil.ADMIN_CLIENT_ID

import scala.concurrent.Future

class TopicControllerTests extends IntTestSpec with BeforeAndAfterEach with MockitoSugar with EmbeddedKafka {
  val mockWs = mock[WSClient]
  val dao = new TopicDao()
  val cluster = "test"
  val avroTopicKeyType = TopicKeyType(KeyType.AVRO, Some("Test.Schema.Key"))
  val ledgerConfigSet = TopicConfiguration("ledger", Some("delete"), Some(3), Some(2629740000L), Some(1))
  val stateConfigSet = TopicConfiguration("state", Some("compact"), Some(3), Some(-1), Some(1))
  val eventConfigSet = TopicConfiguration("event", Some("delete"), Some(3), Some(2629740000L), Some(1))
  val schema = SchemaRequest("testSchema")

  val topic1 = Topic("test.some.topic.1", TopicConfiguration("ledger", None, None, None, None))
  val topic2 = Topic("test.some.topic.2", TopicConfiguration("state", Some("compact"), Some(1), Some(888888), Some(1)))
  val topic3 = Topic("test.some.topic.3", TopicConfiguration("event", Some("delete"), Some(1), Some(888888), Some(1)))
  val topic4 = Topic("test.some.topic.4", TopicConfiguration("event", Some("delete"), Some(1), Some(888888), Some(1)), Some(avroTopicKeyType), Some(List(schema.name)), Some(cluster))
  val topic4_id = UUID.randomUUID.toString

  val mapping = TopicSchemaMapping(topic1.name, schema)
  val keyMappingNone = TopicKeyMappingRequest(topic1.name, NONE, None)
  val keyMappingStr = TopicKeyMappingRequest(topic1.name, STRING, None)
  val keyMappingAvro = TopicKeyMappingRequest(topic1.name, AVRO, Some(schema))
  var conf: Configuration = _

  override def modulesToOverride: Seq[GuiceableModule] = Seq(
    bind[Database].toInstance(db),
    bind[TopicDao].toInstance(dao),
    bind[WSClient].toInstance(mockWs)
  )

  override def beforeAll() = {
    super.beforeAll()
    EmbeddedKafka.start()
    conf = app.injector.instanceOf[Configuration]
    db.withTransaction { implicit conn =>
      SQL"""
           insert into topic_config (name, cluster, description, cleanup_policy, partitions, retention_ms, replicas, created_timestamp)
           values ('state', $cluster,
           'A compacted topic with infinite retention, for keeping state of one type. Topic Key Type cannot be NONE. Only one value schema mapping will be allowed.',
            ${stateConfigSet.cleanupPolicy}, ${stateConfigSet.partitions}, ${stateConfigSet.retentionMs} , ${stateConfigSet.replicas}, now());
           insert into topic_config (name, cluster, description, cleanup_policy, partitions, retention_ms, replicas, created_timestamp)
           values ('ledger', $cluster,
           'A non-compacted audit-log style topic for tracking changes in one value type. Only one value schema mapping will be allowed.',
           ${ledgerConfigSet.cleanupPolicy}, ${ledgerConfigSet.partitions}, ${ledgerConfigSet.retentionMs} , ${ledgerConfigSet.replicas}, now());
           insert into topic_config (name, cluster, description, cleanup_policy, partitions, retention_ms, replicas, created_timestamp)
           values ('event', $cluster,
           'A non-compacted event-stream style topic which may contain multiple types of values. Multiple value schema mapping will be allowed.',
           ${eventConfigSet.cleanupPolicy}, ${eventConfigSet.partitions}, ${eventConfigSet.retentionMs} , ${eventConfigSet.replicas}, now());
           insert into topic (topic_id, cluster, topic, config_name, partitions, replicas, retention_ms, cleanup_policy) values
           ($topic4_id, $cluster, ${topic4.name}, ${topic4.config.name}, ${topic4.config.partitions}, ${topic4.config.replicas}, ${topic4.config.retentionMs}, ${topic4.config.cleanupPolicy});
           insert into topic_key_mapping (cluster, topic_id, key_type, schema) values
           ($cluster, ${topic4_id}, ${avroTopicKeyType.keyType.toString}, ${avroTopicKeyType.schema});
           insert into topic_schema_mapping (cluster, topic_id, schema) values
           ($cluster, ${topic4_id}, ${schema.name});
        """.execute()
    }
  }

  override def afterAll(): Unit = {
    db.withTransaction { implicit conn =>
      SQL"""
           delete from TOPIC_CONFIG;
           delete from TOPIC_KEY_MAPPING;
           delete from TOPIC_SCHEMA_MAPPING;
           delete from TOPIC;
        """.execute()
    }
    EmbeddedKafka.stop()
    super.afterAll()
  }

  def setMockRequestResponseExpectations(url: String, status: Int, json: JsValue = Json.parse("{}")): (WSRequest, WSResponse) = {
    val mockReq = mock[WSRequest]
    val mockResp = mock[WSResponse]

    when(mockWs.url(url)) thenReturn mockReq
    when(mockReq.withHttpHeaders(any[(String, String)])) thenReturn mockReq
    when(mockReq.get()) thenReturn Future.successful(mockResp)
    when(mockResp.status) thenReturn status
    when(mockResp.json) thenReturn json
    (mockReq, mockResp)
  }

  private def getTopicFromDB(topicName: String): Option[Topic] = {
    db.withTransaction { implicit conn =>
      dao.getTopicInfo(topicName)
    }
  }

  private def topicExistsInKafka(topicName: String): Boolean = {
    val kafkaHostName = conf.get[String](cluster.toLowerCase + AdminClientUtil.KAFKA_LOCATION_CONFIG)
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHostName)
    props.put(AdminClientConfig.CLIENT_ID_CONFIG, ADMIN_CLIENT_ID)

    val adminClient = AdminClient.create(props)
    val allTopics = adminClient.listTopics().names().get()
    adminClient.close()
    allTopics.contains(topicName)
  }

  private def createTopic(topic: Topic) = {
    val futureResult = wsUrl(s"/v1/kafka/cluster/$cluster/topic")
      .post(Json.toJson(TopicRequest(topic)))
    val result = futureResult.futureValue
    Status(result.status) mustBe Ok
    result
  }

  "Topic Controller #create" must {
    "create a topic in kafka cluster and database" in {
      val result = createTopic(topic1)
      val expectedTopic = topic1.copy(config = ledgerConfigSet, cluster = Some(cluster))
      result.json mustBe Json.toJson(TopicResponse(expectedTopic))
      topicExistsInKafka(topic1.name) mustBe true
      getTopicFromDB(topic1.name) mustBe Some(expectedTopic)
    }
  }

  "Topic Controller #get" must {
    "get a list of all topics" in {
      val futureResult = wsUrl(s"/v1/kafka/topics").get()
      val result = futureResult.futureValue
      val expectedJson = Json.obj("topics" -> Seq(
        Json.toJson(topic4.copy(schemas = None)),
        Json.toJson(topic1.copy(config = ledgerConfigSet, cluster = Some(cluster)))))

      Status(result.status) mustBe Ok
      result.json mustBe expectedJson
    }

    "get a single topic by name" in {
      val futureResult = wsUrl(s"/v1/kafka/topics/${topic1.name}").get()
      val result = futureResult.futureValue
      val expectedJson = Json.toJson(TopicResponse(topic1.copy(config = ledgerConfigSet, cluster = Some(cluster))))

      Status(result.status) mustBe Ok
      result.json mustBe expectedJson
    }

    "get a single topic by name with key mapping and schema mapping" in {
      val futureResult = wsUrl(s"/v1/kafka/topics/${topic4.name}").get()
      val result = futureResult.futureValue
      val expectedJson = Json.toJson(TopicResponse(topic4))
      println(result.body)
      Status(result.status) mustBe Ok
      result.json mustBe expectedJson
    }

    "return 404 if no topic found by given name" in {
      val fakeTopicName = "this.doesnt.exist"
      val futureResult = wsUrl(s"/v1/kafka/topics/$fakeTopicName").get()
      val result = futureResult.futureValue
      val expectedResponse = s"Cannot find topic '$fakeTopicName'"

      Status(result.status) mustBe NotFound
      result.body mustBe expectedResponse
    }
  }

  "Topic Controller #createSchemaMapping" must {
    "return Ok and create a mapping for topic and schema" in {
      val conf = app.injector.instanceOf[Configuration]
      val url = s"${conf.get[String](cluster.toLowerCase + ".kafka.avro.registry.location")}/subjects/${mapping.schema.name}/versions/latest"
      setMockRequestResponseExpectations(url, 200, Json.toJson(SchemaResponse(schema.name, 2, "")))
      val result = wsUrl(s"/v1/kafka/cluster/$cluster/topic-schema-mapping")
        .post(Json.toJson(mapping)).futureValue
      println(s"${result.status}: ${result.body}")
      Status(result.status) mustBe Ok
      result.json.as[TopicSchemaMapping] mustBe mapping
    }

    "return NotFound when topic is not found" in {
      val schema = SchemaRequest("testSchema")
      val mapping = TopicSchemaMapping("randomTopic", schema)
      val result = wsUrl(s"/v1/kafka/cluster/$cluster/topic-schema-mapping")
        .post(Json.toJson(mapping)).futureValue
      println(s"${result.status}: ${result.body}")
      Status(result.status) mustBe NotFound
    }

    "return NotFound when schema is not found" in {
      val schema = SchemaRequest("testSchema")
      val mapping = TopicSchemaMapping(topic1.name, schema)
      val conf = app.injector.instanceOf[Configuration]
      val url = s"${conf.get[String](cluster.toLowerCase + ".kafka.avro.registry.location")}/subjects/${mapping.schema.name}/versions/latest"
      setMockRequestResponseExpectations(url, 404, Json.obj("error" -> "Not Found"))
      val result = wsUrl(s"/v1/kafka/cluster/$cluster/topic-schema-mapping")
        .post(Json.toJson(mapping)).futureValue
      println(s"${result.status}: ${result.body}")
      Status(result.status) mustBe NotFound
    }
  }

  "Topic Controller #getTopicSchemaMappings" must {
    "return Ok and return list of schema mappings" in {
      val result = wsUrl(s"/v1/kafka/cluster/$cluster/topics/${topic1.name}/topic-schema-mappings")
        .get().futureValue
      println(s"${result.status}: ${result.body}")
      Status(result.status) mustBe Ok
      (result.json \ "mappings").as[List[TopicSchemaMapping]] mustBe List(mapping)
    }
  }

  "Topic Controller #createTopicKeyMapping" must {
    "return Ok for key type none mapping" in {
      val req = s"""{"topic": "${topic1.name}", "keyType": "None"}"""
      val result = wsUrl(s"/v1/kafka/cluster/$cluster/topic-key-mapping")
        .post(Json.parse(req)).futureValue
      println(s"${result.status}: ${result.body}")
      Status(result.status) mustBe Ok
    }

    "return BadRequest for duplicate key mapping request" in {
      val conf = app.injector.instanceOf[Configuration]
      val url = s"${conf.get[String](cluster.toLowerCase + ".kafka.avro.registry.location")}/subjects/${mapping.schema.name}/versions/latest"
      setMockRequestResponseExpectations(url, 200, Json.toJson(SchemaResponse(schema.name, 2, "")))

      val req = s"""{"topic": "${topic1.name}", "keyType": "None"}"""
      val result = wsUrl(s"/v1/kafka/cluster/$cluster/topic-key-mapping")
        .post(Json.parse(req)).futureValue
      println(s"${result.status}: ${result.body}")
      Status(result.status) mustBe BadRequest
    }

    "return Ok for key type string mapping" in {
      createTopic(topic2)

      val req = s"""{"topic": "${topic2.name}", "keyType": "string"}"""
      val result = wsUrl(s"/v1/kafka/cluster/$cluster/topic-key-mapping")
        .post(Json.parse(req)).futureValue
      println(s"${result.status}: ${result.body}")
      Status(result.status) mustBe Ok
    }

    "return Ok for key type avro mapping" in {
      createTopic(topic3)
      val conf = app.injector.instanceOf[Configuration]
      val url = s"${conf.get[String](cluster.toLowerCase + ".kafka.avro.registry.location")}/subjects/${mapping.schema.name}/versions/latest"
      setMockRequestResponseExpectations(url, 200, Json.toJson(SchemaResponse(schema.name, 2, "")))

      val req = s"""{"topic": "${topic3.name}", "keyType": "AVRO", "schema": {"name": "${schema.name}"}}"""
      val result = wsUrl(s"/v1/kafka/cluster/$cluster/topic-key-mapping")
        .post(Json.parse(req)).futureValue
      println(s"${result.status}: ${result.body}")
      Status(result.status) mustBe Ok
    }

    "return Bad Request for key type avro mapping without specifying schema" in {
      createTopic(topic3)
      val conf = app.injector.instanceOf[Configuration]
      val url = s"${conf.get[String](cluster.toLowerCase + ".kafka.avro.registry.location")}/subjects/${mapping.schema.name}/versions/latest"
      setMockRequestResponseExpectations(url, 200, Json.toJson(SchemaResponse(schema.name, 2, "")))

      val req = s"""{"topic": "${topic3.name}", "keyType": "AVRO"}"""
      val result = wsUrl(s"/v1/kafka/cluster/$cluster/topic-key-mapping")
        .post(Json.parse(req)).futureValue
      println(s"${result.status}: ${result.body}")
      Status(result.status) mustBe BadRequest
    }
  }

  "Topic Controller #getAllConfigSets" must {
    "return Ok and return list of Config Sets" in {
      val result = wsUrl(s"/v1/kafka/cluster/$cluster/configs")
        .get().futureValue
      println(s"${result.status}: ${result.body}")
      Status(result.status) mustBe Ok
      (result.json \ "configs").as[List[TopicConfiguration]] mustBe List(stateConfigSet, ledgerConfigSet, eventConfigSet)
    }
  }

  "Topic Controller #getConfigSet" must {
    "return Ok and return Config Set" in {
      val result = wsUrl(s"/v1/kafka/cluster/$cluster/configs/ledger")
        .get().futureValue
      println(s"${result.status}: ${result.body}")
      Status(result.status) mustBe Ok
      (result.json \ "config").as[TopicConfiguration] mustBe ledgerConfigSet
    }
    "return Not Found when config set does not exist" in {
      val result = wsUrl(s"/v1/kafka/cluster/$cluster/configs/ledger2")
        .get().futureValue
      println(s"${result.status}: ${result.body}")
      Status(result.status) mustBe NotFound
    }
  }
}
