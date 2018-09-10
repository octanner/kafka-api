package e2e

import java.util.Properties

import anorm._
import daos.TopicDao
import models.KeyType._
import models.Models.{ Topic, TopicConfiguration }
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
  val topic1 = Topic("test.some.topic.1", "Test topic creation 1", "testOrg", TopicConfiguration(Some("delete"), Some(1), Some(888888), Some(1)))
  val topic2 = Topic("test.some.topic.2", "Test topic creation 2", "testOrg", TopicConfiguration(Some("compact"), Some(1), Some(888888), Some(1)))
  val topic3 = Topic("test.some.topic.3", "Test topic creation 3", "testOrg", TopicConfiguration(Some("delete"), Some(1), Some(888888), Some(1)))
  val schema = SchemaRequest("testSchema", 1)
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
  }

  override def afterAll(): Unit = {
    db.withTransaction { implicit conn =>
      SQL"""
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
      result.json mustBe Json.toJson(TopicResponse(topic1))
      topicExistsInKafka(topic1.name) mustBe true
      getTopicFromDB(topic1.name) mustBe Some(topic1)
    }
  }

  "Topic Controller #get" must {
    "get a list of all topics" in {
      val futureResult = wsUrl(s"/v1/kafka/topics").get()
      val result = futureResult.futureValue
      val expectedJson = Json.obj("topics" -> Seq(Json.toJson(topic1)))

      Status(result.status) mustBe Ok
      result.json mustBe expectedJson
    }

    "get a single topic by name" in {
      val futureResult = wsUrl(s"/v1/kafka/topics/${topic1.name}").get()
      val result = futureResult.futureValue
      val expectedJson = Json.toJson(TopicResponse(topic1))

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
      val url = s"${conf.get[String](cluster.toLowerCase + ".kafka.avro.registry.location")}/subjects/${mapping.schema.name}/versions/${mapping.schema.version}"
      setMockRequestResponseExpectations(url, 200, Json.toJson(SchemaResponse(schema.name, schema.version, "")))
      val result = wsUrl(s"/v1/kafka/cluster/$cluster/topic-schema-mapping")
        .post(Json.toJson(mapping)).futureValue
      println(s"${result.status}: ${result.body}")
      Status(result.status) mustBe Ok
      result.json.as[TopicSchemaMapping] mustBe mapping
    }

    "return NotFound when topic is not found" in {
      val schema = SchemaRequest("testSchema", 1)
      val mapping = TopicSchemaMapping("randomTopic", schema)
      val result = wsUrl(s"/v1/kafka/cluster/$cluster/topic-schema-mapping")
        .post(Json.toJson(mapping)).futureValue
      println(s"${result.status}: ${result.body}")
      Status(result.status) mustBe NotFound
    }

    "return NotFound when schema is not found" in {
      val schema = SchemaRequest("testSchema", 1)
      val mapping = TopicSchemaMapping(topic1.name, schema)
      val conf = app.injector.instanceOf[Configuration]
      val url = s"${conf.get[String](cluster.toLowerCase + ".kafka.avro.registry.location")}/subjects/${mapping.schema.name}/versions/${mapping.schema.version}"
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
      val conf = app.injector.instanceOf[Configuration]
      val url = s"${conf.get[String](cluster.toLowerCase + ".kafka.avro.registry.location")}/subjects/${mapping.schema.name}/versions/${mapping.schema.version}"
      setMockRequestResponseExpectations(url, 200, Json.toJson(SchemaResponse(schema.name, schema.version, "")))

      val req = s"""{"topic": "${topic1.name}", "keyType": "None"}"""
      val result = wsUrl(s"/v1/kafka/cluster/$cluster/topic-key-mapping")
        .post(Json.parse(req)).futureValue
      println(s"${result.status}: ${result.body}")
      Status(result.status) mustBe Ok
    }

    "return BadRequest for duplicate key mapping request" in {
      val conf = app.injector.instanceOf[Configuration]
      val url = s"${conf.get[String](cluster.toLowerCase + ".kafka.avro.registry.location")}/subjects/${mapping.schema.name}/versions/${mapping.schema.version}"
      setMockRequestResponseExpectations(url, 200, Json.toJson(SchemaResponse(schema.name, schema.version, "")))

      val req = s"""{"topic": "${topic1.name}", "keyType": "None"}"""
      val result = wsUrl(s"/v1/kafka/cluster/$cluster/topic-key-mapping")
        .post(Json.parse(req)).futureValue
      println(s"${result.status}: ${result.body}")
      Status(result.status) mustBe BadRequest
    }

    "return Ok for key type string mapping" in {
      createTopic(topic2)
      val conf = app.injector.instanceOf[Configuration]
      val url = s"${conf.get[String](cluster.toLowerCase + ".kafka.avro.registry.location")}/subjects/${mapping.schema.name}/versions/${mapping.schema.version}"
      setMockRequestResponseExpectations(url, 200, Json.toJson(SchemaResponse(schema.name, schema.version, "")))

      val req = s"""{"topic": "${topic2.name}", "keyType": "string"}"""
      val result = wsUrl(s"/v1/kafka/cluster/$cluster/topic-key-mapping")
        .post(Json.parse(req)).futureValue
      println(s"${result.status}: ${result.body}")
      Status(result.status) mustBe Ok
    }

    "return Ok for key type avro mapping" in {
      createTopic(topic3)
      val conf = app.injector.instanceOf[Configuration]
      val url = s"${conf.get[String](cluster.toLowerCase + ".kafka.avro.registry.location")}/subjects/${mapping.schema.name}/versions/${mapping.schema.version}"
      setMockRequestResponseExpectations(url, 200, Json.toJson(SchemaResponse(schema.name, schema.version, "")))

      val req = s"""{"topic": "${topic3.name}", "keyType": "AVRO", "schema": {"name": "${schema.name}", "version": ${schema.version}}}"""
      val result = wsUrl(s"/v1/kafka/cluster/$cluster/topic-key-mapping")
        .post(Json.parse(req)).futureValue
      println(s"${result.status}: ${result.body}")
      Status(result.status) mustBe Ok
    }

    "return Bad Request for key type avro mapping without specifying schema" in {
      createTopic(topic3)
      val conf = app.injector.instanceOf[Configuration]
      val url = s"${conf.get[String](cluster.toLowerCase + ".kafka.avro.registry.location")}/subjects/${mapping.schema.name}/versions/${mapping.schema.version}"
      setMockRequestResponseExpectations(url, 200, Json.toJson(SchemaResponse(schema.name, schema.version, "")))

      val req = s"""{"topic": "${topic3.name}", "keyType": "AVRO"}"""
      val result = wsUrl(s"/v1/kafka/cluster/$cluster/topic-key-mapping")
        .post(Json.parse(req)).futureValue
      println(s"${result.status}: ${result.body}")
      Status(result.status) mustBe BadRequest
    }
  }
}
