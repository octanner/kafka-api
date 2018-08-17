package e2e

import java.util.Properties

import anorm._
import daos.AclDao
import models.Models.{Topic, TopicConfiguration}
import models.http.HttpModels.AclRequest
import net.manub.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.apache.kafka.common.acl._
import org.apache.kafka.common.resource.ResourcePatternFilter
import org.scalatest.BeforeAndAfterEach
import play.api.Configuration
import play.api.db.Database
import play.api.inject.bind
import play.api.inject.guice.GuiceableModule
import play.api.libs.json.Json
import play.api.mvc.Results._
import services.TopicService
import services.TopicService.ADMIN_CLIENT_ID

import scala.collection.JavaConverters._
import scala.reflect.io.Directory
import scala.util.Try

class AclControllerTests extends IntTestSpec with BeforeAndAfterEach with EmbeddedKafka {
  val dao = new AclDao()
  val cluster = "test"
  val username = "testusername"
  val password = "testpassword"
  val topic = Topic("test.some.topic", "Test topic creation", "testOrg", TopicConfiguration(Some("delete"), Some(1), Some(888888), Some(1)))
  var conf: Configuration = _

  override def modulesToOverride: Seq[GuiceableModule] = Seq(
    bind[Database].toInstance(db),
    bind[AclDao].toInstance(dao),
  )

  override def beforeAll() = {
    super.beforeAll()

    db.withTransaction { implicit conn =>
      SQL"""
            insert into topic (cluster, topic, partitions, replicas, retention_ms, cleanup_policy) values
            ($cluster, ${topic.name}, 1, 1, 888888, 'delete');

            insert into acl_source (username, password, cluster, claimed) values ($username, $password, $cluster, false);
         """.executeUpdate()
    }

    EmbeddedKafka.startZooKeeper(Directory.makeTemp("zookeeper-logs"))
    val brokerProperties = Map(
      "authorizer.class.name" -> "kafka.security.auth.SimpleAclAuthorizer",
      "allow.everyone.if.no.acl.found" -> "true"
    )
    val embeddedKafkaConfig: EmbeddedKafkaConfig = EmbeddedKafkaConfig.apply(customBrokerProperties = brokerProperties)
    EmbeddedKafka.startKafka(Directory.makeTemp("kafka-logs"))(embeddedKafkaConfig)
    conf = app.injector.instanceOf[Configuration]
  }

  override def afterEach(): Unit = {
    val kafkaHostName = conf.get[String](cluster.toLowerCase + TopicService.KAFKA_LOCATION_CONFIG)
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHostName)
    props.put(AdminClientConfig.CLIENT_ID_CONFIG, ADMIN_CLIENT_ID)

    val adminClient = AdminClient.create(props)
    Try(adminClient.deleteAcls(List(AclBindingFilter.ANY).asJava))
    adminClient.close()

    db.withConnection { implicit conn =>
      SQL"delete from permissions;".executeUpdate()
    }
  }

  override def afterAll(): Unit = {
    db.withTransaction { implicit conn =>
      SQL"""
            delete from acl_source;
            delete from permissions;
            delete from topic;
         """.executeUpdate()
    }
    EmbeddedKafka.stop()
    super.afterAll()
  }

  private def aclExistsInKafka(user: String, topicName: String, operation: AclOperation) = {
    val kafkaHostName = conf.get[String](cluster.toLowerCase + TopicService.KAFKA_LOCATION_CONFIG)
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHostName)
    props.put(AdminClientConfig.CLIENT_ID_CONFIG, ADMIN_CLIENT_ID)

    val adminClient = AdminClient.create(props)
    val accessControlEntryFilter = new AccessControlEntryFilter(s"User:$user", topicName, operation, AclPermissionType.ALLOW)
    val aclBindingFilter = new AclBindingFilter(ResourcePatternFilter.ANY, accessControlEntryFilter)
    val acls = Try(adminClient.describeAcls(aclBindingFilter).values.get)
    adminClient.close()
    acls.get
  }

  private def entriesWithRoleInDb(role: String): Int = {
    db.withConnection { implicit conn =>
      SQL"SELECT count(*) FROM permissions WHERE role = $role".as(SqlParser.scalar[Int].single)
    }
  }

  "AclController #post" should {
    "claim a new user" in {
      val futureResult = wsUrl(s"/v1/kafka/cluster/$cluster/user").post("{}")
      val result = futureResult.futureValue
      val expectedJson = Json.obj(
        "aclCredentials" -> Json.obj(
          "username" -> username,
          "password" -> password
        )
      )

      Status(result.status) mustBe Ok
      result.json mustBe expectedJson
    }

    "allow user write access for topic" in {
      val role = "PRODUCER"
      val aclRequest = AclRequest(topic.name, username, role)
      val futureResult = wsUrl(s"/v1/kafka/cluster/$cluster/acls").post(Json.toJson(aclRequest))
      val result = futureResult.futureValue

      Status(result.status) mustBe NoContent
      entriesWithRoleInDb(role) mustBe 1
      aclExistsInKafka(username, topic.name, AclOperation.WRITE).size() mustEqual 1
    }

    "allow user read access for topic" in {
      val role = "CONSUMER"
      val aclRequest = AclRequest(topic.name, username, role)
      val futureResult = wsUrl(s"/v1/kafka/cluster/$cluster/acls").post(Json.toJson(aclRequest))
      val result = futureResult.futureValue

      Status(result.status) mustBe NoContent
      entriesWithRoleInDb(role) mustBe 1
      aclExistsInKafka(username, topic.name, AclOperation.READ).size() mustEqual 1
    }

    "fail to grant permissions for unknown role" in {
      val role = "UNKNOWN"
      val aclRequest = AclRequest(topic.name, username, role)
      val futureResult = wsUrl(s"/v1/kafka/cluster/$cluster/acls").post(Json.toJson(aclRequest))
      val result = futureResult.futureValue

      println(s"Result body: ${result.body}")
      Status(result.status) mustBe BadRequest
      entriesWithRoleInDb(role) mustBe 0
      aclExistsInKafka(username, topic.name, AclOperation.ANY).size() mustEqual 0
    }

    "fail to grant permissions for unknown username" in {
      val role = "CONSUMER"
      val username = "badUsername"
      val aclRequest = AclRequest(topic.name, username, role)
      val futureResult = wsUrl(s"/v1/kafka/cluster/$cluster/acls").post(Json.toJson(aclRequest))
      val result = futureResult.futureValue

      println(s"Result body: ${result.body}")
      Status(result.status) mustBe BadRequest
      entriesWithRoleInDb(role) mustBe 0
      aclExistsInKafka(username, topic.name, AclOperation.ANY).size() mustEqual 0
    }

    "fail to grant permissions for unknown topic" in {
      val role = "CONSUMER"
      val topicName = "badTopicName"
      val aclRequest = AclRequest(topicName, username, role)
      val futureResult = wsUrl(s"/v1/kafka/cluster/$cluster/acls").post(Json.toJson(aclRequest))
      val result = futureResult.futureValue

      println(s"Result body: ${result.body}")
      Status(result.status) mustBe BadRequest
      entriesWithRoleInDb(role) mustBe 0
      aclExistsInKafka(username, topicName, AclOperation.ANY).size() mustEqual 0
    }

    /**  SHOULD I TEST THE VALIDITY OF A REQUEST BEFORE PASSING INTO ACLSERVICE?  **/
  }
}
