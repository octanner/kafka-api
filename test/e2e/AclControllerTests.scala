package e2e

import java.util.{Properties, UUID}

import anorm.SqlParser.scalar
import anorm._
import daos.{AclDao, TopicDao}
import models.AclRole
import models.Models.{Acl, Topic, TopicConfiguration}
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
import services.AclService
import utils.AdminClientUtil
import utils.AdminClientUtil.ADMIN_CLIENT_ID

import scala.collection.JavaConverters._
import scala.reflect.io.Directory
import scala.util.Try

class AclControllerTests extends IntTestSpec with BeforeAndAfterEach with EmbeddedKafka {
  val dao = new AclDao()
  val topicDao = new TopicDao()
  val cluster = "test"
  val username = "testusername"
  val username2 = "testusername1"
  val password = "testpassword"
  val password2 = "testpassword1"
  val topic = Topic("test-some-topic", TopicConfiguration("state", Some("compact"), Some(1), Some(-1), Some(1)))
  val topic2 = Topic("test.some.topic.2", TopicConfiguration("state", Some("compact"), Some(1), Some(-1), Some(1)))
  val topic3 = Topic("test.some.topic.3", TopicConfiguration("state", Some("compact"), Some(1), Some(-1), Some(1)))
  val topic4 = Topic("test.some.topic.4", TopicConfiguration("state", Some("compact"), Some(1), Some(-1), Some(1)))
  val topicId = UUID.randomUUID.toString
  val topicId2 = UUID.randomUUID.toString
  val topicId3 = UUID.randomUUID.toString
  val topicId4 = UUID.randomUUID.toString
  var conf: Configuration = _

  override def modulesToOverride: Seq[GuiceableModule] = Seq(
    bind[Database].toInstance(db),
    bind[AclDao].toInstance(dao),
    bind[TopicDao].toInstance(topicDao),
  )

  override def beforeAll() = {
    super.beforeAll()

    db.withTransaction { implicit conn =>
      SQL"""
           insert into topic_config(name, cluster, description, cleanup_policy, partitions, retention_ms, replicas, created_timestamp)
           values ('state', $cluster,
           'A compacted topic with infinite retention, for keeping state of one type. Topic Key Type cannot be NONE. Only one value schema mapping will be allowed.',
           'compact', 3, -1, 3, now());
            insert into topic (topic_id, cluster, topic, partitions, replicas, retention_ms, cleanup_policy) values
            ($topicId, $cluster, ${topic.name}, ${topic.config.partitions}, ${topic.config.replicas}, ${topic.config.retentionMs}, ${topic.config.cleanupPolicy});
            insert into topic (topic_id, cluster, topic, partitions, replicas, retention_ms, cleanup_policy) values
            ($topicId2, $cluster, ${topic2.name}, ${topic.config.partitions}, ${topic.config.replicas}, ${topic.config.retentionMs}, ${topic.config.cleanupPolicy});
            insert into topic (topic_id, cluster, topic, partitions, replicas, retention_ms, cleanup_policy) values
            ($topicId3, $cluster, ${topic3.name}, ${topic.config.partitions}, ${topic.config.replicas}, ${topic.config.retentionMs}, ${topic.config.cleanupPolicy});
            insert into topic (topic_id, cluster, topic, partitions, replicas, retention_ms, cleanup_policy) values
            ($topicId4, $cluster, ${topic4.name}, ${topic.config.partitions}, ${topic.config.replicas}, ${topic.config.retentionMs}, ${topic.config.cleanupPolicy});

            insert into topic_key_mapping (cluster, topic_id, key_type) values
            ($cluster, ${topicId}, 'NONE');
            insert into topic_schema_mapping (cluster, topic_id, schema) values
            ($cluster, ${topicId}, 'testschema');

            insert into topic_key_mapping (cluster, topic_id, key_type) values
            ($cluster, ${topicId2}, 'NONE');

            insert into topic_schema_mapping (cluster, topic_id, schema) values
            ($cluster, ${topicId3}, 'testschema');

            insert into acl_source (username, password, cluster, claimed) values ($username, $password, $cluster, false);
            insert into acl_source (username, password, cluster, claimed) values ($username2, $password2, $cluster, false);
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
    val kafkaHostName = conf.get[String](cluster.toLowerCase + AdminClientUtil.KAFKA_LOCATION_CONFIG)
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHostName)
    props.put(AdminClientConfig.CLIENT_ID_CONFIG, ADMIN_CLIENT_ID)

    val adminClient = AdminClient.create(props)
    Try(adminClient.deleteAcls(List(AclBindingFilter.ANY).asJava))
    adminClient.close()

    db.withConnection { implicit conn =>
      SQL"DELETE FROM acl;".executeUpdate()
    }
  }

  override def afterAll(): Unit = {
    db.withTransaction { implicit conn =>
      SQL"""
            DELETE FROM acl_source;
            DELETE FROM acl;
            DELETE FROM topic_config;
            DELETE FROM topic_schema_mapping;
            DELETE FROM topic_key_mapping;
            DELETE FROM topic;
         """.executeUpdate()
    }
    EmbeddedKafka.stop()
    super.afterAll()
  }

  private def aclExistsInKafka(user: String, operation: AclOperation) = {
    val kafkaHostName = conf.get[String](cluster.toLowerCase + AdminClientUtil.KAFKA_LOCATION_CONFIG)
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHostName)
    props.put(AdminClientConfig.CLIENT_ID_CONFIG, ADMIN_CLIENT_ID)

    val adminClient = AdminClient.create(props)
    val accessControlEntryFilter = new AccessControlEntryFilter(s"User:$user", "*", operation, AclPermissionType.ALLOW)
    val aclBindingFilter = new AclBindingFilter(ResourcePatternFilter.ANY, accessControlEntryFilter)
    val acls = Try(adminClient.describeAcls(aclBindingFilter).values.get)
    adminClient.close()
    acls.get
  }

  private def entriesWithRoleInDb(role: String): Int = {
    db.withConnection { implicit conn =>
      SQL"SELECT count(*) FROM acl WHERE role = $role".as(scalar[Int].single)
    }
  }

  private def getAclId(aclRequest: AclRequest) = {
    db.withConnection { implicit conn =>
      val userId = dao.getUserIdByName(cluster, aclRequest.user)
      val topicId = dao.getTopicIdByName(cluster, aclRequest.topic)
      val role = aclRequest.role.role
      val cgname = aclRequest.consumerGroupName.getOrElse("*")
      SQL"SELECT acl_id FROM acl WHERE cluster = $cluster AND topic_id = $topicId AND role = $role AND user_id = $userId AND cg_name = $cgname".as(dao.stringParser.single)
    }
  }

  private def getUserClaimedStatus(cluster: String, user: String) = {
    db.withConnection { implicit conn =>
      SQL"""SELECT claimed FROM acl_source WHERE cluster = $cluster AND username = $user""" .as(scalar[Boolean].single)
    }
  }

  "AclController #post" should {
    "claim a new user" in {
      val futureResult = wsUrl(s"/v1/kafka/cluster/$cluster/user").post("{}")
      val result = futureResult.futureValue
      val expectedJson = Json.obj(
        "aclCredentials" -> Json.obj(
          "username" -> username,
          "password" -> password,
          "cluster"  -> cluster
        )
      )

      Status(result.status) mustBe Ok
      result.json mustBe expectedJson
    }


    "allow user write access for topic" in {
      val role = AclRole.PRODUCER
      val aclRequest = AclRequest(topic.name, username, role, Some("*"))
      val aclRequestJson = Json.obj("topic" -> topic.name, "user" -> username, "role" -> "Producer")
      val futureResult = wsUrl(s"/v1/kafka/cluster/$cluster/acls").post(aclRequestJson)
      val result = futureResult.futureValue
      println(s"${result.status}: ${result.body}")
      val expectedJson = Json.obj("id" -> getAclId(aclRequest)).toString

      Status(result.status) mustBe Ok
      entriesWithRoleInDb(role.role) mustBe 1
      result.body mustBe expectedJson
      // Acl should be created for topic and group
      aclExistsInKafka(username, role.operation).size() mustEqual 2
    }

    "return same id for repeat permission request" in {
      val role = AclRole.PRODUCER
      val roleName = role.role
      val aclRequest = AclRequest(topic.name, username, role, None)
      val aclRequestJson = Json.obj("topic" -> topic.name, "user" -> username, "role" -> "Producer")
      val aclId = db.withConnection { implicit conn => dao.addPermissionToDb(cluster, aclRequest) }

      entriesWithRoleInDb(roleName) mustBe 1

      val futureResult = wsUrl(s"/v1/kafka/cluster/$cluster/acls").post(aclRequestJson)
      val result = futureResult.futureValue
      val expectedJson = Json.obj("id" -> aclId).toString

      Status(result.status) mustBe Ok
      entriesWithRoleInDb(roleName) mustBe 1
      result.body mustBe expectedJson
      // Acl should be created for topic and group
      aclExistsInKafka(username, role.operation).size() mustEqual 2
    }

    "allow user read access for topic" in {
      val role = AclRole.CONSUMER
      val cgName = s"$username-cg1"
      val aclRequest = AclRequest(topic.name, username, role, Some(cgName))
      val aclRequestJson = Json.obj("topic" -> topic.name, "user" -> username, "role" -> "consumer", "consumerGroupName" -> cgName)

      val futureResult = wsUrl(s"/v1/kafka/cluster/$cluster/acls").post(aclRequestJson)
      val result = futureResult.futureValue
      val expectedJson = Json.obj("id" -> getAclId(aclRequest), "consumerGroupName" -> cgName).toString

      Status(result.status) mustBe Ok
      entriesWithRoleInDb(role.role) mustBe 1
      result.body mustBe expectedJson
      // Acl should be created for topic and group
      println(s"-------${aclExistsInKafka(username, role.operation)}")
      aclExistsInKafka(username, role.operation).size() mustEqual 2
    }

    "same ID and consumer group name for repeat request with same cg name " in {
      val role = AclRole.CONSUMER
      val cgName = s"$username-cg1"
      val aclRequest = AclRequest(topic.name, username, role, Some(cgName))
      val aclRequestJson = Json.obj("topic" -> topic.name, "user" -> username, "role" -> "consumer", "consumerGroupName" -> cgName)

      val futureResult = wsUrl(s"/v1/kafka/cluster/$cluster/acls").post(aclRequestJson)
      val result = futureResult.futureValue
      val expectedJson = Json.obj("id" -> getAclId(aclRequest), "consumerGroupName" -> cgName).toString

      Status(result.status) mustBe Ok
      entriesWithRoleInDb(role.role) mustBe 1
      result.body mustBe expectedJson
      // Acl should be created for topic and group
      println(s"-------${aclExistsInKafka(username, role.operation)}")
      aclExistsInKafka(username, role.operation).size() mustEqual 2
    }

    "allow user second read access for topic with different cg name" in {
      val role = AclRole.CONSUMER
      val cgName = s"$username-cg2"
      val aclRequest = AclRequest(topic.name, username, role, Some(cgName))
      val aclRequestJson = Json.obj("topic" -> topic.name, "user" -> username, "role" -> "consumer", "consumerGroupName" -> cgName)

      val futureResult = wsUrl(s"/v1/kafka/cluster/$cluster/acls").post(aclRequestJson)
      val result = futureResult.futureValue
      val expectedJson = Json.obj("id" -> getAclId(aclRequest), "consumerGroupName" -> cgName).toString

      Status(result.status) mustBe Ok
      entriesWithRoleInDb(role.role) mustBe 1
      result.body mustBe expectedJson
      // Acl should be created for topic and group
      aclExistsInKafka(username, role.operation).size() mustEqual 2
    }

    "fail to grant permissions for unknown role" in {
      val role = "UNKNOWN"
      val aclRequest = Json.obj(
        "topic" -> topic.name,
        "user" -> username,
        "role" -> role
      )
      val futureResult = wsUrl(s"/v1/kafka/cluster/$cluster/acls").post(aclRequest)
      val result = futureResult.futureValue

      println(s"Result body: ${result.body}")
      Status(result.status) mustBe BadRequest
      entriesWithRoleInDb(role) mustBe 0
      aclExistsInKafka(username, AclOperation.ANY).size() mustEqual 0
    }

    "fail to grant permissions for unknown username" in {
      val role = AclRole.CONSUMER
      val username = "badUsername"
      val aclRequest = AclRequest(topic.name, username, role, None)
      val futureResult = wsUrl(s"/v1/kafka/cluster/$cluster/acls").post(Json.toJson(aclRequest))
      val result = futureResult.futureValue

      println(s"Result body: ${result.body}")
      Status(result.status) mustBe BadRequest
      entriesWithRoleInDb(role.role) mustBe 0
      aclExistsInKafka(username, AclOperation.ANY).size() mustEqual 0
    }

    "fail to grant permissions for unknown topic" in {
      val role = AclRole.CONSUMER
      val topicName = "badTopicName"
      val aclRequest = AclRequest(topicName, username, role, None)
      val futureResult = wsUrl(s"/v1/kafka/cluster/$cluster/acls").post(Json.toJson(aclRequest))
      val result = futureResult.futureValue

      println(s"Result body: ${result.body}")
      Status(result.status) mustBe BadRequest
      entriesWithRoleInDb(role.role) mustBe 0
      aclExistsInKafka(username, AclOperation.ANY).size() mustEqual 0
    }

  }

  "AclController #getCredentials" must {
    "return Ok with credentials when the user is claimed" in {
      val cgName = s"$username-cg1"
      val cgName2 = s"$username-cg2"
      val aclId1 = db.withConnection { implicit conn =>
        dao.addPermissionToDb(cluster, AclRequest(topic.name, username, AclRole.CONSUMER, Some(cgName)))
      }
      val aclId2 = db.withConnection { implicit conn =>
        dao.addPermissionToDb(cluster, AclRequest(topic2.name, username, AclRole.PRODUCER, None))
      }
      val aclId3 = db.withConnection { implicit conn =>
        dao.addPermissionToDb(cluster, AclRequest(topic.name, username, AclRole.CONSUMER, Some(cgName2)))
      }

      val expectedMap = Map[String, String](
        ("KAFKA_PORT" -> conf.get[String]("test.kafka.port")),
        ("KAFKA_LOCATION" -> conf.get[String]("test.kafka.location")),
        ("KAFKA_HOSTNAME" -> conf.get[String]("test.kafka.hostname")),
        ("KAFKA_AVRO_REGISTRY_LOCATION" -> conf.get[String]("test.kafka.avro.registry.location")),
        ("KAFKA_AVRO_REGISTRY_PORT" -> conf.get[String]("test.kafka.avro.registry.port")),
        ("KAFKA_AVRO_REGISTRY_HOSTNAME" -> conf.get[String]("test.kafka.avro.registry.hostname")),
        ("KAFKA_USERNAME" -> username),
        ("KAFKA_PASSWORD" -> password),
        ("KAFKA_CONSUMER_TOPICS" -> topic.name),
        ("KAFKA_PRODUCER_TOPICS" -> topic2.name),
        (s"${AclService.getTopicConfigPrefix(topic.name)}_TOPIC_CONSUMER_GROUPS" -> s"$cgName,$cgName2"),
        (s"${AclService.getTopicConfigPrefix(topic.name)}_TOPIC_NAME" -> topic.name),
        (s"${AclService.getTopicConfigPrefix(topic.name)}_TOPIC_KEY_TYPE" -> "NONE"),
        (s"${AclService.getTopicConfigPrefix(topic.name)}_TOPIC_SCHEMAS" -> "testschema"),
        (s"${AclService.getTopicConfigPrefix(topic2.name)}_TOPIC_NAME" -> topic2.name),
        (s"${AclService.getTopicConfigPrefix(topic2.name)}_TOPIC_KEY_TYPE" -> "NONE"),
        (s"${AclService.getTopicConfigPrefix(topic2.name)}_TOPIC_SCHEMAS" -> "")
      )

      val result = wsUrl(s"/v1/kafka/credentials/$username").get().futureValue
      println(s"${result.status}; Result body: ${result.body}")
      Status(result.status) mustBe Ok
      result.json.as[Map[String, String]] mustBe expectedMap
    }

    "return Bad Request when the user is unclaimed" in {
      val result = wsUrl(s"/v1/kafka/credentials/$username2").get().futureValue
      println(s"${result.status}; Result body: ${result.body}")
      Status(result.status) mustBe BadRequest
    }

    "return Bad Request when the user does not exist" in {
      val result = wsUrl(s"/v1/kafka/credentials/nonexistinguser").get().futureValue
      println(s"${result.status}; Result body: ${result.body}")
      Status(result.status) mustBe BadRequest
    }
  }

  "AclController #getAclsForTopic" must {
    "return list of acls for a topic" in {
      val cg = s"$username-cg1"
      val aclId1 = db.withConnection { implicit conn =>
        dao.addPermissionToDb(cluster, AclRequest(topic.name, username, AclRole.CONSUMER, Some(cg)))
      }
      val aclId2 = db.withConnection { implicit conn =>
        dao.addPermissionToDb(cluster, AclRequest(topic.name, username, AclRole.PRODUCER, None))
      }

      val result = wsUrl(s"/v1/kafka/cluster/$cluster/acls?topic=${topic.name}").get().futureValue
      val expectedJson = Json.obj("acls" -> Json.toJson(List(
        Acl(aclId1, username, topic.name, cluster, AclRole.CONSUMER, Some(cg)),
        Acl(aclId2, username, topic.name, cluster, AclRole.PRODUCER, None)
      )))

      println(s"${result.status}; Result body: ${result.body}")
      Status(result.status) mustBe Ok
      result.json mustBe expectedJson
    }
  }

  "AclController #delete" must {
    "return Ok and delete producer acl in kafka and database" in {
      val role = AclRole.PRODUCER
      val aclRequest = AclRequest(topic.name, username, role, None)

      // Create Acl
      val create = wsUrl(s"/v1/kafka/cluster/$cluster/acls").post(Json.toJson(aclRequest)).futureValue
      val aclId = (create.json \ "id").as[String]

      val result = wsUrl(s"/v1/kafka/acls/$aclId").delete().futureValue
      Status(result.status) mustBe Ok
      aclExistsInKafka(username, role.operation).size() mustEqual 0
      db.withConnection{ implicit conn => dao.getAcl(aclId) } mustBe None
    }

    "return Ok and delete consumer acl in kafka and database" in {
      val role = AclRole.CONSUMER
      val aclRequest = AclRequest(topic.name, username, role, None)
      // Create Acl
      val create = wsUrl(s"/v1/kafka/cluster/$cluster/acls").post(Json.toJson(aclRequest)).futureValue
      val aclId = (create.json \ "id").as[String]

      val result = wsUrl(s"/v1/kafka/acls/$aclId").delete().futureValue
      Status(result.status) mustBe Ok
      aclExistsInKafka(username, role.operation).size() mustEqual 0
      db.withConnection{ implicit conn => dao.getAcl(aclId) } mustBe None
    }

    "return NotFound when a non existing acl id request is sent" in {
      val result = wsUrl(s"/v1/kafka/acls/nonexistingid").delete().futureValue
      Status(result.status) mustBe NotFound
      println(result.body)
    }
  }

  "AclController #deleteUser" must {
    "Delete all ACls for user and unclaim the user" in {
      println(s"Count of ACLs for user ${username} before delete ${db.withConnection { implicit conn => dao.getAclsForUsername(cluster, username)}}")
      val result = wsUrl(s"/v1/kafka/user/$username").delete().futureValue
      Status(result.status) mustBe Ok
      println(s"Count of ACLs for user ${username} before delete ${db.withConnection { implicit conn => dao.getAclsForUsername(cluster, username)}}")
      getUserClaimedStatus(cluster, username) mustBe false
    }
  }
}
