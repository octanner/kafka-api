package e2e

import java.util.Properties

import anorm._
import daos.TopicDao
import models.Models.{ Topic, TopicConfiguration }
import models.http.HttpModels.{ TopicRequest, TopicResponse }
import net.manub.embeddedkafka.EmbeddedKafka
import org.apache.kafka.clients.admin.{ AdminClient, AdminClientConfig }
import org.scalatest.BeforeAndAfterEach
import play.api.db.Database
import play.api.mvc.Results._
import play.api.inject.bind
import play.api.inject.guice.GuiceableModule
import play.api.libs.json.Json
import play.api.{ Configuration, Logger }
import services.TopicService
import services.TopicService.ADMIN_CLIENT_ID

class TopicControllerTests extends IntTestSpec with BeforeAndAfterEach with EmbeddedKafka {
  val dao = new TopicDao()
  val cluster = "test"
  val topic = Topic("test.some.topic", "Test topic creation", "testOrg", TopicConfiguration(Some("delete"), Some(1), Some(888888), Some(1)))
  var conf: Configuration = _

  override def modulesToOverride: Seq[GuiceableModule] = Seq(
    bind[Database].toInstance(db),
    bind[TopicDao].toInstance(dao)
  )

  override def beforeAll() = {
    super.beforeAll()
    EmbeddedKafka.start()
    conf = app.injector.instanceOf[Configuration]
  }

  override def afterAll(): Unit = {
    db.withTransaction { implicit conn =>
      SQL"""delete from TOPIC;
         """.execute()
    }
    EmbeddedKafka.stop()
    super.afterAll()
  }

  private def getTopicFromDB(cluster: String, topicName: String): Option[Topic] = {
    db.withTransaction { implicit conn =>
      dao.getTopicInfo(cluster, topicName)
    }
  }

  private def topicExistsInKafka(topicName: String): Boolean = {
    val kafkaHostName = conf.get[String](cluster.toLowerCase + TopicService.KAFKA_LOCATION_CONFIG)
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHostName)
    props.put(AdminClientConfig.CLIENT_ID_CONFIG, ADMIN_CLIENT_ID)

    val adminClient = AdminClient.create(props)
    val allTopics = adminClient.listTopics().names().get()
    allTopics.contains(topicName)
  }

  "Topic Controller #create" must {
    "create a topic in kafka cluster and database" in {
      val futureResult = wsUrl(s"/v1/kafka/cluster/$cluster/topic")
        .post(Json.toJson(TopicRequest(topic)))
      val result = futureResult.futureValue
      Logger.info(s"result status: ${result.status}, result body: ${result.body}")
      Logger.info(s"Kafka has new topic? ${topicExistsInKafka(topic.name)}")
      Logger.info(s"Topic in DB: ${getTopicFromDB(cluster, topic.name)}")
      Status(result.status) mustBe Ok
      result.json mustBe Json.toJson(TopicResponse(topic))
      topicExistsInKafka(topic.name) mustBe true
      getTopicFromDB(cluster, topic.name) mustBe Some(topic)
    }
  }
}
