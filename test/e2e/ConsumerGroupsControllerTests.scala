package e2e

import anorm._
import models.Models.{ ConsumerGroupOffset, Topic, TopicConfiguration }
import models.http.HttpModels.{ ConsumerGroupSeekRequest, TopicRequest }
import net.manub.embeddedkafka.EmbeddedKafka
import org.scalatest.BeforeAndAfterEach
import org.scalatest.mockito.MockitoSugar
import play.api.libs.json.Json
import play.api.mvc.Results._

class ConsumerGroupsControllerTests extends IntTestSpec with BeforeAndAfterEach with MockitoSugar with EmbeddedKafka {
  val cluster = "test"
  val topicName = "test-topic"
  var consumerGroupName: Option[String] = None
  val ledgerConfigSet = TopicConfiguration("ledger", Some("delete"), Some(1), Some(2629740000L), Some(1))
  val topic = Topic(topicName, TopicConfiguration(ledgerConfigSet.name, None, None, None, None))
  val message1 = "message1"
  val message2 = "message2"

  override def beforeAll() = {
    super.beforeAll()
    db.withTransaction { implicit conn =>
      SQL"""
           insert into topic_config (name, cluster, description, cleanup_policy, partitions, retention_ms, replicas, created_timestamp)
           values ('ledger', $cluster,
           'A non-compacted audit-log style topic for tracking changes in one value type. Only one value schema mapping will be allowed.',
           ${ledgerConfigSet.cleanupPolicy}, ${ledgerConfigSet.partitions}, ${ledgerConfigSet.retentionMs} , ${ledgerConfigSet.replicas}, now());
        """.execute()
    }
    EmbeddedKafka.start()
    //publish messages to a topic and create a consumer group
    createTopic(topic)
    publishStringMessageToKafka(topic.name, message1)
    publishStringMessageToKafka(topic.name, message2)
    consumeFirstStringMessageFrom(topic.name)
  }

  override def afterAll(): Unit = {
    EmbeddedKafka.stop()
    db.withTransaction { implicit conn =>
      SQL"""
           delete from TOPIC_CONFIG;
           delete from TOPIC;
        """.execute()
    }
    super.afterAll()
  }

  private def createTopic(topic: Topic) = {
    val futureResult = wsUrl(s"/v1/kafka/cluster/$cluster/topic")
      .post(Json.toJson(TopicRequest(topic)))
    val result = futureResult.futureValue
    Status(result.status) mustBe Ok
  }

  "ConsumerGroupsController #list" must {
    "return list of consumer group names" in {
      val futureResult = wsUrl(s"/v1/kafka/cluster/$cluster/consumer-groups").get()
      val result = futureResult.futureValue
      Status(result.status) mustBe Ok
      val consumerGroupNames = result.json.as[List[String]]
      consumerGroupNames.size mustBe 1
      consumerGroupName = consumerGroupNames.headOption
      println(s"consumer group name : ${consumerGroupName}")
    }
  }

  "ConsumerGroupsController #offsets" must {
    "return Ok" in {
      val futureResult = wsUrl(s"/v1/kafka/cluster/$cluster/consumer-groups/${consumerGroupName.get}/offsets").get()
      val result = futureResult.futureValue
      println(s"${result.status}: ${result.body}")
      Status(result.status) mustBe Ok
      val offsets = result.json.as[Seq[ConsumerGroupOffset]]
      offsets.size mustBe 1
      offsets.head.lag mustBe 1
    }
  }

  "ConsumerGroupsController #members" must {
    "return Ok" in {
      val futureResult = wsUrl(s"/v1/kafka/cluster/$cluster/consumer-groups/${consumerGroupName.get}/members").get()
      val result = futureResult.futureValue
      println(s"${result.status}: ${result.body}")
    }
  }

  "ConsumerGroupsController #seek" must {
    "return Ok" in {
      val futureResult = wsUrl(s"/v1/kafka/cluster/$cluster/consumer-groups/${consumerGroupName.get}/seek").post(
        Json.toJson(ConsumerGroupSeekRequest(topic.name, Some(List(0)), "beginning", None))
      )
      val result = futureResult.futureValue
      println(s"${result.status}: ${result.body}")

      val result2 = wsUrl(s"/v1/kafka/cluster/$cluster/consumer-groups/${consumerGroupName.get}/offsets").get().futureValue
      val offsets = result2.json.as[Seq[ConsumerGroupOffset]]
      offsets.size mustBe 1
      offsets.head.currentOffset mustBe 0
      offsets.head.logEndOffset mustBe 2
      offsets.head.lag mustBe 2
      consumeFirstStringMessageFrom(topic.name) mustBe message1
    }
  }
}
