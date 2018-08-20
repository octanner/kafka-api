package daos

import java.sql.Connection

import anorm._
import models.Models.{ Topic, TopicConfiguration }
import org.joda.time.DateTime

class TopicDao {
  def insert(cluster: String, topic: Topic, partitions: Int, replicas: Int, retentionMs: Long, cleanupPolicy: String)(implicit conn: Connection) = {
    SQL"""
        insert into TOPIC (topic, partitions, replicas, retention_ms, cleanup_policy, created_timestamp,
        cluster, organization, description) values
        (${topic.name}, ${partitions}, ${replicas}, ${retentionMs}, ${cleanupPolicy},
        ${DateTime.now().toDate}, ${cluster}, ${topic.organization}, ${topic.description})
      """
      .execute()
  }

  def getTopicInfo(topicName: String)(implicit conn: Connection): Option[Topic] = {
    SQL"SELECT #$topicColumns FROM topic WHERE topic = $topicName".as(topicParser.singleOpt)
  }

  def getAllTopics()(implicit conn: Connection): Seq[Topic] = {
    SQL"SELECT #$topicColumns FROM topic".as(topicParser.*)
  }

  val topicConfigColumns = "cleanup_policy, partitions, retention_ms, replicas"
  val topicColumns = s"topic, description, organization, $topicConfigColumns"
  implicit val topicConfigParser = Macro.parser[TopicConfiguration]("cleanup_policy", "partitions", "retention_ms", "replicas")
  implicit val topicParser = Macro.parser[Topic]("topic", "description", "organization", "cleanup_policy", "partitions", "retention_ms", "replicas")

}
