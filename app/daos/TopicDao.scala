package daos

import java.sql.Connection

import anorm._
import models.Models.{ BasicTopicInfo, Topic, TopicConfiguration }
import models.http.HttpModels.TopicSchemaMapping
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

  def getBasicTopicInfo(cluster: String, topicName: String)(implicit conn: Connection): Option[BasicTopicInfo] = {
    SQL"""
        SELECT topic_id, topic, cluster FROM topic WHERE topic = $topicName AND cluster = $cluster;
      """
      .as(basicTopicInfoParser.singleOpt)
  }

  def getAllTopics()(implicit conn: Connection): Seq[Topic] = {
    SQL"SELECT #$topicColumns FROM topic".as(topicParser.*)
  }

  def upsertTopicSchemaMapping(cluster: String, topicId: String, mapping: TopicSchemaMapping)(implicit conn: Connection) = {
    SQL"""
        INSERT INTO TOPIC_SCHEMA_MAPPING (topic_id, schema, version, cluster)
        values ($topicId, ${mapping.schema.name}, ${mapping.schema.version}, $cluster)
        ON CONFLICT DO NOTHING;
      """
      .execute()
  }

  val topicConfigColumns = "cleanup_policy, partitions, retention_ms, replicas"
  val topicColumns = s"topic, description, organization, $topicConfigColumns"
  implicit val topicConfigParser = Macro.parser[TopicConfiguration]("cleanup_policy", "partitions", "retention_ms", "replicas")
  implicit val topicParser = Macro.parser[Topic]("topic", "description", "organization", "cleanup_policy", "partitions", "retention_ms", "replicas")
  implicit val basicTopicInfoParser = Macro.parser[BasicTopicInfo]("topic_id", "topic", "cluster")
}
