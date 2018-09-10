package daos

import java.sql.Connection

import anorm._
import models.Models.{ BasicTopicInfo, Topic, TopicConfiguration, TopicKeyMapping }
import models.http.HttpModels.{ SchemaRequest, TopicSchemaMapping }
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

  def getTopicSchemaMappings(cluster: String, topic: String)(implicit conn: Connection): List[TopicSchemaMapping] = {
    SQL"""
        SELECT topic, schema, version
        FROM TOPIC_SCHEMA_MAPPING tsm INNER JOIN TOPIC t ON tsm.topic_id = t.topic_id
        WHERE t.topic = $topic AND tsm.cluster = $cluster
      """
      .as(topicSchemaMappingParser.*)
  }

  def insertTopicKeyMapping(cluster: String, topicKeyMapping: TopicKeyMapping)(implicit conn: Connection) = {
    SQL"""
        INSERT INTO TOPIC_KEY_MAPPING (TOPIC_ID, KEY_TYPE, SCHEMA, VERSION, CLUSTER) VALUES
        (${topicKeyMapping.topicId}, ${topicKeyMapping.keyType.toString}, ${topicKeyMapping.schema},
        ${topicKeyMapping.version}, ${cluster})
      """
      .execute()
  }

  val topicConfigColumns = "cleanup_policy, partitions, retention_ms, replicas"
  val topicColumns = s"topic, description, organization, $topicConfigColumns"
  implicit val topicConfigParser = Macro.parser[TopicConfiguration]("cleanup_policy", "partitions", "retention_ms", "replicas")
  implicit val topicParser = Macro.parser[Topic]("topic", "description", "organization", "cleanup_policy", "partitions", "retention_ms", "replicas")
  implicit val schemaParser = Macro.parser[SchemaRequest]("schema", "version")
  implicit val topicSchemaMappingParser = Macro.parser[TopicSchemaMapping]("topic", "schema", "version")
  implicit val basicTopicInfoParser = Macro.parser[BasicTopicInfo]("topic_id", "topic", "cluster")
}
