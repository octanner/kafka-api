package daos

import java.sql.Connection

import anorm.SqlParser.{ get, scalar, str }
import anorm._
import models.KeyType
import models.KeyType.KeyType
import models.Models.{ BasicTopicInfo, Topic, TopicConfiguration, TopicKeyMapping, TopicKeyType }
import models.http.HttpModels.{ SchemaRequest, TopicSchemaMapping }
import org.joda.time.DateTime
import utils.Exceptions.InvalidKeyTypeException

class TopicDao {
  def insert(cluster: String, topic: Topic, partitions: Int, replicas: Int, retentionMs: Long, cleanupPolicy: String)(implicit conn: Connection) = {
    SQL"""
        insert into TOPIC (topic, partitions, replicas, retention_ms, cleanup_policy, created_timestamp,
        cluster, config_name) values
        (${topic.name}, ${partitions}, ${replicas}, ${retentionMs}, ${cleanupPolicy},
        ${DateTime.now().toDate}, ${cluster}, ${topic.config.name})
      """
      .execute()
  }

  def getTopicInfo(topicName: String)(implicit conn: Connection): Option[Topic] = {
    SQL"""SELECT #$topicColumns
          FROM topic t LEFT OUTER JOIN topic_key_mapping tkm ON (t.topic_id = tkm.topic_id)
          WHERE topic = $topicName
       """.as(topicParser.singleOpt)
  }

  def getBasicTopicInfo(cluster: String, topicName: String)(implicit conn: Connection): Option[BasicTopicInfo] = {
    SQL"""
        SELECT topic_id, topic, cluster FROM topic WHERE topic = $topicName AND cluster = $cluster;
      """
      .as(basicTopicInfoParser.singleOpt)
  }

  def getAllTopics()(implicit conn: Connection): Seq[Topic] = {
    SQL"""SELECT #$topicColumns
          FROM topic t LEFT OUTER JOIN topic_key_mapping tkm ON (t.topic_id = tkm.topic_id)
       """.as(topicParser.*)
  }

  def upsertTopicSchemaMapping(cluster: String, topicId: String, mapping: TopicSchemaMapping)(implicit conn: Connection) = {
    SQL"""
        INSERT INTO TOPIC_SCHEMA_MAPPING (topic_id, schema, cluster)
        values ($topicId, ${mapping.schema.name}, $cluster)
        ON CONFLICT DO NOTHING;
      """
      .execute()
  }

  def getTopicSchemaMappings(cluster: String, topic: String)(implicit conn: Connection): List[TopicSchemaMapping] = {
    SQL"""
        SELECT topic, schema
        FROM TOPIC_SCHEMA_MAPPING tsm INNER JOIN TOPIC t ON tsm.topic_id = t.topic_id
        WHERE t.topic = $topic AND tsm.cluster = $cluster
      """
      .as(topicSchemaMappingParser.*)
  }

  def getTopicSchemaMappings(topic: String)(implicit conn: Connection): List[String] = {
    SQL"""
        SELECT schema
        FROM TOPIC_SCHEMA_MAPPING tsm INNER JOIN TOPIC t ON tsm.topic_id = t.topic_id
        WHERE t.topic = $topic
      """
      .as[List[String]](scalar[String].*)
  }

  def getTopicKeyMapping(cluster: String, topic: String)(implicit conn: Connection): Option[TopicKeyMapping] = {
    SQL"""
        SELECT t.topic_id as topic_id, key_type, schema
        FROM TOPIC_KEY_MAPPING tkm INNER JOIN TOPIC t ON tkm.topic_id = t.topic_id
        WHERE t.topic = $topic AND tkm.cluster = $cluster
      """
      .as(topicKeyMappingParser.singleOpt)
  }

  def insertTopicKeyMapping(cluster: String, topicKeyMapping: TopicKeyMapping)(implicit conn: Connection) = {
    SQL"""
        INSERT INTO TOPIC_KEY_MAPPING (TOPIC_ID, KEY_TYPE, SCHEMA, CLUSTER) VALUES
        (${topicKeyMapping.topicId}, ${topicKeyMapping.keyType.toString}, ${topicKeyMapping.schema}, ${cluster})
      """
      .execute()
  }

  def getConfigSet(cluster: String, configName: String)(implicit connection: Connection): Option[TopicConfiguration] = {
    SQL"""
         SELECT #${topicConfigColumns} FROM TOPIC_CONFIG where cluster=$cluster AND name=$configName;
      """
      .as(topicConfigParser.singleOpt)
  }

  def getAllConfigSets(cluster: String)(implicit connection: Connection): Seq[TopicConfiguration] = {
    SQL"""
         SELECT #${topicConfigColumns} FROM TOPIC_CONFIG where cluster=$cluster;
      """
      .as(topicConfigParser.*)
  }

  def deleteTopicKeyMapping(topicId: String)(implicit connection: Connection) = {
    SQL"""
        DELETE FROM TOPIC_KEY_MAPPING WHERE TOPIC_ID = $topicId
      """
      .execute()
  }

  def deleteTopicSchemaMapping(topicId: String)(implicit connection: Connection) = {
    SQL"""
        DELETE FROM TOPIC_SCHEMA_MAPPING WHERE TOPIC_ID = $topicId
      """
      .execute()
  }

  def deleteTopic(topicId: String)(implicit connection: Connection) = {
    SQL"""
        DELETE FROM TOPIC WHERE TOPIC_ID = $topicId
      """
      .execute()
  }

  val topicConfigColumns = "name, cleanup_policy, partitions, retention_ms, replicas"
  val topicKeyTypeColumns = "key_type, schema"
  val topicColumns = s"topic, t.cluster as cluster, config_name as $topicConfigColumns, $topicKeyTypeColumns"

  implicit val topicConfigParser = Macro.parser[TopicConfiguration]("name", "cleanup_policy", "partitions", "retention_ms", "replicas")
  implicit val schemaParser = Macro.parser[SchemaRequest]("schema")
  implicit val topicSchemaMappingParser = Macro.parser[TopicSchemaMapping]("topic", "schema")
  implicit val keyTypeParser: Column[KeyType] = Column.nonNull { (value, _) =>
    value match {
      case keyType: String =>
        Right(KeyType.values.find(_.toString == keyType.toUpperCase).getOrElse(throw InvalidKeyTypeException(s"Invalid key type `$keyType`")))
    }
  }
  implicit val topicKeyMappingParser = Macro.parser[TopicKeyMapping]("topic_id", "key_type", "schema")
  implicit val basicTopicInfoParser = Macro.parser[BasicTopicInfo]("topic_id", "topic", "cluster")
  implicit val topicParser: RowParser[Topic] =
    (str("topic") ~ str("cluster") ~ topicConfigParser ~ get[Option[String]]("key_type") ~ get[Option[String]]("schema")) map {
      case topic ~ cluster ~ topicConfig ~ keyTypeOpt ~ schemaOpt =>
        val keyType = keyTypeOpt.map { k => KeyType.values.find(_.toString == k.toUpperCase).getOrElse(throw InvalidKeyTypeException(s"Invalid key type `$k`")) }
        Topic(topic, topicConfig, keyType.map { k => TopicKeyType(k, schemaOpt) }, cluster = Some(cluster))
    }
}
