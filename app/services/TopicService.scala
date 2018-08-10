package services

import java.util.Properties
import java.util.concurrent.ExecutionException

import daos.TopicDao
import javax.inject.Inject
import models.Models.{ Topic, TopicConfiguration }
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.{ AdminClient, AdminClientConfig, NewTopic }
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.config.{ SaslConfigs, TopicConfig }
import org.apache.kafka.common.errors.TopicExistsException
import org.postgresql.util.PSQLException
import play.api.{ Configuration, Logger }
import play.api.db.Database
import utils.Exceptions.NonUniqueTopicNameException

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{ Failure, Success, Try }

class TopicService @Inject() (db: Database, dao: TopicDao, conf: Configuration) {
  import TopicService._

  val logger = Logger(this.getClass)

  def createTopic(cluster: String, topic: Topic): Future[Topic] = {
    val topicName = topic.name
    val partitions = topic.config.partitions.getOrElse(
      conf.get[Int](cluster.toLowerCase + DEFAULT_PARTITIONS_CONFIG))
    val replicas = topic.config.replicas.getOrElse(
      conf.get[Int](cluster.toLowerCase + DEFAULT_REPLICAS_CONFIG))
    val retentionMs = topic.config.retentionMs.getOrElse(
      conf.get[Long](cluster.toLowerCase + DEFAULT_RETENTION_CONFIG))
    val cleanupPolicy = topic.config.cleanupPolicy.getOrElse(TopicConfig.CLEANUP_POLICY_DELETE)
    val configs = Map(
      TopicConfig.RETENTION_MS_CONFIG -> retentionMs.toString,
      TopicConfig.CLEANUP_POLICY_CONFIG -> cleanupPolicy)
    val topicConfig = TopicConfiguration(Some(cleanupPolicy), Some(partitions), Some(retentionMs), Some(replicas))

    Future {
      Try(createTopicInKafka(cluster, topicName, partitions, replicas, configs).get()) match {
        case Success(_) =>
          createTopicInDB(cluster, topic, partitions, replicas, retentionMs, cleanupPolicy)
          logger.info(s"""Successfully Created Topic ${topic.name} with ${partitions} partitions, ${replicas} replicas, ${retentionMs} retention ms, "${cleanupPolicy}" cleanup policy""")
          topic.copy(config = topicConfig)
        case Failure(e: ExecutionException) if e.getCause.isInstanceOf[TopicExistsException] =>
          logger.error(s"""Topic "${topic.name}" already exists""")
          topic.copy(config = topicConfig)
        case Failure(e: ExecutionException) =>
          throw e.getCause
        case Failure(e) =>
          logger.error(s"Failed to create topic: ${e.getMessage}", e)
          throw e
      }
    }
  }

  private def createTopicInDB(cluster: String, topic: Topic, partitions: Int, replicas: Int, retentionMs: Long, cleanupPolicy: String) = {
    db.withConnection { implicit conn =>
      Try(dao.insert(cluster, topic, partitions, replicas, retentionMs, cleanupPolicy)) match {
        case Success(_) =>
        case Failure(e: PSQLException) if (e.getSQLState == PSQL_UNIQUE_VIOLATION_CODE) =>
          throw NonUniqueTopicNameException("Topic Name must be unique across all clusters")
        case Failure(e) =>
          logger.error(s"Cannot write to DB ${e.getMessage}", e)
          throw e
      }
    }
  }

  private def createTopicInKafka(
    cluster:      String,
    topicName:    String,
    partitions:   Int,
    replicas:     Int,
    topicConfigs: Map[String, String]): KafkaFuture[Void] = {

    val kafkaHostName = conf.get[String](cluster.toLowerCase + KAFKA_LOCATION_CONFIG)
    val kafkaSecurityProtocol = conf.getOptional[String](cluster.toLowerCase + KAFKA_SECURITY_PROTOCOL_CONFIG)
    val kafkaSaslMechanism = conf.getOptional[String](cluster.toLowerCase + KAFKA_SASL_MECHANISM_CONFIG)
    val username = conf.getOptional[String](cluster.toLowerCase + KAFKA_ADMIN_USERNAME_CONFIG)
    val password = conf.getOptional[String](cluster.toLowerCase + KAFKA_ADMIN_PASSWORD_CONFIG)

    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHostName)
    props.put(AdminClientConfig.CLIENT_ID_CONFIG, ADMIN_CLIENT_ID)
    kafkaSecurityProtocol.map { props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, _) }
    kafkaSaslMechanism.map { props.put(SaslConfigs.SASL_MECHANISM, _) }
    kafkaSaslMechanism match {
      case Some("PLAIN") =>
        props.put(
          SaslConfigs.SASL_JAAS_CONFIG,
          s"""org.apache.kafka.common.security.plain.PlainLoginModule required username="${username.getOrElse("")}" password="${password.getOrElse("")}";""")
      case _ =>
    }

    val adminClient = AdminClient.create(props)
    val topic = new NewTopic(topicName, partitions, replicas.toShort).configs(topicConfigs.asJava)
    val topicCreationResult = adminClient.createTopics(List(topic).asJava).all()
    adminClient.close()
    topicCreationResult
  }

  def getTopic(cluster: String, topicName: String): Option[Topic] = {
    db.withConnection { implicit conn =>
      dao.getTopicInfo(cluster, topicName)
    }
  }
}

object TopicService {
  val ADMIN_CLIENT_ID = "kafka-api"
  val DEFAULT_PARTITIONS_CONFIG = ".kafka.topic.default.partitions"
  val DEFAULT_REPLICAS_CONFIG = ".kafka.topic.default.replicas"
  val DEFAULT_RETENTION_CONFIG = ".kafka.topic.default.retention.ms"
  val KAFKA_LOCATION_CONFIG = ".kafka.location"
  val KAFKA_SECURITY_PROTOCOL_CONFIG = ".kafka.security.protocol"
  val KAFKA_SASL_MECHANISM_CONFIG = ".kafka.sasl.mechanism"
  val KAFKA_ADMIN_USERNAME_CONFIG = ".kafka.admin.username"
  val KAFKA_ADMIN_PASSWORD_CONFIG = ".kafka.admin.password"
  val PSQL_UNIQUE_VIOLATION_CODE = "23505"
}
