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
import play.api.Logger
import play.api.db.Database
import utils.Exceptions.NonUniqueTopicNameException

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{ Failure, Success, Try }

class TopicService @Inject() (db: Database, dao: TopicDao) {
  import TopicService._

  val logger = Logger(this.getClass)

  def createTopic(cluster: String, topic: Topic): Future[Topic] = {
    val topicName = topic.name
    val partitions = topic.config.partitions.getOrElse(System.getenv(cluster.toUpperCase() + "_DEFAULT_PARTITIONS").toInt)
    val replicas = topic.config.replicas.getOrElse(System.getenv(cluster.toUpperCase() + "_DEFAULT_REPLICAS").toInt)
    val retentionMs = topic.config.retentionMs.getOrElse(System.getenv(cluster.toUpperCase() + "_DEFAULT_RETENTION").toLong)
    val cleanupPolicy = topic.config.cleanupPolicy.getOrElse(TopicConfig.CLEANUP_POLICY_DELETE)
    val configs = Map(TopicConfig.RETENTION_MS_CONFIG -> retentionMs.toString, TopicConfig.CLEANUP_POLICY_CONFIG -> cleanupPolicy)
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

    val kafkaHostName = System.getenv(cluster.toUpperCase + "_KAFKA_LOCATION")
    val kafkaSecurityProtocol = System.getenv(cluster.toUpperCase + "_KAFKA_SECURITY_PROTOCOL")
    val kafkaSaslMechanism = System.getenv(cluster.toUpperCase + "_KAFKA_SASL_MECHANISM")
    val username = System.getenv(cluster.toUpperCase + "_KAFKA_ADMIN_USERNAME")
    val password = System.getenv(cluster.toUpperCase + "_KAFKA_ADMIN_PASSWORD")
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHostName)
    props.put(AdminClientConfig.CLIENT_ID_CONFIG, ADMIN_CLIENT_ID)
    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, kafkaSecurityProtocol)
    props.put(SaslConfigs.SASL_MECHANISM, kafkaSaslMechanism)
    props.put(
      SaslConfigs.SASL_JAAS_CONFIG,
      s"""org.apache.kafka.common.security.plain.PlainLoginModule required username="$username" password="$password";""")

    logger.info(s"${kafkaHostName}; ${kafkaSecurityProtocol}; ${kafkaSaslMechanism}; ${username}: $password ")
    val adminClient = AdminClient.create(props)
    val topic = new NewTopic(topicName, partitions, replicas.toShort).configs(topicConfigs.asJava)
    val topicCreationResult = adminClient.createTopics(List(topic).asJava).all()
    adminClient.close()
    topicCreationResult
  }

}

object TopicService {
  val ADMIN_CLIENT_ID = "kafka-api"
  val PSQL_UNIQUE_VIOLATION_CODE = "23505"
}
