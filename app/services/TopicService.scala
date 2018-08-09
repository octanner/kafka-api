package services

import java.util.Properties
import java.util.concurrent.ExecutionException

import daos.TopicDao
import javax.inject.Inject
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.errors.TopicExistsException
import models.Models.Topic
import org.apache.kafka.clients.admin.{ AdminClient, AdminClientConfig }
import org.apache.kafka.common.config.TopicConfig
import play.api.Logger
import play.api.db.Database

import scala.collection.JavaConverters._
import scala.util.{ Failure, Success, Try }

class TopicService @Inject() (db: Database, dao: TopicDao) {
  import TopicService._

  val logger = Logger(this.getClass)

  def createTopic(cluster: String, topic: Topic): Unit = {
    val topicName = topic.name
    val partitions = topic.config.partitions.getOrElse(System.getenv(cluster.toUpperCase() + "_DEFAULT_PARTITIONS").toInt)
    val replicas = topic.config.replicas.getOrElse(System.getenv(cluster.toUpperCase() + "_DEFAULT_REPLICAS").toInt)
    val retentionMs = topic.config.retentionMs.getOrElse(System.getenv(cluster.toUpperCase() + "_DEFAULT_RETENTION").toLong)
    val cleanupPolicy = topic.config.cleanupPolicy.getOrElse(TopicConfig.CLEANUP_POLICY_DELETE)
    val topicConfig = Map(TopicConfig.RETENTION_MS_CONFIG -> retentionMs.toString, TopicConfig.CLEANUP_POLICY_CONFIG -> cleanupPolicy)

    Try(createTopic(cluster, topicName, partitions, replicas, topicConfig).get()) match {
      case Success(_) =>
        db.withConnection { implicit conn =>
          dao.insert(cluster, topic, partitions, replicas, retentionMs, cleanupPolicy)
        }
        logger.info(s"""Successfully Created Topic ${topic.name} with ${partitions} partitions, ${replicas} replicas, ${retentionMs} retention ms, "${cleanupPolicy}" cleanup policy""")
      case Failure(e: ExecutionException) if e.getCause.isInstanceOf[TopicExistsException] =>
        logger.error(s"""------ Topic "${topic.name}" Already Exist""")
      case Failure(e) => logger.error(s"***** failed to create topic: ${e.getMessage}")
    }
  }

  private def createTopic(
    cluster:      String,
    topicName:    String,
    partitions:   Int,
    replicas:     Int,
    topicConfigs: Map[String, String]): KafkaFuture[Void] = {
    val kafkaHostName = System.getenv(cluster.toUpperCase + "_KAFKA_LOCATION")
    val kafkaSecurityProtocol = System.getenv(cluster.toUpperCase + "_KAFKA_SECURITY_PROTOCOL")
    val kafkaSecurityMechanism = System.getenv(cluster.toUpperCase + "_KAFKA_SECURITY_MECHANISM")
    val username = System.getenv(cluster.toUpperCase + "_KAFKA_ADMIN_USERNAME")
    val password = System.getenv(cluster.toUpperCase + "_KAFKA_ADMIN_PASSWORD")
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHostName)
    props.put(AdminClientConfig.CLIENT_ID_CONFIG, ADMIN_CLIENT_ID)
    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, kafkaSecurityProtocol)
    props.put(SaslConfigs.SASL_MECHANISM, kafkaSecurityMechanism)
    props.put(
      SaslConfigs.SASL_JAAS_CONFIG,
      s"""org.apache.kafka.common.security.plain.PlainLoginModule required username="$username" password="$password";""")

    val adminClient = AdminClient.create(props)
    val topic = new NewTopic(topicName, partitions, replicas.toShort).configs(topicConfigs.asJava)
    val topicCreationResult = adminClient.createTopics(List(topic).asJava).all()
    adminClient.close()
    topicCreationResult
  }

}

object TopicService {
  val ADMIN_CLIENT_ID = "kafka-api"
}
