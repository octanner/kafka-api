package services

import java.util.Properties

import daos.TopicDao
import javax.inject.Inject
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.KafkaFuture

import java.util.concurrent.ExecutionException
//import kafka.admin.RackAwareMode
//import kafka.zk.AdminZkClient
//import kafka.zk.KafkaZkClient
//import org.apache.kafka.common.utils.Time
import models.Models.Topic
import org.apache.kafka.clients.admin.{AdminClientConfig, AdminClient}
import org.apache.kafka.common.config.TopicConfig
import play.api.Logger
import play.api.db.Database
import collection.JavaConverters._

import scala.util.{Failure, Success, Try}

class TopicService @Inject()(db: Database, dao: TopicDao) {
  import TopicService._

  val logger = Logger("TopicService")

  def createTopic(cluster: String, topic: Topic) = {
    try {
      val topicName = topic.name
      val partitions = topic.config.partitions.getOrElse(System.getenv(cluster.toUpperCase() + "_DEFAULT_PARTITIONS").toInt)
      val replicas = topic.config.replicas.getOrElse(System.getenv(cluster.toUpperCase() + "_DEFAULT_REPLICAS").toInt)
      val retentionMs = topic.config.retentionMs.getOrElse(System.getenv(cluster.toUpperCase() + "_DEFAULT_RETENTION").toLong)
      val cleanupPolicy = topic.config.cleanupPolicy.getOrElse(TopicConfig.CLEANUP_POLICY_DELETE)
      val topicConfig = Map(TopicConfig.RETENTION_MS_CONFIG -> retentionMs.toString, TopicConfig.CLEANUP_POLICY_CONFIG -> cleanupPolicy)

      Try(createTopic(cluster, topicName, partitions, replicas, topicConfig).get()) match {
        case Success(_) =>

        case Failure(e) =>
      }


    }
  }

  private def createTopic(cluster: String,
                          topicName: String,
                          partitions: Int,
                          replicas: Int,
                          topicConfigs: Map[String, String]): KafkaFuture[Void] = {
    val kafkaHostName = System.getenv(cluster.toUpperCase + "_KAFKA_HOSTNAME")
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHostName)
    props.put(AdminClientConfig.CLIENT_ID_CONFIG, ADMIN_CLIENT_ID)

    val adminClient = AdminClient.create(props)
    val topic = new NewTopic(topicName, partitions, replicas.toShort).configs(topicConfigs.asJava)
    adminClient.createTopics(List(topic).asJava).all()
  }

}

object TopicService {
  val ADMIN_CLIENT_ID = "kafka-api"
}
