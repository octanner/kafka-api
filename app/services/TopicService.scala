package services

import java.util.concurrent.ExecutionException

import daos.TopicDao
import javax.inject.Inject
import models.Models.{ Topic, TopicConfiguration, TopicKeyMapping }
import models.http.HttpModels.{ SchemaRequest, TopicKeyMappingRequest, TopicSchemaMapping }
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.KafkaFuture
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.errors.TopicExistsException
import org.postgresql.util.PSQLException
import play.api.db.Database
import play.api.{ Configuration, Logger }
import utils.AdminClientUtil
import utils.Exceptions.{ NonUniqueTopicNameException, ResourceExistsException, ResourceNotFoundException, UndefinedResourceException }

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{ Failure, Success, Try }

class TopicService @Inject() (
    db:        Database,
    dao:       TopicDao,
    conf:      Configuration,
    util:      AdminClientUtil,
    schemaSvc: SchemaRegistryService) {
  import TopicService._

  val logger = Logger(this.getClass)

  def createTopic(cluster: String, topic: Topic): Future[Topic] = {
    val topicName = topic.name

    Future {
      val configSet = db.withConnection { implicit conn => dao.getConfigSet(cluster, topic.config.name) }
        .getOrElse(throw new UndefinedResourceException(s"Config Set not yet setup for cluster `$cluster` and config name `${topic.config.name}`"))
      val partitions = topic.config.partitions.getOrElse(configSet.partitions.getOrElse(
        conf.get[Int](cluster.toLowerCase + DEFAULT_PARTITIONS_CONFIG)))
      val replicas = topic.config.replicas.getOrElse(configSet.replicas.getOrElse(
        conf.get[Int](cluster.toLowerCase + DEFAULT_REPLICAS_CONFIG)))
      val retentionMs = topic.config.retentionMs.getOrElse(configSet.retentionMs.getOrElse(
        conf.get[Long](cluster.toLowerCase + DEFAULT_RETENTION_CONFIG)))
      val cleanupPolicy = topic.config.cleanupPolicy.getOrElse(configSet.cleanupPolicy.getOrElse(
        TopicConfig.CLEANUP_POLICY_DELETE))
      val configs = Map(
        TopicConfig.RETENTION_MS_CONFIG -> retentionMs.toString,
        TopicConfig.CLEANUP_POLICY_CONFIG -> cleanupPolicy)
      val topicConfig = TopicConfiguration(topic.config.name, Some(cleanupPolicy), Some(partitions), Some(retentionMs), Some(replicas))

      Try(createTopicInKafka(cluster, topicName, partitions, replicas, configs).get()) match {
        case Success(_) =>
          createTopicInDB(cluster, topic, partitions, replicas, retentionMs, cleanupPolicy)
          logger.info(s"""Successfully Created Topic ${topic.name} with ${partitions} partitions, ${replicas} replicas, ${retentionMs} retention ms, "${cleanupPolicy}" cleanup policy""")
          topic.copy(config = topicConfig, cluster = Some(cluster))
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

  def createTopicSchemaMapping(cluster: String, mapping: TopicSchemaMapping) = {
    Future {
      val topic = db.withConnection { implicit conn => dao.getBasicTopicInfo(cluster, mapping.topic) }
        .getOrElse(throw ResourceNotFoundException(s"Topic `${mapping.topic}` not found in cluster `$cluster`"))
      for (schema <- schemaSvc.getSchema(cluster, mapping.schema.name)) yield {
        val validatedSchemaRequest = SchemaRequest(schema.subject)
        db.withTransaction { implicit conn =>
          dao.upsertTopicSchemaMapping(cluster, topic.id, mapping.copy(schema = validatedSchemaRequest))
        }
        mapping
      }

    }.flatten
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

    val adminClient = util.getAdminClient(cluster)
    val topic = new NewTopic(topicName, partitions, replicas.toShort).configs(topicConfigs.asJava)
    val topicCreationResult = Try(adminClient.createTopics(List(topic).asJava).all())
    adminClient.close()
    topicCreationResult.get
  }

  def getTopic(topicName: String): Future[Option[Topic]] = {
    Future {
      db.withConnection { implicit conn =>
        dao.getTopicInfo(topicName)
      }
    }
  }

  def getAllTopics: Future[Seq[Topic]] = {
    Future {
      db.withConnection { implicit conn =>
        dao.getAllTopics
      }
    }
  }

  def getTopicSchemaMappings(cluster: String, topic: String): Future[List[TopicSchemaMapping]] = {
    Future {
      db.withConnection { implicit conn =>
        dao.getTopicSchemaMappings(cluster, topic)
      }
    }
  }

  def getTopicSchemaMappings(topic: String): Future[List[String]] = {
    Future {
      db.withConnection { implicit conn =>
        dao.getTopicSchemaMappings(topic)
      }
    }
  }

  def createTopicKeyMapping(cluster: String, topicKeyMappingRequest: TopicKeyMappingRequest) = {
    val topicFut = Future {
      db.withConnection { implicit conn => dao.getBasicTopicInfo(cluster, topicKeyMappingRequest.topic) }
    }
    for {
      topicOpt <- topicFut
    } yield {
      val topic = topicOpt.getOrElse(throw ResourceNotFoundException(s"Topic `${topicKeyMappingRequest.topic}` Not Found in cluster ${cluster}"))
      val topiKeyMapping = TopicKeyMapping(topic.id, topicKeyMappingRequest.keyType,
        topicKeyMappingRequest.schema.map(_.name))

      db.withTransaction { implicit conn =>
        Try(dao.insertTopicKeyMapping(cluster, topiKeyMapping)) match {
          case Success(insert) => insert
          case Failure(e: PSQLException) if (e.getSQLState == PSQL_UNIQUE_VIOLATION_CODE) =>
            throw ResourceExistsException(s"Topic Key Mapping already exists for topic `${topic.name}` and cannot be changed")
          case Failure(e) => throw e

        }
      }
    }
  }

  def getAllConfigSets(cluster: String) = {
    Future {
      db.withConnection { implicit conn =>
        dao.getAllConfigSets(cluster)
      }
    }
  }

  def getConfigSet(cluster: String, name: String) = {
    Future {
      db.withConnection { implicit conn =>
        dao.getConfigSet(cluster, name).getOrElse(throw ResourceNotFoundException(s"Config Set not defined for cluster `$cluster` and name `$name`"))
      }
    }
  }

}

object TopicService {
  val DEFAULT_PARTITIONS_CONFIG = ".kafka.topic.default.partitions"
  val DEFAULT_REPLICAS_CONFIG = ".kafka.topic.default.replicas"
  val DEFAULT_RETENTION_CONFIG = ".kafka.topic.default.retention.ms"
  val PSQL_UNIQUE_VIOLATION_CODE = "23505"
}
