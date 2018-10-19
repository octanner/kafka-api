package services

import java.util.UUID
import java.util.concurrent.TimeUnit

import daos.{ AclDao, TopicDao }
import javax.inject.Inject
import models.{ AclRole, KeyType }
import models.Models.{ Acl, TopicKeyMapping }
import models.http.HttpModels.{ AclRequest, TopicSchemaMapping }
import org.apache.kafka.common.acl._
import org.apache.kafka.common.resource.{ PatternType, ResourcePattern, ResourcePatternFilter, ResourceType }
import play.api.{ Configuration, Logger }
import play.api.db.Database
import utils.AdminClientUtil
import utils.Exceptions.{ InvalidRequestException, InvalidUserException, ResourceNotFoundException }

import scala.collection.mutable.Map
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{ Failure, Success, Try }

class AclService @Inject() (db: Database, dao: AclDao, topicDao: TopicDao, util: AdminClientUtil, conf: Configuration) {
  val logger = Logger(this.getClass)
  val VALID_TOPIC_KEY_VALUE_MAPPINGS = "valid"

  def getConfigMap(user: String): Future[Map[String, String]] = {

    for {
      cred <- getCredentials(user)
      cluster = cred.cluster
      acls <- getAclsForUsername(cluster, user)
    } yield {
      val configMap = getKafkaHostConfigMap(cluster)
      configMap += ("KAFKA_USERNAME" -> cred.username)
      configMap += ("KAFKA_PASSWORD" -> cred.password)

      val producers = acls.filter(_.role == AclRole.PRODUCER)
      val consumers = acls.filter(_.role == AclRole.CONSUMER)
      configMap += ("KAFKA_PRODUCER_TOPICS" -> producers.map(_.topic).mkString(","))
      configMap += ("KAFKA_CONSUMER_TOPICS" -> consumers.map(_.topic).mkString(","))
      acls.foreach { acl =>
        val topicConfigName = acl.topic.toUpperCase().replaceAll("[\\.-]", "_")
        if (acl.role == AclRole.CONSUMER && acl.consumerGroupName.isDefined && acl.consumerGroupName != Some("*")) {
          configMap += (topicConfigName + "_TOPIC_CONSUMER_GROUP" -> acl.consumerGroupName.get)
        }
        db.withConnection { implicit conn =>
          val schemaMappings = topicDao.getTopicSchemaMappings(cluster, acl.topic)
          val keyType = topicDao.getTopicKeyMapping(cluster, acl.topic) match {
            case Some(k) if k == KeyType.AVRO => s"""${k.keyType.toString}:${k.schema.getOrElse("")}"""
            case Some(k)                      => k.keyType.toString
            case None                         => ""
          }
          configMap += (topicConfigName + "_TOPIC_NAME" -> acl.topic)
          configMap += (topicConfigName + "_TOPIC_KEY_TYPE" -> keyType)
          configMap += (topicConfigName + "_TOPIC_SCHEMAS" -> schemaMappings.map { sm => sm.schema.name }.mkString(","))
        }
      }
      configMap
    }
  }

  def getCredentials(user: String) = {
    Future {
      db.withConnection { implicit conn =>
        dao.getCredentials(user) match {
          case Some(credentials) => credentials
          case None =>
            logger.error(s"Failed to get credentials for user $user. " +
              s"Either user name does not exist or it is not claimed")
            throw InvalidUserException(s"Either user $user does not exist or the user is not claimed.")
        }
      }
    }
  }

  def getAclsForUsername(cluster: String, user: String) = {
    Future {
      db.withConnection { implicit conn =>
        dao.getAclsForUsername(cluster, user).map { acl =>
          if (acl.role == AclRole.CONSUMER) acl else acl.copy(consumerGroupName = None)
        }
      }
    }
  }

  def getKafkaHostConfigMap(cluster: String): Map[String, String] = {
    val configMap = collection.mutable.Map[String, String]()
    conf.getOptional[String](cluster.toLowerCase() + ".kafka.hostname").map { h => configMap += ("KAFKA_HOSTNAME" -> h) }
    conf.getOptional[String](cluster.toLowerCase() + ".kafka.port").map { p => configMap += ("KAFKA_PORT" -> p) }
    conf.getOptional[String](cluster.toLowerCase() + ".kafka.location").map { l => configMap += ("KAFKA_LOCATION" -> l) }
    conf.getOptional[String](cluster.toLowerCase() + ".kafka.avro.registry.location").map { al =>
      configMap += ("KAFKA_AVRO_REGISTRY_LOCATION" -> al)
    }
    conf.getOptional[String](cluster.toLowerCase() + ".kafka.avro.registry.hostname").map { ah =>
      configMap += ("KAFKA_AVRO_REGISTRY_HOSTNAME" -> ah)
    }
    conf.getOptional[String](cluster.toLowerCase() + ".kafka.avro.registry.port").map { ap =>
      configMap += ("KAFKA_AVRO_REGISTRY_PORT" -> ap)
    }
    conf.getOptional[String](cluster.toLowerCase() + ".kafka.sasl.mechanism").map { sm =>
      configMap += ("KAFKA_SASL_MECHANISM" -> sm)
    }
    conf.getOptional[String](cluster.toLowerCase() + ".kafka.security.protocol").map { sp =>
      configMap += ("KAFKA_SECURITY_PROTOCOL" -> sp)
    }
    configMap
  }

  def getAclsByTopic(cluster: String, topic: String) = {
    Future {
      db.withConnection { implicit conn =>
        dao.getAclsForTopic(cluster, topic).map { acl =>
          if (acl.role == AclRole.CONSUMER) acl else acl.copy(consumerGroupName = None)
        }
      }
    }
  }

  def claimAcl(cluster: String) = {
    Future {
      db.withTransaction { implicit conn =>
        val aclCredentials = dao.getUnclaimedAcl(cluster)
        Try(dao.claimAcl(cluster, aclCredentials.username)) match {
          case Success(_) =>
            logger.info(s"Claimed user '${aclCredentials.username}' in cluster '$cluster'")
            aclCredentials
          case Failure(e) =>
            logger.error(s"Failed to claim ACL in database: ${e.getMessage}")
            throw e
        }
      }
    }
  }

  def createPermissions(cluster: String, aclRequest: AclRequest) = {
    db.withTransaction { implicit conn =>
      val topicKeyMapping = topicDao.getTopicKeyMapping(cluster, aclRequest.topic)
      val topicSchemaMappings = topicDao.getTopicSchemaMappings(cluster, aclRequest.topic)
      val validationMessage = validateTopicKeyAndValueSchemaMappings(topicKeyMapping, topicSchemaMappings)
      val validatedAclRequest = validateOrAddConsumerGroupName(aclRequest)
      validationMessage match {
        case VALID_TOPIC_KEY_VALUE_MAPPINGS =>
          Try(dao.addPermissionToDb(cluster, validatedAclRequest)) match {
            case Success(id) =>
              createKafkaAcl(cluster, validatedAclRequest)
              (id -> validatedAclRequest)
            case Failure(e) =>
              logger.error(s"Failed to create permission in DB: ${e.getMessage}")
              throw e
          }
        case invalidMessage: String =>
          throw InvalidRequestException(s"$invalidMessage for topic `${aclRequest.topic}`, before creating acl")
      }

    }
  }

  def createKafkaAcl(cluster: String, aclRequest: AclRequest) = {
    val topicAclBinding = createAclBinding(aclRequest, ResourceType.TOPIC, aclRequest.topic, host = "*")
    val groupName = if (aclRequest.role == AclRole.CONSUMER)
      aclRequest.consumerGroupName.get
    else
      "*"
    val groupAclBinding = createAclBinding(aclRequest, ResourceType.GROUP, resourceName = groupName, host = "*")

    val adminClient = util.getAdminClient(cluster)
    val aclCreationResponse = adminClient.createAcls(List(topicAclBinding, groupAclBinding).asJava).all()
    adminClient.close()
    Try(aclCreationResponse.get) match {
      case Success(_) =>
        logger.info(s"Successfully added permission for '${aclRequest.user}' with role '${aclRequest.role}'" +
          s"${if (aclRequest.role == AclRole.CONSUMER) " to consumer group " + aclRequest.consumerGroupName.get else ""}" +
          s" on topic '${aclRequest.topic}' in cluster '$cluster' to Kafka")
      case Failure(e: InvalidRequestException) =>
        logger.error(e.getMessage, e)
        throw e
      case Failure(e) =>
        logger.error(s"Unable to add permission for '${aclRequest.user}' with role '${aclRequest.role}'" +
          s"${if (aclRequest.role == AclRole.CONSUMER) " to consumer group " + aclRequest.consumerGroupName.get else ""}" +
          s"on topic '${aclRequest.topic}' in cluster '$cluster' to Kafka", e)
        throw e
    }
  }

  def deleteAcl(id: String) = {
    Future {
      db.withTransaction { implicit conn =>
        val acl = dao.getAcl(id).getOrElse(throw ResourceNotFoundException(s"Acl not found for id $id"))
        dao.deleteAcl(id)
        deleteKafkaAcl(acl)
      }
    }
  }

  def deleteKafkaAcl(acl: Acl) = {
    val topicAclBinding = createAclBindingFilter(acl, ResourceType.TOPIC, acl.topic, host = "*")
    val groupAclBinding = createAclBindingFilter(acl, ResourceType.GROUP, resourceName = acl.consumerGroupName.getOrElse("*"), host = "*")

    val adminClient = util.getAdminClient(acl.cluster)
    val aclDeletionResponse = adminClient.deleteAcls(List(topicAclBinding, groupAclBinding).asJava).all()
    adminClient.close()
    Try(aclDeletionResponse.get) match {
      case Success(bindings) =>
        logger.info(s"Successfully deleted permissions for '${acl.user}' with role '${acl.role.role}' " +
          s"on topic '${acl.topic}' in cluster '${acl.cluster}' to Kafka")
        logger.info(s"Delete Response : $bindings")
      case Failure(e) =>
        logger.error(s"Unable to delete permission for '${acl.user}' with role '${acl.role.role}' " +
          s"on topic '${acl.topic}' in cluster '${acl.cluster}' to Kafka", e)
        throw e
    }
  }

  private def createAclBinding(aclRequest: AclRequest, resourceType: ResourceType, resourceName: String, host: String) = {
    val username = aclRequest.user
    val role = aclRequest.role.operation
    val resourcePattern = new ResourcePattern(resourceType, resourceName, PatternType.LITERAL)
    val accessControlEntry = new AccessControlEntry(s"User:$username", host, role, AclPermissionType.ALLOW)
    new AclBinding(resourcePattern, accessControlEntry)
  }

  private def createAclBindingFilter(acl: Acl, resourceType: ResourceType, resourceName: String, host: String) = {
    val username = acl.user
    val role = acl.role.operation
    val resourcePattern = new ResourcePatternFilter(resourceType, resourceName, PatternType.LITERAL)
    val accessControlEntry = new AccessControlEntryFilter(s"User:$username", host, role, AclPermissionType.ALLOW)
    new AclBindingFilter(resourcePattern, accessControlEntry)
  }

  private def validateTopicKeyAndValueSchemaMappings(
    topicKeyMapping:     Option[TopicKeyMapping],
    topicSchemaMappings: List[TopicSchemaMapping]): String = {
    if (topicKeyMapping.isEmpty && topicSchemaMappings.isEmpty) {
      "Topic Key Mapping and atleast one Topic Value Schema Mapping needs to be defined"
    } else if (topicKeyMapping.isDefined && topicSchemaMappings.isEmpty) {
      "Atleast one Topic Value Schema Mapping need to be defined"
    } else if (topicKeyMapping.isEmpty && !topicSchemaMappings.isEmpty) {
      "Topic Key Mapping need to be defined"
    } else {
      VALID_TOPIC_KEY_VALUE_MAPPINGS
    }
  }

  def validateOrAddConsumerGroupName(aclRequest: AclRequest) = {
    if (aclRequest.role == AclRole.CONSUMER && aclRequest.consumerGroupName.isEmpty) {
      aclRequest.copy(consumerGroupName = Some(s"${aclRequest.user}-${UUID.randomUUID.toString}"))
    } else if (aclRequest.role == AclRole.PRODUCER && aclRequest.consumerGroupName.isDefined) {
      logger.error(s"Ignoring consumer group name `${aclRequest.consumerGroupName.get}` for producer role")
      aclRequest.copy(consumerGroupName = None)
    } else {
      aclRequest
    }
  }
}
