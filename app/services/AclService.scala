package services

import java.util.UUID

import daos.{ AclDao, TopicDao }
import javax.inject.Inject
import models.Models.{ Acl, TopicKeyMapping }
import models.http.HttpModels.{ AclRequest, TopicSchemaMapping }
import models.{ AclRole, KeyType }
import org.apache.kafka.common.acl._
import org.apache.kafka.common.resource.{ PatternType, ResourcePattern, ResourcePatternFilter, ResourceType }
import play.api.db.Database
import play.api.{ Configuration, Logger }
import utils.AdminClientUtil
import utils.Exceptions.{ InvalidRequestException, InvalidUserException, ResourceNotFoundException }

import scala.collection.JavaConverters._
import scala.collection.mutable.Map
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{ Failure, Success, Try }

class AclService @Inject() (db: Database, dao: AclDao, topicDao: TopicDao, util: AdminClientUtil, conf: Configuration) {
  import AclService._
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
      configMap += ("KAFKA_CONSUMER_TOPICS" -> consumers.map(_.topic).distinct.mkString(","))
      acls.foreach { acl =>
        val topicConfigName = getTopicConfigPrefix(acl.topic)
        getConsumerGroupConfigMap(acl, topicConfigName, configMap)
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

  private def getConsumerGroupConfigMap(acl: Acl, topicConfigName: String, configMap: collection.mutable.Map[String, String]) = {
    if (acl.role == AclRole.CONSUMER && acl.consumerGroupName.isDefined && acl.consumerGroupName.isDefined) {
      val key = topicConfigName + "_TOPIC_CONSUMER_GROUPS"
      if (configMap.contains(key))
        configMap += (key -> Seq(configMap(key), acl.consumerGroupName.get).mkString(","))
      else
        configMap += (key -> acl.consumerGroupName.get)
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
        dao.getAclsForUsername(cluster, user)
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
        Try(dao.claimUser(cluster, aclCredentials.username)) match {
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
    logger.info(s"acl create request $aclRequest")

    val validatedAclRequest = validateOrAddConsumerGroupName(aclRequest)
    logger.info(s"validated acl create request $validatedAclRequest")
    Try(db.withTransaction { implicit conn => dao.addPermissionToDb(cluster, validatedAclRequest) }) match {
      case Success(id) =>
        createKafkaAcl(cluster, validatedAclRequest)
        val acl = db.withTransaction { implicit conn => dao.getAcl(id) }
        if (acl.isEmpty) {
          logger.error(s"cannot get acl from database for acl id ${id}")
        }
        (id -> acl.get)
      case Failure(e) =>
        logger.error(s"Failed to create permission in DB: ${e.getMessage}")
        throw e
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
        val otherAclsForUserTopicAndRole = dao.getAclsByUsernameTopicClusterAndRole(acl).filter { a => a.id != id }
        dao.deleteAcl(id)
        deleteKafkaAcl(acl, otherAclsForUserTopicAndRole)
      }
    }
  }

  def deleteKafkaAcl(acl: Acl, otherAclsForUserTopicAndRole: List[Acl]) = {
    val groupAclBinding = createAclBindingFilter(acl, ResourceType.GROUP, resourceName = acl.consumerGroupName.getOrElse("*"), host = "*")
    val aclBindingsToRemove = if (otherAclsForUserTopicAndRole.isEmpty) {
      val topicAclBinding = createAclBindingFilter(acl, ResourceType.TOPIC, acl.topic, host = "*")
      List(topicAclBinding, groupAclBinding)
    } else {
      List(groupAclBinding)
    }

    val adminClient = util.getAdminClient(acl.cluster)
    val aclDeletionResponse = adminClient.deleteAcls(aclBindingsToRemove.asJava).all()
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

  def unclaimUser(user: String) = {
    val cluster = db.withConnection { implicit conn => dao.getCredentials(user) }.getOrElse(
      throw new ResourceNotFoundException(s"User ${user} does not exist or is already unclaimed")
    ).cluster
    val acls = db.withConnection { implicit conn => dao.getAclsForUsername(cluster, user) }
    Future.sequence(acls.map { acl => deleteAcl(acl.id) }).map { _ =>
      db.withTransaction { implicit conn => dao.unclaimUser(cluster, user) }
    }
  }
}

object AclService {
  val ENVS = List("TEST", "DEV", "QA", "STG", "PROD", "PRD")
  def getTopicConfigPrefix(topic: String): String = {
    val topicConfigPrefix = topic.toUpperCase().replaceAll("[\\.-]", "_")
    val topicPrefixParts = topicConfigPrefix.split("_")
    if (ENVS.contains(topicPrefixParts.head))
      topicPrefixParts.drop(1).mkString("_")
    else
      topicPrefixParts.mkString("_")
  }
}
