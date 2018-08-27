package services

import java.util.concurrent.TimeUnit

import daos.AclDao
import javax.inject.Inject
import models.Models.Acl
import models.http.HttpModels.AclRequest
import org.apache.kafka.common.acl._
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.resource.{ PatternType, ResourcePattern, ResourcePatternFilter, ResourceType }
import play.api.Logger
import play.api.db.Database
import utils.AdminClientUtil
import utils.Exceptions.{ InvalidUserException, ResourceNotFound }

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{ Failure, Success, Try }

class AclService @Inject() (db: Database, dao: AclDao, util: AdminClientUtil) {

  val logger = Logger(this.getClass)

  def getCredentials(cluster: String, user: String) = {
    Future {
      db.withConnection { implicit conn =>
        dao.getCredentials(cluster, user) match {
          case Some(credentials) => credentials
          case None =>
            logger.error(s"Failed to get credentials for cluster $cluster and user $user. " +
              s"Either user name does not exist or it is not claimed")
            throw InvalidUserException(s"Either user $user does not exist in cluster $cluster or the user is not claimed.")
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
      Try(dao.addPermissionToDb(cluster, aclRequest)) match {
        case Success(id) =>
          addPermissionToKafka(cluster, aclRequest)
          id
        case Failure(e) =>
          logger.error(s"Failed to create permission in DB: ${e.getMessage}")
          throw e
      }
    }
  }

  def addPermissionToKafka(cluster: String, aclRequest: AclRequest) = {
    Try(createKafkaAcl(cluster, aclRequest)) match {
      case Success(_) =>
        logger.info(s"Successfully added permission for '${aclRequest.user}' with role '${aclRequest.role}' " +
          s"on topic '${aclRequest.topic}' in cluster '$cluster' to Kafka")
      case Failure(e: InvalidRequestException) =>
        logger.error(e.getMessage, e)
        throw e
      case Failure(e) =>
        logger.error(s"Unable to add permission for '${aclRequest.user}' with role '${aclRequest.role}' " +
          s"on topic '${aclRequest.topic}' in cluster '$cluster' to Kafka", e)
        throw e
    }
  }

  def createKafkaAcl(cluster: String, aclRequest: AclRequest) = {
    val topicName = aclRequest.topic
    val username = aclRequest.user
    val role = aclRequest.role.operation
    val topicResourcePattern = new ResourcePattern(ResourceType.TOPIC, topicName, PatternType.LITERAL)
    val groupResourcePattern = new ResourcePattern(ResourceType.GROUP, "*", PatternType.LITERAL)
    val accessControlEntry = new AccessControlEntry(s"User:$username", "*", role, AclPermissionType.ALLOW)
    val topicAclBinding = new AclBinding(topicResourcePattern, accessControlEntry)
    val groupAclBinding = new AclBinding(groupResourcePattern, accessControlEntry)

    val adminClient = util.getAdminClient(cluster)
    val aclCreationResponse = Try(adminClient.createAcls(List(topicAclBinding, groupAclBinding).asJava).all()
      .get(500, TimeUnit.MILLISECONDS))
    adminClient.close()
    aclCreationResponse.get
  }

  def deleteAcl(id: String) = {
    Future {
      db.withTransaction { implicit conn =>
        val acl = dao.getAcl(id).getOrElse(throw ResourceNotFound(s"Acl not found for id $id"))
        dao.deleteAcl(id)
        Try(deleteKafkaAcl(acl)) match {
          case Success(_) =>
            logger.info(s"Successfully deleted permissions for '${acl.user}' with role '${acl.role.role}' " +
              s"on topic '${acl.topicName}' in cluster '${acl.cluster}' to Kafka")
          case Failure(e) =>
            logger.error(s"Unable to delete permission for '${acl.user}' with role '${acl.role.role}' " +
              s"on topic '${acl.topicName}' in cluster '${acl.cluster}' to Kafka", e)
            throw e
        }
      }
    }
  }

  def deleteKafkaAcl(acl: Acl) = {
    val topicName = acl.topicName
    val username = acl.user
    val role = acl.role.operation
    val topicResourcePattern = new ResourcePatternFilter(ResourceType.TOPIC, topicName, PatternType.LITERAL)
    val groupResourcePattern = new ResourcePatternFilter(ResourceType.GROUP, "*", PatternType.LITERAL)
    val accessControlEntry = new AccessControlEntryFilter(s"User:$username", "*", role, AclPermissionType.ALLOW)
    val topicAclBinding = new AclBindingFilter(topicResourcePattern, accessControlEntry)
    val groupAclBinding = new AclBindingFilter(groupResourcePattern, accessControlEntry)

    val adminClient = util.getAdminClient(acl.cluster)
    val aclCreationResponse = Try(adminClient.deleteAcls(List(topicAclBinding, groupAclBinding).asJava).all()
      .get(500, TimeUnit.MILLISECONDS))
    adminClient.close()
    aclCreationResponse.get
  }
}
