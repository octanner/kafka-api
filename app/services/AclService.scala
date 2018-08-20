package services

import daos.AclDao
import javax.inject.Inject
import models.http.HttpModels.AclRequest
import org.apache.kafka.common.acl._
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.resource.{ PatternType, ResourcePattern, ResourceType }
import play.api.Logger
import play.api.db.Database
import utils.AdminClientUtil

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{ Failure, Success, Try }

class AclService @Inject() (db: Database, dao: AclDao, util: AdminClientUtil) {

  val logger = Logger(this.getClass)

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
        case Success(_) =>
          addPermissionToKafka(cluster, aclRequest)
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
    val role = aclRequest.role.getOperation
    val resourcePattern = new ResourcePattern(ResourceType.TOPIC, topicName, PatternType.LITERAL)
    val accessControlEntry = new AccessControlEntry(s"User:$username", topicName, role, AclPermissionType.ALLOW)
    val aclBinding = new AclBinding(resourcePattern, accessControlEntry)

    val adminClient = util.getAdminClient(cluster)
    val aclCreationResponse = Try(adminClient.createAcls(List(aclBinding).asJava).all().get)
    adminClient.close()
    aclCreationResponse.get
  }

}
