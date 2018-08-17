package services

import daos.AclDao
import javax.inject.Inject
import models.AclRoleEnum
import models.http.HttpModels.AclRequest
import org.apache.kafka.common.acl._
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.resource.{PatternType, ResourcePattern, ResourceType}
import org.postgresql.util.PSQLException
import play.api.Logger
import play.api.db.Database
import utils.AdminClientUtil

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

class AclService @Inject() (db: Database, dao: AclDao, util: AdminClientUtil) {

  val logger = Logger(this.getClass)

  def claimAcl(cluster: String) = {
    Future {
      db.withTransaction { implicit conn =>
        val aclCredentials = dao.getUnclaimedAcl(cluster)
        Try(dao.claimAcl(cluster, aclCredentials.username)) match {
          case Success(_) => aclCredentials
          case Failure(e) =>
            logger.error(s"Failed to claim ACL in database", e)
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
        case Failure(e: PSQLException) =>
          logger.error(s"Failed to add permission to DB", e)
          throw e
        case Failure(e) =>
          logger.error(s"Failed to create permission in DB", e)
          throw e
      }
    }
  }

  def addPermissionToKafka(cluster: String, aclRequest: AclRequest) = {
    Try(createKafkaAcl(cluster, aclRequest)) match {
      case Success(_) =>
        logger.info(s"Successfully added permission for ${aclRequest.user} with role ${aclRequest.role} " +
          s"on topic ${aclRequest.topic} in cluster $cluster to Kafka")
      case Failure(e: InvalidRequestException) =>
        logger.error(e.getMessage, e)
        throw e
      case Failure(e) =>
        println("Generic failure")
        logger.error(s"Unable to add permission for ${aclRequest.user} with role ${aclRequest.role} " +
          s"on topic ${aclRequest.topic} in cluster $cluster to Kafka", e)
        throw e
    }
  }

  def createKafkaAcl(cluster: String, aclRequest: AclRequest) = {
    val topic = aclRequest.topic
    val username = aclRequest.user
    val role = aclRequest.role
    val operation = Try(AclRoleEnum.valueOf(role.toUpperCase).getOperation) match {
      case Success(op)                          => op
      case Failure(e: IllegalArgumentException) => throw new IllegalArgumentException(s"Role $role not defined")
    }
    val resourcePattern = new ResourcePattern(ResourceType.TOPIC, topic, PatternType.LITERAL)
    val accessControlEntry = new AccessControlEntry(s"User:$username", topic, operation, AclPermissionType.ALLOW)
    val aclBinding = new AclBinding(resourcePattern, accessControlEntry)

    val adminClient = util.getAdminClient(cluster)
    val aclCreationResponse = Try(adminClient.createAcls(List(aclBinding).asJava).all().get)
    adminClient.close()
    aclCreationResponse.get
  }

}
