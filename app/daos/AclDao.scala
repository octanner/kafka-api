package daos

import java.sql.Connection

import anorm._
import models.Models.AclCredentials
import models.http.HttpModels.AclRequest

class AclDao {

  def getUnclaimedAcl(cluster: String)(implicit conn: Connection): AclCredentials = {
    SQL"""
          SELECT username, password FROM acl_source WHERE cluster = $cluster AND claimed = false LIMIT 1;
      """.as(aclCredentialsParser.single)
  }

  def claimAcl(cluster: String, username: String)(implicit conn: Connection) = {
    SQL"""
          UPDATE acl_source SET claimed = TRUE, claimed_timestamp = now() WHERE cluster = $cluster AND username = $username;
       """.executeUpdate()
  }

  def addPermissionToDb(cluster: String, aclRequest: AclRequest)(implicit conn: Connection) = {
    val topicId = getTopicIdByName(cluster, aclRequest.topic).getOrElse(throw new IllegalArgumentException(s"Topic '${aclRequest.topic}' not found in cluster '$cluster'"))
    val userId = getUserIdByName(cluster, aclRequest.user).getOrElse(throw new IllegalArgumentException(s"Username '${aclRequest.user}' not claimed in cluster '$cluster'"))
    val role = aclRequest.role.name
    SQL"""
          INSERT INTO acl (user_id, topic_id, role, cluster) VALUES ($userId, $topicId, $role, $cluster)
          ON CONFLICT ON CONSTRAINT acl_unique DO UPDATE SET topic_id = acl.topic_id;
       """.executeInsert(stringParser.single)
  }

  def getUserIdByName(cluster: String, username: String)(implicit conn: Connection): Option[String] = {
    SQL"""
          SELECT user_id FROM acl_source WHERE cluster = $cluster AND username = $username AND claimed = true;
      """.as(stringParser.singleOpt)
  }

  def getTopicIdByName(cluster: String, topic: String)(implicit conn: Connection): Option[String] = {
    SQL"""
          SELECT topic_id FROM topic WHERE cluster = $cluster AND topic = $topic;
      """.as(stringParser.singleOpt)
  }

  implicit val aclCredentialsParser = Macro.parser[AclCredentials]("username", "password")
  implicit val stringParser = SqlParser.scalar[String]

}
