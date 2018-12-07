package daos

import java.sql.Connection

import anorm.SqlParser.{ get, str }
import anorm._
import models.AclRole
import models.AclRole.AclRole
import models.Models.{ Acl, AclCredentials }
import models.http.HttpModels.AclRequest
import utils.Exceptions.InvalidAclRoleException

class AclDao {
  import AclDao._

  def getUnclaimedAcl(cluster: String)(implicit conn: Connection): AclCredentials = {
    SQL"""
          SELECT username, password, cluster FROM acl_source WHERE cluster = $cluster AND claimed = false LIMIT 1;
      """.as(aclCredentialsParser.single)
  }

  def getCredentials(user: String)(implicit conn: Connection): Option[AclCredentials] = {
    SQL"""
          SELECT username, password, cluster FROM acl_source WHERE username = $user AND claimed = true;
      """.as(aclCredentialsParser.singleOpt)
  }

  def claimUser(cluster: String, username: String)(implicit conn: Connection) = {
    SQL"""
          UPDATE acl_source SET claimed = TRUE, claimed_timestamp = now() WHERE cluster = $cluster AND username = $username;
      """.executeUpdate()
  }

  def unclaimUser(cluster: String, username: String)(implicit conn: Connection) = {
    SQL"""
          UPDATE acl_source SET claimed = false, claimed_timestamp = now() WHERE cluster = $cluster AND username = $username;
      """.executeUpdate()
  }

  def addPermissionToDb(cluster: String, aclRequest: AclRequest)(implicit conn: Connection) = {
    val topicId = getTopicIdByName(cluster, aclRequest.topic).getOrElse(throw new IllegalArgumentException(s"Topic '${aclRequest.topic}' not found in cluster '$cluster'"))
    val userId = getUserIdByName(cluster, aclRequest.user).getOrElse(throw new IllegalArgumentException(s"Username '${aclRequest.user}' not claimed in cluster '$cluster'"))
    val role = aclRequest.role.role
    val cg_name = aclRequest.consumerGroupName.getOrElse("*")
    SQL"""
          INSERT INTO acl (user_id, topic_id, role, cluster, cg_name) VALUES
          ($userId, $topicId, $role, $cluster, ${cg_name})
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

  def getAclsForTopic(cluster: String, topic: String)(implicit conn: Connection) = {
    SQL"""
          SELECT acl.acl_id as id, acl_source.username, topic, acl.cluster as cluster, acl.role, cg_name
          FROM acl
          INNER JOIN acl_source ON acl.user_id = acl_source.user_id
          INNER JOIN topic ON acl.topic_id = topic.topic_id
          WHERE acl.cluster = $cluster AND topic.topic = $topic;
      """.as(aclParser.*)
  }

  def getAclsForUsername(cluster: String, username: String)(implicit conn: Connection) = {
    SQL"""
          SELECT acl.acl_id as id, acl_source.username, topic, acl.cluster as cluster, acl.role, cg_name
          FROM acl
          INNER JOIN acl_source ON acl.user_id = acl_source.user_id
          INNER JOIN topic ON acl.topic_id = topic.topic_id
          WHERE acl.cluster = $cluster AND acl_source.username = $username;
      """.as(aclParser.*)
  }

  def getAcl(id: String)(implicit conn: Connection): Option[Acl] = {
    SQL"""
          SELECT acl.acl_id as id, username, topic, acl.cluster as cluster, role, cg_name
           FROM acl, topic, acl_source u
           WHERE acl.user_id = u.user_id AND
                 acl.topic_id = topic.topic_id AND
                 acl.acl_id = ${id}
      """.as(aclParser.singleOpt)
  }

  def getAclsByUsernameTopicClusterAndRole(acl: Acl)(implicit conn: Connection): List[Acl] = {
    SQL"""
          SELECT acl.acl_id as id, username, topic, acl.cluster as cluster, role, cg_name
           FROM acl, topic, acl_source u
           WHERE acl.user_id = u.user_id AND
                 acl.topic_id = topic.topic_id AND
                 topic.topic = ${acl.topic} AND
                 u.username = ${acl.user} AND
                 role = ${acl.role.role} AND
                 acl.cluster = ${acl.cluster}
      """.as(aclParser.*)
  }

  def deleteAcl(id: String)(implicit conn: Connection) = {
    SQL"""
        DELETE FROM ACL WHERE acl_id = ${id}
      """.execute()
  }

  implicit val aclCredentialsParser = Macro.parser[AclCredentials]("username", "password", "cluster")
  implicit val aclParser: RowParser[Acl] =
    (str("id") ~ str("username") ~ str("topic") ~ str("cluster") ~ aclRole("role") ~ str("cg_name")) map {
      case id ~ username ~ topic ~ cluster ~ role ~ cgName =>
        cgName match {
          case "*" => Acl(id, username, topic, cluster, role, None)
          case cg  => Acl(id, username, topic, cluster, role, Some(cg))
        }
    }
  implicit val stringParser = SqlParser.scalar[String]
}

object AclDao {
  def aclRole(columnName: String)(implicit c: Column[AclRole]): RowParser[AclRole] = get[AclRole](columnName)(c)
  implicit val aclRoleColumnParser: Column[AclRole] = Column.nonNull { (value, _) =>
    value match {
      case role: String => Right(AclRole.get(role).getOrElse(throw InvalidAclRoleException(s"role `$role` for ACL is not valid")))
    }
  }
}
