package models

import models.AclRole.AclRole
import models.KeyType.KeyType
import play.api.libs.functional.syntax._
import play.api.libs.json.Reads._
import play.api.libs.json._

object Models {
  case class TopicConfiguration(name: String, cleanupPolicy: Option[String], partitions: Option[Int], retentionMs: Option[Long], replicas: Option[Int])
  case class TopicKeyType(keyType: KeyType, schema: Option[String])
  case class Topic(name: String, config: TopicConfiguration, keyMapping: Option[TopicKeyType] = None, schemas: Option[List[String]] = None, cluster: Option[String] = None)
  case class BasicTopicInfo(id: String, name: String, cluster: String)
  case class AclCredentials(username: String, password: String)
  case class Acl(id: String, user: String, topic: String, cluster: String, role: AclRole)
  case class TopicKeyMapping(topicId: String, keyType: KeyType, schema: Option[String])

  val topicConfigReads: Reads[TopicConfiguration] = (
    (JsPath \ "name").read[String] and
    (JsPath \ "cleanup.policy").readNullable[String] and
    (JsPath \ "partitions").readNullable[Int] and
    (JsPath \ "retention.ms").readNullable[Long] and
    (JsPath \ "replicas").readNullable[Int]
  )(TopicConfiguration.apply _)

  val topicConfigWrites: Writes[TopicConfiguration] = (
    (JsPath \ "name").write[String] and
    (JsPath \ "cleanup.policy").writeNullable[String] and
    (JsPath \ "partitions").writeNullable[Int] and
    (JsPath \ "retention.ms").writeNullable[Long] and
    (JsPath \ "replicas").writeNullable[Int]
  )(unlift(TopicConfiguration.unapply))

  implicit val topicConfigFormat: Format[TopicConfiguration] = Format(topicConfigReads, topicConfigWrites)
  implicit val topicKeyTypeFormat: Format[TopicKeyType] = Json.format[TopicKeyType]
  implicit val topicFormat: Format[Topic] = Json.format[Topic]
  implicit val aclCredentialsFormat: Format[AclCredentials] = Json.format[AclCredentials]
  implicit val aclFormat: Format[Acl] = Json.format[Acl]
  implicit val basicTopicInfoFormat: Format[BasicTopicInfo] = Json.format[BasicTopicInfo]
  implicit val topicKeyMappingFormat: Format[TopicKeyMapping] = Json.format[TopicKeyMapping]
}
