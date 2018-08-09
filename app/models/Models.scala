package models

import play.api.libs.json._
import play.api.libs.json.Reads._
import play.api.libs.functional.syntax._

object Models {
  case class TopicConfig(cleanupPolicy: Option[String], partitions: Option[Int], retentionMs: Option[Long], replicas: Option[Int])
  case class Topic(name: String, description: String, organization: String, config: TopicConfig)

  val topicConfigReads: Reads[TopicConfig] = (
    (JsPath \ "cleanup.policy").readNullable[String] and
    (JsPath \ "partitions").readNullable[Int] and
    (JsPath \ "retention.ms").readNullable[Long] and
    (JsPath \ "replicas").readNullable[Int]
  )(TopicConfig.apply _)

  val topicConfigWrites: Writes[TopicConfig] = (
    (JsPath \ "cleanup.policy").writeNullable[String] and
    (JsPath \ "partitions").writeNullable[Int] and
    (JsPath \ "retention.ms").writeNullable[Long] and
    (JsPath \ "replicas").writeNullable[Int]
  )(unlift(TopicConfig.unapply))

  implicit val topicConfigFormat: Format[TopicConfig] = Format(topicConfigReads, topicConfigWrites)
  implicit val topicFormat: Format[Topic] = Json.format[Topic]
}
