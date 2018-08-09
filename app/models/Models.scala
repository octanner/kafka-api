package models

import play.api.libs.json._
import play.api.libs.json.Reads._
import play.api.libs.functional.syntax._

object Models {
  case class TopicConfiguration(cleanupPolicy: Option[String], partitions: Option[Int], retentionMs: Option[Long], replicas: Option[Int])
  case class Topic(name: String, description: String, organization: String, config: TopicConfiguration)

  val topicConfigReads: Reads[TopicConfiguration] = (
    (JsPath \ "cleanup.policy").readNullable[String] and
    (JsPath \ "partitions").readNullable[Int] and
    (JsPath \ "retention.ms").readNullable[Long] and
    (JsPath \ "replicas").readNullable[Int]
  )(TopicConfiguration.apply _)

  val topicConfigWrites: Writes[TopicConfiguration] = (
    (JsPath \ "cleanup.policy").writeNullable[String] and
    (JsPath \ "partitions").writeNullable[Int] and
    (JsPath \ "retention.ms").writeNullable[Long] and
    (JsPath \ "replicas").writeNullable[Int]
  )(unlift(TopicConfiguration.unapply))

  implicit val topicConfigFormat: Format[TopicConfiguration] = Format(topicConfigReads, topicConfigWrites)
  implicit val topicFormat: Format[Topic] = Json.format[Topic]
}
