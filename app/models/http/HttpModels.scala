package models.http

import models.AclRole.AclRole
import models.KeyType.KeyType
import models.Models.{ AclCredentials, Topic }
import play.api.libs.json._

trait HttpRequest
sealed trait HttpResponse

object HttpModels {
  final case class Error(title: String, detail: String)
  final case class ErrorResponse(errors: Seq[Error])

  final case class TopicRequest(topic: Topic) extends HttpRequest
  final case class TopicResponse(topic: Topic) extends HttpResponse
  final case class AclRequest(topic: String, user: String, role: AclRole) extends HttpRequest
  final case class AclResponse(aclCredentials: AclCredentials) extends HttpResponse
  final case class SchemaRequest(name: String) extends HttpRequest
  final case class SchemaResponse(subject: String, version: Int, schema: String) extends HttpRequest
  final case class TopicSchemaMapping(topic: String, schema: SchemaRequest) extends HttpRequest
  final case class TopicKeyMappingRequest(topic: String, keyType: KeyType, schema: Option[SchemaRequest]) extends HttpRequest

  implicit val topicRequestFormat = Json.format[TopicRequest]
  implicit val topicResponseFormat = Json.format[TopicResponse]
  implicit val aclRequestFormat = Json.format[AclRequest]
  implicit val aclResponseFormat = Json.format[AclResponse]
  implicit val schemaRequestFormat = Json.format[SchemaRequest]
  implicit val schemaResponseFormat = Json.format[SchemaResponse]
  implicit val topicSchemaMappingFormat = Json.format[TopicSchemaMapping]
  implicit val topicKeyMappingRequestFormat = Json.format[TopicKeyMappingRequest]
  implicit val errorFormat = Json.format[Error]
  implicit val errorRespFormat = Json.format[ErrorResponse]

  def createErrorResponse(title: String, message: String): ErrorResponse = ErrorResponse(Seq(Error(title, message)))
}
