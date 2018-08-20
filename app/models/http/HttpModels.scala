package models.http

import models.AclRoleEnum
import models.Models.{ AclCredentials, Topic }
import play.api.libs.json._

import scala.util.{ Failure, Success, Try }

trait HttpRequest
sealed trait HttpResponse

object HttpModels {
  final case class Error(title: String, detail: String)
  final case class ErrorResponse(errors: Seq[Error])

  final case class TopicRequest(topic: Topic) extends HttpRequest
  final case class TopicResponse(topic: Topic) extends HttpResponse
  final case class AclRequest(topic: String, user: String, role: AclRoleEnum) extends HttpRequest
  final case class AclResponse(aclCredentials: AclCredentials) extends HttpResponse

  implicit val aclRoleEnumFormat = new Format[AclRoleEnum] {
    def reads(json: JsValue): JsResult[AclRoleEnum] = Try(AclRoleEnum.valueOf(json.as[String].toUpperCase)) match {
      case Success(role) => JsSuccess(role)
      case Failure(e)    => JsError(s"Invalid role, expected ${AclRoleEnum.values.mkString(", ")}")
    }
    def writes(role: AclRoleEnum) = JsString(role.toString)
  }

  implicit val topicRequestFormat = Json.format[TopicRequest]
  implicit val topicResponseFormat = Json.format[TopicResponse]
  implicit val aclRequestFormat = Json.format[AclRequest]
  implicit val aclResponseFormat = Json.format[AclResponse]
  implicit val errorFormat = Json.format[Error]
  implicit val errorRespFormat = Json.format[ErrorResponse]

  def createErrorResponse(title: String, message: String): ErrorResponse = ErrorResponse(Seq(Error(title, message)))
}
