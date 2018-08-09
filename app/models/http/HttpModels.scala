package models.http

import models.Models.Topic
import play.api.libs.json.Json

trait HttpRequest
sealed trait HttpResponse

object HttpModels {
  final case class Error(title: String, detail: String)
  final case class ErrorResponse(errors: Seq[Error])

  final case class TopicRequest(topic: Topic) extends HttpRequest
  final case class TopicResponse(topic: Topic) extends HttpResponse

  implicit val topicRequestFormat = Json.format[TopicRequest]
  implicit val topicResponseFormat = Json.format[TopicResponse]
  implicit val errorFormat = Json.format[Error]
  implicit val errorRespFormat = Json.format[ErrorResponse]

  def createErrorResponse(title: String, message: String): ErrorResponse = ErrorResponse(Seq(Error(title, message)))
}
