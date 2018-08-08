package models.http

import models.Models.Topic
import play.api.libs.json.Json

trait HttpRequest
sealed trait HttpResponse

object HttpModels {
  final case class Error(title: String, detail: String)
  final case class ErrorResponse(errors: Seq[Error])

  final case class TopicRequest(topic: Topic) extends HttpRequest

  implicit val topicRequestFormat = Json.format[TopicRequest]

  def createErrorResponse(title: String, message: String): ErrorResponse = ErrorResponse(Seq(Error(title, message)))
}
