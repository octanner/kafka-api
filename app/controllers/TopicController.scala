package controllers

import javax.inject.Inject
import models.http.HttpModels._
import play.api.Logger
import play.api.libs.json.Json
import play.api.mvc.{ InjectedController, Result }
import services.TopicService

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class TopicController @Inject() (service: TopicService) extends InjectedController with RequestProcessor {
  val logger = Logger(this.getClass)

  def create(cluster: String) = Action.async { implicit request =>
    processRequest[TopicRequest](createTopic(cluster))
  }

  private def createTopic(cluster: String)(topic: TopicRequest): Future[Result] = {
    service.createTopic(cluster, topic.topic).map { topic =>
      Ok(Json.toJson(TopicResponse(topic)))
    }
  }
}
