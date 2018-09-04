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

  def getTopicInfo(topicName: String) = Action.async { implicit request =>
    service.getTopic(topicName).map {
      case Some(topic) => Ok(Json.toJson(TopicResponse(topic)))
      case None        => NotFound(s"Cannot find topic '$topicName'")
    }
  }

  def getAllTopics = Action.async { implicit request =>
    service.getAllTopics.map { topics =>
      Ok(Json.obj("topics" -> topics))
    }
  }

  def getTopicSchemaMappings(cluster: String, topic: String) = Action.async { implicit request =>
    service.getTopicSchemaMappings(cluster, topic).map { mappings =>
      Ok(Json.obj("mappings" -> mappings))
    }
  }

  def createSchemaMapping(cluster: String) = Action.async { implicit request =>
    processRequest[TopicSchemaMapping](createTopicSchemaMapping(cluster))
  }

  private def createTopicSchemaMapping(cluster: String)(mapping: TopicSchemaMapping): Future[Result] = {
    service.createTopicSchemaMapping(cluster, mapping).map { m => Ok(Json.toJson(m)) }
  }

}
