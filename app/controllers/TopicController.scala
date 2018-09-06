package controllers

import javax.inject.Inject
import models.KeyType
import models.http.HttpModels.{ TopicKeyMappingRequest, TopicRequest, TopicResponse, TopicSchemaMapping }
import play.api.Logger
import play.api.libs.json.Json
import play.api.mvc.{ InjectedController, Result }
import services.{ SchemaRegistryService, TopicService }
import utils.Exceptions.InvalidRequestException

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{ Failure, Success }

class TopicController @Inject() (service: TopicService, schemaService: SchemaRegistryService) extends InjectedController with RequestProcessor {
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

  def createKeyMapping(cluster: String) = Action.async { implicit request =>
    processRequest[TopicKeyMappingRequest](createTopicKeyMapping(cluster))
  }

  private def createTopicSchemaMapping(cluster: String)(mapping: TopicSchemaMapping): Future[Result] = {
    service.createTopicSchemaMapping(cluster, mapping).map { m => Ok(Json.toJson(m)) }
  }

  private def createTopicKeyMapping(cluster: String)(mapping: TopicKeyMappingRequest): Future[Result] = {
    for { isValidRequest <- validateTopicKeyMappingRequest(cluster, mapping) } yield {
      if (isValidRequest)
        service.createTopicKeyMapping(cluster, mapping).map(_ => Ok)
      else
        throw InvalidRequestException(s"Failed to validate the schema `${mapping.schema}` in cluster `$cluster`")
    }
  }.flatMap(f => f)

  private def validateTopicKeyMappingRequest(cluster: String, topicKeyMappingRequest: TopicKeyMappingRequest): Future[Boolean] = {
    if (topicKeyMappingRequest.keyType == KeyType.AVRO) {
      val schema = topicKeyMappingRequest.schema.getOrElse(throw InvalidRequestException("schema needs to be defined for key type `AVRO`"))
      schemaService.getSchema(cluster, schema.name, schema.version).transform {
        case Success(_) => Success(true)
        case Failure(e) =>
          logger.error(e.getMessage, e)
          Success(false)
      }
    } else {
      Future(true)
    }
  }

}
