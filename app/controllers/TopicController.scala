package controllers

import javax.inject.Inject
import models.KeyType
import models.http.HttpModels.{ TopicKeyMappingRequest, TopicRequest, TopicResponse, TopicSchemaMapping }
import play.api.Logger
import play.api.libs.json.Json
import play.api.mvc.{ InjectedController, Result }
import services.{ SchemaRegistryService, TopicService }
import utils.Exceptions.{ InvalidRequestException, ResourceNotFoundException }

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{ Failure, Success }

class TopicController @Inject() (service: TopicService, schemaService: SchemaRegistryService) extends InjectedController with RequestProcessor {
  val logger = Logger(this.getClass)
  val VALID = "VALID"
  val INVALID = "INVALID"
  val DUPLICATE = "DUPLICATE"

  def create(cluster: String) = Action.async { implicit request =>
    processRequest[TopicRequest](createTopic(cluster))
  }

  private def createTopic(cluster: String)(topic: TopicRequest): Future[Result] = {
    service.createTopic(cluster, topic.topic).map { topic =>
      Ok(Json.toJson(TopicResponse(topic)))
    }
  }

  def getTopicInfo(topicName: String) = Action.async { implicit request =>
    for {
      topicOpt <- service.getTopic(topicName)
      schemaMappings <- service.getTopicSchemaMappings(topicName)
    } yield {

      topicOpt match {
        case Some(topic) =>
          schemaMappings match {
            case List() => Ok(Json.toJson(TopicResponse(topic)))
            case mappings =>
              Ok(Json.toJson(TopicResponse(topic.copy(schemas = Some(mappings)))))
          }
        case None => NotFound(s"Cannot find topic '$topicName'")
      }
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

  def getAllConfigSets(cluster: String) = Action.async { implicit request =>
    service.getAllConfigSets(cluster).map { configs => Ok(Json.obj("configs" -> configs)) }
  }

  def getConfigSet(cluster: String, name: String) = Action.async { implicit request =>
    service.getConfigSet(cluster, name).map { config => Ok(Json.obj("config" -> config)) }
  }

  def deleteTopic(cluster: String, topic: String) = Action.async { implicit request =>
    service.deleteTopic(cluster, topic).map { _ =>
      Ok
    }
  }

  private def createTopicSchemaMapping(cluster: String)(mapping: TopicSchemaMapping): Future[Result] = {
    for { isValidRequest <- validateTopicSchemaMappingRequest(cluster, mapping) } yield {
      if (isValidRequest == VALID)
        service.createTopicSchemaMapping(cluster, mapping).map { m => Ok(Json.toJson(m)) }
      else if (isValidRequest == DUPLICATE)
        Future.successful(Ok(Json.toJson(mapping)))
      else
        throw InvalidRequestException(s"Failed to validate the schema `${mapping.schema}` in cluster `$cluster`")
    }
  }.flatten

  private def validateTopicSchemaMappingRequest(cluster: String, mapping: TopicSchemaMapping): Future[String] = {
    for {
      topicOpt <- service.getTopic(mapping.topic)
      schemaMappings <- service.getTopicSchemaMappings(cluster, mapping.topic)
    } yield {
      val topic = topicOpt.getOrElse(throw ResourceNotFoundException(s"Topic `${mapping.topic}` does not exist"))
      if (schemaMappings.map(_.schema.name).contains(mapping.schema.name)) {
        logger.info(s"Duplicate request : topic `${mapping.topic}` is already mapped to schemas `${schemaMappings.map(_.schema.name).mkString(",")}`")
        Future.successful(DUPLICATE)
      } else if ((topic.config.name == "ledger" || topic.config.name == "state") && !schemaMappings.isEmpty) {
        throw InvalidRequestException(s"Cannot map schema `${mapping.schema.name}`. " +
          s"""Topic `${mapping.topic}` is already mapped to schemas `${schemaMappings.map(_.schema.name).mkString(",")}`. """ +
          s"Ledger or State type topic cannot have more than one schema mappings.")
      } else {
        validateSchema(cluster, mapping.schema.name)
      }
    }
  }.flatten

  private def createTopicKeyMapping(cluster: String)(mapping: TopicKeyMappingRequest): Future[Result] = {
    for { isValidRequest <- validateTopicKeyMappingRequest(cluster, mapping) } yield {
      if (isValidRequest == VALID)
        service.createTopicKeyMapping(cluster, mapping).map(m => Ok(Json.toJson(m)))
      else
        throw InvalidRequestException(s"Failed to validate the schema `${mapping.schema.map(_.name)}` in cluster `$cluster`")
    }
  }.flatten

  private def validateTopicKeyMappingRequest(cluster: String, topicKeyMappingRequest: TopicKeyMappingRequest): Future[String] = {
    for {
      topicOpt <- service.getTopic(topicKeyMappingRequest.topic)
    } yield {
      val topic = topicOpt.getOrElse(throw ResourceNotFoundException(s"Topic `${topicKeyMappingRequest.topic}` does not exist"))
      if (topicKeyMappingRequest.keyType == KeyType.AVRO) {
        val schema = topicKeyMappingRequest.schema
          .getOrElse(throw InvalidRequestException("schema needs to be defined for key type `AVRO`"))
        validateSchema(cluster, schema.name)
      } else if (topic.config.name == "state" && topicKeyMappingRequest.keyType == KeyType.NONE) {
        throw InvalidRequestException("Compact topic cannot be mapped to `NONE` keytype")
      } else {
        Future(VALID)
      }
    }
  }.flatten

  private def validateSchema(cluster: String, schema: String): Future[String] = {
    schemaService.getSchema(cluster, schema).transform {
      case Success(_) => Success(VALID)
      case Failure(e) =>
        logger.error(e.getMessage, e)
        Success(INVALID)
    }
  }
}
