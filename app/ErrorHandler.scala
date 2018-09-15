import javax.inject.Singleton
import models.http.HttpModels._
import org.apache.kafka.common.errors.InvalidReplicationFactorException
import play.api.Logger
import play.api.http.HttpErrorHandler
import play.api.libs.json.Json
import play.api.mvc.Results._
import play.api.mvc._
import utils.Exceptions._

import scala.concurrent._

/**
 * Top-level error handler for both client (HTTP status code 4xx) and server (HTTP status code 5xx) errors.
 *
 * This class should be the only class responsible for determining the JSON representation of errors.
 * That is, make the appropriate changes here to control the format of the JSON representation of errors.
 */
@Singleton
class ErrorHandler extends HttpErrorHandler {
  val logger = Logger(this.getClass)

  def onClientError(request: RequestHeader, statusCode: Int, message: String) = {
    val status = Status(statusCode)
    val title = status match {
      case BadRequest => "Bad Request"
      case _          => s"Client Error - $statusCode"
    }

    Future.successful(
      status(Json.toJson(createErrorResponse(title, message)))
    )
  }

  def onServerError(request: RequestHeader, exception: Throwable) = {
    logger.error(exception.getMessage, exception)
    val status = exception match {
      case e: NonUniqueTopicNameException =>
        Conflict(Json.toJson(createErrorResponse(e.title, e.message)))
      case e: InvalidReplicationFactorException =>
        BadRequest(Json.toJson(createErrorResponse("Invalid Topic Replicas", e.getMessage)))
      case e: InvalidUserException =>
        BadRequest(Json.toJson(createErrorResponse(e.title, e.message)))
      case e: InvalidAclRoleException =>
        BadRequest(Json.toJson(createErrorResponse(e.title, e.message)))
      case e: InvalidKeyTypeException =>
        BadRequest(Json.toJson(createErrorResponse(e.title, e.message)))
      case e: ResourceNotFoundException =>
        NotFound(Json.toJson(createErrorResponse(e.title, e.message)))
      case e: ResourceExistsException =>
        BadRequest(Json.toJson(createErrorResponse(e.title, e.message)))
      case e: UndefinedResourceException =>
        InternalServerError(Json.toJson(createErrorResponse(e.title, e.message)))
      case e: ExternalServiceException =>
        InternalServerError(Json.toJson(createErrorResponse(e.title, e.message)))
      case e: InvalidRequestException =>
        BadRequest(Json.toJson(createErrorResponse(e.title, e.message)))
      case e: Exception =>
        val message = "A server error occurred: " + exception.getMessage
        InternalServerError(Json.toJson(createErrorResponse("Internal Server Error", message)))
      case _ =>
        val message = "A server error occurred: " + exception.getMessage
        InternalServerError(Json.toJson(createErrorResponse("Internal Server Error", message)))
    }

    Future.successful(status)
  }
}
