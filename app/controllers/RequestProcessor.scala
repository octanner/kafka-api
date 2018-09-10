package controllers

import models.http.HttpModels._
import models.http.HttpRequest
import play.api.Logger
import play.api.libs.json._
import play.api.mvc.{ AnyContent, InjectedController, Request, Result }

import scala.concurrent.Future

/**
 * Mix-in trait for controllers that need to process request body.
 */
trait RequestProcessor {
  this: InjectedController =>

  val logger: Logger

  def processRequest[A <: HttpRequest](action: A => Future[Result], statusFn: Status = BadRequest)(implicit request: Request[AnyContent], reader: Reads[A]): Future[Result] = {
    request.body.asJson.map { jsValue =>
      jsValue.validate[A].fold(
        (errors: Seq[(JsPath, Seq[JsonValidationError])]) => {
          logger.error(s"Validation failed for request ${request.body.toString} with ${JsError.toJson(errors)}")
          Future.successful(statusFn(Json.toJson(createErrorResponse("Bad Request", "Request body does not conform to API spec."))))
        },
        parsedRequest => action(parsedRequest)
      )
    }.getOrElse(Future.successful(statusFn(Json.toJson(createErrorResponse("Bad Request", "Request body was not parsable as JSON")))))
  }
}
