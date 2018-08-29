package services

import play.api.Logger
import play.api.http.Status
import play.api.libs.json.{ JsResult, JsSuccess, Reads }
import play.api.libs.ws.WSResponse
import utils.Exceptions.ExternalServiceException

trait HttpResponseProcessor {
  val logger: Logger

  def processGetResponse[T: Reads](
    response:   WSResponse,
    failureMsg: String     = "Failed Service Call"): Option[T] = {
    response.status match {
      case Status.OK        => process200Response(response, failureMsg)
      case Status.NOT_FOUND => None
      case _ => {
        logger.error(s"$failureMsg : ${response.status}: ${response.body}")
        throw ExternalServiceException("A service call failed")
      }
    }
  }

  private def process200Response[T: Reads](response: WSResponse, failureMsg: String = "Failed"): Option[T] = {
    val result: JsResult[T] = (response.json).validate[T]
    result match {
      case success: JsSuccess[T] => Some(success.get)
      case _ => {
        logger.error(s"$failureMsg. Cannot Validate Response : ${response.status}: ${response.body}")
        throw ExternalServiceException("A service call failed")
      }
    }
  }
}
