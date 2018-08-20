package controllers

import javax.inject._
import play.api.Logger
import play.api.libs.json.Json
import play.api.mvc._

@Singleton
class HealthController @Inject() extends InjectedController {

  val logger = Logger(this.getClass)

  def health = Action {
    logger.info(s"Healthcheck was pinged.")
    Ok(Json.obj("status" -> "App is running."))
  }
}
