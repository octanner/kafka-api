package controllers

import javax.inject.Inject
import play.api.Logger
import play.api.libs.json.Json
import play.api.mvc.InjectedController
import services.SchemaRegistryService

import scala.concurrent.ExecutionContext.Implicits.global

class SchemaController @Inject() (service: SchemaRegistryService) extends InjectedController {
  val logger = Logger(this.getClass)
  def getSchemas(cluster: String) = Action.async { implicit request =>
    service.getSchemas(cluster).map { schemas =>
      Ok(Json.obj("schemas" -> schemas))
    }
  }

  def getSchemaVersions(cluster: String, schema: String) = Action.async { implicit request =>
    service.getSchemaVersions(cluster, schema).map { versions =>
      Ok(Json.obj("versions" -> versions))
    }
  }

  def getSchema(cluster: String, schema: String) = Action.async { implicit request =>
    service.getSchema(cluster, schema).map { schemaResponse =>
      Ok(Json.toJson(schemaResponse))
    }
  }
}
