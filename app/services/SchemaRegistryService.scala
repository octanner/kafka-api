package services

import javax.inject.Inject
import models.http.HttpModels.SchemaResponse
import play.api.libs.ws.WSClient
import play.api.{ Configuration, Logger }
import utils.Exceptions.{ ExternalServiceException, ResourceNotFoundException, UndefinedResourceException }

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class SchemaRegistryService @Inject() (conf: Configuration, ws: WSClient) extends HttpResponseProcessor {
  import SchemaRegistryService._
  val logger = Logger(this.getClass)

  def getSchemas(cluster: String): Future[List[String]] = {
    val schemaRegistryUrl = getSchemaRegistryUrl(cluster)
    ws.url(s"$schemaRegistryUrl/subjects")
      .get()
      .map { response =>
        processGetResponse[List[String]](response, "Failed Schema Registry Get Schemas Service Call")
          .getOrElse(throw ExternalServiceException("Failed Schema Registry Service Call"))
      }
  }

  def getSchemaVersions(cluster: String, schema: String): Future[List[Int]] = {
    val schemaRegistryUrl = getSchemaRegistryUrl(cluster)
    ws.url(s"$schemaRegistryUrl/subjects/${schema}/versions")
      .get()
      .map { response =>
        processGetResponse[List[Int]](response, "Failed Schema Registry Get Schema Versions Service Call")
          .getOrElse(throw ResourceNotFoundException(s"Schema with subject `$schema` not found"))
      }
  }

  def getSchema(cluster: String, schema: String): Future[SchemaResponse] = {
    val schemaRegistryUrl = getSchemaRegistryUrl(cluster)
    ws.url(s"$schemaRegistryUrl/subjects/${schema}/versions/latest")
      .get()
      .map { response =>
        processGetResponse[SchemaResponse](response, "Failed Schema Registry Get Schema Service Call")
          .getOrElse(throw ResourceNotFoundException(s"Schema not found for `$schema`"))
      }
  }

  private def getSchemaRegistryUrl(cluster: String): String = {
    conf.getOptional[String](cluster.toLowerCase + SCHEMA_REGISTRY_URL_CONFIG)
      .getOrElse(throw UndefinedResourceException(s"Undefined Schema Registry Location for cluster $cluster"))
  }
}

object SchemaRegistryService {
  val SCHEMA_REGISTRY_URL_CONFIG = ".kafka.avro.registry.location"
}
