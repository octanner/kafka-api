package services

import javax.inject.Inject
import play.api.libs.ws.WSClient
import play.api.{ Configuration, Logger }
import utils.Exceptions.{ ExternalServiceException, UndefinedResource }

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
          .getOrElse(throw ExternalServiceException("Failed Schema Registry Service Call"))
      }
  }

  private def getSchemaRegistryUrl(cluster: String): String = {
    conf.getOptional[String](cluster.toLowerCase + SCHEMA_REGISTRY_URL_CONFIG)
      .getOrElse(throw UndefinedResource(s"Undefined Schema Registry Location for cluster $cluster"))
  }
}

object SchemaRegistryService {
  val SCHEMA_REGISTRY_URL_CONFIG = ".kafka.avro.registry.location"
}
