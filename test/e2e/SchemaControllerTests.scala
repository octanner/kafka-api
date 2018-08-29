package e2e

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfterEach
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.mockito.MockitoSugar
import play.api.Configuration
import play.api.inject.bind
import play.api.inject.guice.GuiceableModule
import play.api.libs.json.{ JsValue, Json }
import play.api.libs.ws.{ WSClient, WSRequest, WSResponse }
import play.api.mvc.Results._

import scala.concurrent.Future

class SchemaControllerTests extends IntTestSpec with BeforeAndAfterEach with MockitoSugar with ScalaFutures {
  val mockWs = mock[WSClient]

  val cluster = "test"

  override def modulesToOverride: Seq[GuiceableModule] = Seq(
    bind[WSClient].toInstance(mockWs)
  )

  def setMockRequestResponseExpectations(url: String, status: Int, json: JsValue = Json.parse("{}")): (WSRequest, WSResponse) = {
    val mockReq = mock[WSRequest]
    val mockResp = mock[WSResponse]

    when(mockWs.url(url)) thenReturn mockReq
    when(mockReq.withHttpHeaders(any[(String, String)])) thenReturn mockReq
    when(mockReq.get()) thenReturn Future.successful(mockResp)
    when(mockResp.status) thenReturn status
    when(mockResp.json) thenReturn json
    (mockReq, mockResp)
  }

  "SchemaController #getSchemas" must {
    "return Ok and return schemas for the cluster" in {
      val conf = app.injector.instanceOf[Configuration]
      val schemas = List("schema1", "schema2")
      val url = s"${conf.get[String](cluster.toLowerCase + ".kafka.avro.registry.location")}/subjects"
      val resp = Json.toJson(schemas)
      setMockRequestResponseExpectations(url, 200, resp)

      val result = wsUrl(s"/v1/kafka/cluster/$cluster/schemas").get().futureValue
      println(s"${result.status}; ${result.body}")
      Status(result.status) mustBe Ok
      result.json mustBe Json.obj("schemas" -> schemas)
    }
  }

  "SchemaController #getSchemaVersions" must {
    "return Ok and return schema versions for the cluster and schema name" in {
      val conf = app.injector.instanceOf[Configuration]
      val schema = "schema1"
      val versions = List(1, 2, 3)
      val url = s"${conf.get[String](cluster.toLowerCase + ".kafka.avro.registry.location")}/subjects/$schema/versions"
      val resp = Json.toJson(versions)
      setMockRequestResponseExpectations(url, 200, resp)

      val result = wsUrl(s"/v1/kafka/cluster/$cluster/schemas/$schema/versions").get().futureValue
      println(s"${result.status}; ${result.body}")
      Status(result.status) mustBe Ok
      result.json mustBe Json.obj("versions" -> versions)
    }
  }

}
