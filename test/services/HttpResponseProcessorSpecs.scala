package services

import models.http.HttpModels.SchemaResponse
import org.mockito.Mockito.when
import org.scalatest.mockito.MockitoSugar
import org.scalatestplus.play.PlaySpec
import play.api.Logger
import play.api.libs.json.Json
import play.api.libs.ws.WSResponse
import utils.Exceptions.ExternalServiceException

class HttpResponseProcessorSpecs extends PlaySpec with HttpResponseProcessor with MockitoSugar {
  val logger = Logger(this.getClass)
  val mockResponse = mock[WSResponse]
  val schemaName = "test.schema1"
  val version = 1
  val schema = """{"type": "string"}"""
  val schemaResponse = SchemaResponse(schemaName, version, schema)

  "Process Get Response" must {
    "throw External Service Exception when response status is not 200 or 404" in {
      when(mockResponse.status) thenReturn 500
      an[ExternalServiceException] should be thrownBy processGetResponse[SchemaResponse](mockResponse)
    }

    "return None when response status is 404(Not Found)" in {
      when(mockResponse.status) thenReturn 404
      processGetResponse[SchemaResponse](mockResponse) mustBe None
    }

    "throw External Service Exception when response status is 200 and cannot be validated" in {
      when(mockResponse.status) thenReturn 200
      when(mockResponse.json) thenReturn Json.parse("{}")
      an[ExternalServiceException] should be thrownBy processGetResponse[SchemaResponse](mockResponse)
    }

    "return Option of SchemaResponse on success" in {
      when(mockResponse.status) thenReturn 200
      when(mockResponse.json) thenReturn Json.toJson(schemaResponse)

      processGetResponse[SchemaResponse](mockResponse) mustBe Some(schemaResponse)
    }
  }
}
