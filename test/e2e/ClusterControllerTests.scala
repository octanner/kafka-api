package e2e

import anorm.SqlStringInterpolation
import models.Models.Cluster
import org.scalatest.BeforeAndAfterEach
import play.api.mvc.Results._

class ClusterControllerTests extends IntTestSpec with BeforeAndAfterEach {
  val cluster = Cluster("test", "Test Embedded Kafka Cluster", "test")
  override def beforeAll() = {
    super.beforeAll()
    db.withTransaction { implicit conn =>
      SQL"""
          Insert into cluster (name, description) values (${cluster.name}, ${cluster.description})
        """.execute()
    }
  }

  override def afterAll(): Unit = {
    db.withTransaction { implicit conn =>
      SQL"""
           delete from CLUSTER;
        """.execute()
    }
    super.afterAll()
  }

  "ClusterController #getClusters" must {
    "return Ok with all clusters" in {
      val futureResult = wsUrl(s"/v1/kafka/clusters").get()
      val result = futureResult.futureValue

      Status(result.status) mustBe Ok
      result.json.as[List[Cluster]] mustBe List(cluster)
    }
  }
}
