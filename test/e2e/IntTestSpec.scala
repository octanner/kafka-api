package e2e

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.scalatest.concurrent.{ IntegrationPatience, ScalaFutures }
import org.scalatest.mockito.MockitoSugar
import org.scalatestplus.play.PlaySpec
import org.scalatestplus.play.guice.GuiceOneServerPerSuite
import platform.PostgresTestSpec
import play.api.Application
import play.api.db.evolutions.EvolutionsModule
import play.api.db.{ DBModule, Database }
import play.api.inject.bind
import play.api.inject.guice.{ GuiceApplicationBuilder, GuiceableModule }
import play.api.libs.ws.ahc.AhcWSClient

trait AbstractSpec {
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val wsClient = AhcWSClient()
}

trait IntTestSpec extends PlaySpec with PostgresTestSpec with GuiceOneServerPerSuite with ScalaFutures with IntegrationPatience with MockitoSugar with AbstractSpec {

  def modulesToOverride: Seq[GuiceableModule] = Nil

  def configuration: Map[String, Any] = Map.empty

  def applicationBuilder: GuiceApplicationBuilder = {
    new GuiceApplicationBuilder()
      .disable(classOf[DBModule])
      .disable(classOf[EvolutionsModule])
      .bindings(
        bind[Database].toInstance(db)
      )
      .configure(configuration)
      .overrides(modulesToOverride: _*)
  }

  override implicit lazy val app: Application = applicationBuilder.build
}
