import com.google.inject.AbstractModule
import play.api.{ Configuration, Environment }
import services.AdminConfigFileGenerator

class Module(environment: Environment, configuration: Configuration) extends AbstractModule {
  override def configure() = {
    bind(classOf[AdminConfigFileGenerator]).asEagerSingleton()
  }
}
