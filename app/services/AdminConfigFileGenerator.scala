package services

import java.io.{ BufferedWriter, File, FileWriter }

import javax.inject.{ Inject, Singleton }
import play.api.Configuration
import utils.AdminClientUtil._

@Singleton
class AdminConfigFileGenerator @Inject() (conf: Configuration) {
  val CLUSTERS = List("dev", "test", "maru", "nonprod", "prod")

  def createConfigFiles(): Unit = {
    for { cluster <- CLUSTERS } {
      val kafkaHostName = conf.getOptional[String](cluster.toLowerCase + KAFKA_LOCATION_CONFIG)

      if (kafkaHostName.isDefined) {
        val filename = conf.get[String](cluster.toLowerCase + KAFKA_ADMIN_CONFIG_FILE)

        val username = conf.getOptional[String](cluster.toLowerCase + KAFKA_ADMIN_USERNAME_CONFIG)
        val password = conf.getOptional[String](cluster.toLowerCase + KAFKA_ADMIN_PASSWORD_CONFIG)
        val saslJaas = username.map { u =>
          password.map { p =>
            s"""sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="$u" password="$p";\n"""
          }
        }.flatten

        val saslMechanism = conf.getOptional[String](cluster.toLowerCase + KAFKA_SASL_MECHANISM_CONFIG)
          .map(m => s"sasl.mechanism=$m\n")
        val securityProtocol = conf.getOptional[String](cluster.toLowerCase + KAFKA_SECURITY_PROTOCOL_CONFIG)
          .map(p => s"security.protocol=$p\n")

        val file = new File(filename)
        val bw = new BufferedWriter(new FileWriter(file))
        saslJaas.map(bw.write(_))
        saslMechanism.map(bw.write(_))
        securityProtocol.map(bw.write(_))
        bw.close()
      }

    }
  }

  createConfigFiles()
}
