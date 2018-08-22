package utils

import java.util.Properties

import javax.inject.Inject
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.{ AdminClient, AdminClientConfig }
import org.apache.kafka.common.config.SaslConfigs
import play.api.{ Configuration, Logger }

class AdminClientUtil @Inject() (conf: Configuration) {
  import AdminClientUtil._

  val logger = Logger(this.getClass)

  def getAdminClient(cluster: String) = {
    val kafkaHostName = conf.get[String](cluster.toLowerCase + KAFKA_LOCATION_CONFIG)
    val kafkaSecurityProtocol = conf.getOptional[String](cluster.toLowerCase + KAFKA_SECURITY_PROTOCOL_CONFIG)
    val kafkaSaslMechanism = conf.getOptional[String](cluster.toLowerCase + KAFKA_SASL_MECHANISM_CONFIG)
    val username = conf.getOptional[String](cluster.toLowerCase + KAFKA_ADMIN_USERNAME_CONFIG)
    val password = conf.getOptional[String](cluster.toLowerCase + KAFKA_ADMIN_PASSWORD_CONFIG)

    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHostName)
    props.put(AdminClientConfig.CLIENT_ID_CONFIG, ADMIN_CLIENT_ID)
    kafkaSecurityProtocol.map {
      props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, _)
    }
    kafkaSaslMechanism.map {
      props.put(SaslConfigs.SASL_MECHANISM, _)
    }
    kafkaSaslMechanism match {
      case Some("PLAIN") =>
        props.put(
          SaslConfigs.SASL_JAAS_CONFIG,
          s"""org.apache.kafka.common.security.plain.PlainLoginModule required username="${username.getOrElse("")}" password="${password.getOrElse("")}";""")
      case _ =>
    }
    logger.info(s"adminClientProps: $props")
    AdminClient.create(props)
  }

}

object AdminClientUtil {
  val ADMIN_CLIENT_ID = "kafka-api"
  val KAFKA_LOCATION_CONFIG = ".kafka.location"
  val KAFKA_SECURITY_PROTOCOL_CONFIG = ".kafka.security.protocol"
  val KAFKA_SASL_MECHANISM_CONFIG = ".kafka.sasl.mechanism"
  val KAFKA_ADMIN_USERNAME_CONFIG = ".kafka.admin.username"
  val KAFKA_ADMIN_PASSWORD_CONFIG = ".kafka.admin.password"
}