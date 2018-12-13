package utils

import java.util.{ Properties, UUID }

import javax.inject.Inject
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.common.config.SaslConfigs
import play.api.{ Configuration, Logger }
import AdminClientUtil._
import org.apache.kafka.clients.consumer.{ ConsumerConfig, KafkaConsumer }
import scala.collection.JavaConverters._

class ConsumerUtil @Inject() (conf: Configuration) {
  val logger = Logger(this.getClass)

  def getKafkaConsumer(cluster: String, topics: Option[Seq[String]], groupId: String = s"kafka-api-admin-${UUID.randomUUID.toString}"): KafkaConsumer[Nothing, Nothing] = {
    val kafkaHostName = conf.get[String](cluster.toLowerCase + KAFKA_LOCATION_CONFIG)
    val kafkaSecurityProtocol = conf.getOptional[String](cluster.toLowerCase + KAFKA_SECURITY_PROTOCOL_CONFIG)
    val kafkaSaslMechanism = conf.getOptional[String](cluster.toLowerCase + KAFKA_SASL_MECHANISM_CONFIG)
    val username = conf.getOptional[String](cluster.toLowerCase + KAFKA_ADMIN_USERNAME_CONFIG)
    val password = conf.getOptional[String](cluster.toLowerCase + KAFKA_ADMIN_PASSWORD_CONFIG)

    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaHostName)
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, s"${ADMIN_CLIENT_ID}-${UUID.randomUUID.toString}")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    kafkaSecurityProtocol.map { props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, _) }
    kafkaSaslMechanism.map { props.put(SaslConfigs.SASL_MECHANISM, _) }
    kafkaSaslMechanism match {
      case Some("PLAIN") =>
        props.put(
          SaslConfigs.SASL_JAAS_CONFIG,
          s"""org.apache.kafka.common.security.plain.PlainLoginModule required username="${username.getOrElse("")}" password="${password.getOrElse("")}";""")
      case _ =>
    }
    val consumer = new KafkaConsumer(props)
    topics.map { t => consumer.subscribe(t.asJava) }
    consumer
  }

}
