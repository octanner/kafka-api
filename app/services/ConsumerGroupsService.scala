package services

import javax.inject.Inject
import kafka.admin.ConsumerGroupCommand
import models.Models.{ ConsumerGroupMember, ConsumerGroupOffset }
import models.http.HttpModels.ConsumerGroupSeekRequest
import org.apache.kafka.clients.admin.ConsumerGroupDescription
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import play.api.{ Configuration, Logger }
import utils.AdminClientUtil
import utils.ConsumerUtil
import utils.Exceptions.{ InvalidRequestException, ResourceNotFoundException }

import scala.collection.JavaConverters._
import scala.collection.immutable.Map
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{ Failure, Success, Try }

class ConsumerGroupsService @Inject() (util: AdminClientUtil, consumerUtil: ConsumerUtil, conf: Configuration) {
  import ConsumerGroupsService.{ SEEK_TO_BEGINNING, SEEK_TO_END }
  val logger = Logger(this.getClass)

  def list(cluster: String): Future[Seq[String]] = {
    Future {
      val adminClient = util.getAdminClient(cluster)
      val listConsumerGroupsResult = adminClient.listConsumerGroups().all()
      Try(listConsumerGroupsResult.get) match {
        case Success(consumerGroups) =>
          consumerGroups.asScala.map { cg => cg.groupId() }.toSeq.sorted
        case Failure(e) =>
          logger.error(s"Unable to list consumer groups for cluster $cluster", e)
          throw e
      }
    }
  }

  def listOffsets(cluster: String, consumerGroupName: String): Future[Seq[ConsumerGroupOffset]] = {
    Future {
      val adminClient = util.getAdminClient(cluster)
      val descResponse = Try(adminClient.describeConsumerGroups(Seq(consumerGroupName).asJava).all().get)
      val offsetResponse = Try(adminClient.listConsumerGroupOffsets(consumerGroupName).partitionsToOffsetAndMetadata().get)
      adminClient.close()
      offsetResponse match {
        case Success(partitionToOffsetAndMetadata) =>
          val partitionToOffsetAndMetadataMap = partitionToOffsetAndMetadata.asScala
          val topics = partitionToOffsetAndMetadataMap.keys.map(_.topic()).toSet
          val consumer = consumerUtil.getKafkaConsumer(cluster, Some(topics.toSeq))
          val endOffsets = consumer.endOffsets(partitionToOffsetAndMetadataMap.keys.toList.asJava)
          consumer.close()

          descResponse match {
            case Success(consumerGroupDescMap) =>
              val map = consumerGroupDescMap.asScala
              val cgDesc = map(consumerGroupName)
              getConsumerGroupOffsets(cgDesc, partitionToOffsetAndMetadataMap.toMap, endOffsets.asScala.toMap)
            case Failure(e) =>
              logger.error(s"Failed to describe consumer group `${consumerGroupName}` for cluster `${cluster}`")
              throw e
          }
        case Failure(e) =>
          logger.error(s"Failed to list consumer group offsets for consumer group `${consumerGroupName}` for cluster `${cluster}`")
          throw e
      }

    }
  }

  def listMembers(cluster: String, consumerGroupName: String): Future[Seq[ConsumerGroupMember]] = {
    Future {
      val adminClient = util.getAdminClient(cluster)
      val descResponse = Try(adminClient.describeConsumerGroups(Seq(consumerGroupName).asJava).all().get)
      adminClient.close()
      descResponse match {
        case Success(consumerGroupDescMap) =>
          val map = consumerGroupDescMap.asScala
          val cgDesc = map(consumerGroupName)
          cgDesc.members().asScala.map { member =>
            ConsumerGroupMember(member.consumerId(), member.host(), member.clientId(),
              member.assignment().topicPartitions().asScala.size)
          }.toList
        case Failure(e) =>
          logger.error(s"Failed to describe consumer group `${consumerGroupName}` for cluster `${cluster}`")
          throw e
      }
    }
  }

  def seek(cluster: String, consumerGroupName: String, seekRequest: ConsumerGroupSeekRequest) = {
    Future {
      val topic = seekRequest.topic
      val allPartitionsForTopic = getAllTopicPartitions(cluster, topic)

      val partitionsToSeek = if (seekRequest.allPartitions.isDefined && seekRequest.allPartitions.get) {
        allPartitionsForTopic.toList
      } else {
        seekRequest.partitions.get.map { p =>
          val partition = new TopicPartition(topic, p)
          if (allPartitionsForTopic.contains(partition)) {
            partition
          } else {
            throw new InvalidRequestException(s"Topic `${topic}` does not contain partion ${p} to seek")
          }
        }
      }

      val brokers = conf.get[String](cluster.toLowerCase + AdminClientUtil.KAFKA_LOCATION_CONFIG)
      var cmdArgs = new ListBuffer[String]() ++ List("--bootstrap-server", brokers, "--group", consumerGroupName,
        "--topic", s"${topic}:${partitionsToSeek.map(_.partition()).mkString(",")}",
        "--command-config", conf.get[String](cluster.toLowerCase + AdminClientUtil.KAFKA_ADMIN_CONFIG_FILE),
        "--reset-offsets", "--execute")
      if (seekRequest.seekTo.equalsIgnoreCase(SEEK_TO_BEGINNING)) {
        cmdArgs += "--to-earliest"
        ConsumerGroupCommand.main(cmdArgs.toArray)
      } else if (seekRequest.seekTo.equalsIgnoreCase(SEEK_TO_END)) {
        cmdArgs += "--to-latest"
        ConsumerGroupCommand.main(cmdArgs.toArray)
      } else {
        throw new InvalidRequestException(s"seekTo is not valid. Allowed values are (${SEEK_TO_BEGINNING}, ${SEEK_TO_END})")
      }
    }
  }

  private def getConsumerGroupOffsets(
    consumerGroupDesc:            ConsumerGroupDescription,
    partitionToOffsetMetadataMap: Map[TopicPartition, OffsetAndMetadata],
    partitionToEndOffsetMap:      Map[TopicPartition, java.lang.Long]): Seq[ConsumerGroupOffset] = {
    var topicPartitionDescMap = Map[TopicPartition, ConsumerGroupOffset]()
    val members = consumerGroupDesc.members().asScala.toList
    for {
      member <- members
      partition: TopicPartition <- member.assignment().topicPartitions().asScala
    } {
      if (partitionToOffsetMetadataMap.get(partition).isDefined && partitionToEndOffsetMap.get(partition).isDefined) {
        val currentOffset = partitionToOffsetMetadataMap(partition).offset
        val logEndOffset = partitionToEndOffsetMap(partition)
        val lag = logEndOffset - currentOffset
        topicPartitionDescMap += (partition -> ConsumerGroupOffset(partition.topic(), partition.partition(), currentOffset,
          logEndOffset, lag, Some(member.consumerId()),
          Some(member.host()), Some(member.clientId())))
      }
    }

    for { (partition, offsetAndMetadata) <- partitionToOffsetMetadataMap if (!topicPartitionDescMap.isDefinedAt(partition)) } {
      val currentOffset = offsetAndMetadata.offset
      val logEndOffset = partitionToEndOffsetMap(partition)
      val lag = logEndOffset - currentOffset
      topicPartitionDescMap += (partition -> ConsumerGroupOffset(partition.topic(), partition.partition(), currentOffset,
        logEndOffset, lag, None, None, None))
    }
    topicPartitionDescMap.values.toList.sortBy(t => (t.topic, t.partition))
  }

  private def getAllTopicPartitions(cluster: String, topic: String) = {
    val adminClient = util.getAdminClient(cluster)
    val topicDescResponse = Try(adminClient.describeTopics(List(topic).asJava).all().get)
    adminClient.close()
    topicDescResponse match {
      case Success(topicDescMap) =>
        val topicDesc = topicDescMap.asScala.get(topic)
          .getOrElse(throw new ResourceNotFoundException(s"Topic ${topic} not found"))
        topicDesc.partitions().asScala.map(p => new TopicPartition(topic, p.partition())).toSet
      case Failure(e) =>
        logger.error(s"Failed to get Topic description for topic ${topic}")
        throw e
    }
  }
}

object ConsumerGroupsService {
  val SEEK_TO_BEGINNING = "beginning"
  val SEEK_TO_END = "end"
}
