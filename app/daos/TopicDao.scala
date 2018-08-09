package daos

import java.sql.Connection

import anorm._
import javax.inject.Inject
import models.Models.Topic
import org.joda.time.DateTime

class TopicDao @Inject() () {
  def insert(cluster: String, topic: Topic, partitions: Int, replicas: Int, retentionMs: Long, cleanupPolicy: String)(implicit conn: Connection) = {
    SQL"""
        insert into TOPIC (topic, partitions, replicas, retention_ms, cleanup_policy, created_timestamp,
        cluster, organization, description) values
        (${topic.name}, ${partitions}, ${replicas}, ${retentionMs}, ${cleanupPolicy},
        ${DateTime.now().toDate}, ${cluster}, ${topic.organization}, ${topic.description})
      """
      .execute()
  }
}
