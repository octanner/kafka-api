package daos

import anorm.{ Macro, RowParser, SqlStringInterpolation }
import java.sql.Connection

import models.Models.Cluster

class ClusterDao {
  def getClusters()(implicit connection: Connection): List[Cluster] = {
    SQL"""
        SELECT NAME, DESCRIPTION, NAME as size FROM CLUSTER
      """
      .as(clusterParser.*)
  }

  implicit val clusterParser: RowParser[Cluster] = Macro.indexedParser[Cluster]
}
