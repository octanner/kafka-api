package controllers

import com.google.common.util.concurrent.Futures.FutureCombiner
import daos.ClusterDao
import javax.inject.Inject
import play.api.db.Database
import play.api.libs.json.Json
import play.api.mvc.InjectedController

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class ClusterController @Inject() (db: Database, dao: ClusterDao) extends InjectedController {
  def getClusters = Action.async { implicit request =>
    Future {
      db.withConnection { implicit conn => dao.getClusters() }
    }
      .map { clusters =>
        Ok(Json.toJson(clusters))
      }
  }
}
