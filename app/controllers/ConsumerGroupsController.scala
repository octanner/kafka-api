package controllers

import javax.inject.Inject
import models.http.HttpModels.ConsumerGroupSeekRequest
import play.api.Logger
import play.api.libs.json.Json
import play.api.mvc.{ InjectedController, Result }
import services.ConsumerGroupsService
import utils.Exceptions.InvalidRequestException

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

class ConsumerGroupsController @Inject() (service: ConsumerGroupsService) extends InjectedController with RequestProcessor {
  val logger = Logger(this.getClass)

  def list(cluster: String) = Action.async { implicit request =>
    service.list(cluster).map { cgs =>
      Ok(Json.toJson(cgs))
    }
  }

  def listOffsets(cluster: String, consumerGroupName: String) = Action.async {
    service.listOffsets(cluster, consumerGroupName).map { offsets =>
      Ok(Json.toJson(offsets))
    }
  }

  def listMembers(cluster: String, consumerGroupName: String) = Action.async {
    service.listMembers(cluster, consumerGroupName).map { members =>
      Ok(Json.toJson(members))
    }
  }

  def seek(cluster: String, consumerGroupName: String) = Action.async { implicit request =>
    processRequest[ConsumerGroupSeekRequest](handleSeekRequest(cluster, consumerGroupName))
  }

  def preview(cluster: String, topic: String) = Action.async {
    service.previewTopic(cluster, topic).map { tp =>
      Ok(Json.toJson(tp))
    }
  }

  private def handleSeekRequest(cluster: String, consumerGroupName: String)(seekRequest: ConsumerGroupSeekRequest): Future[Result] = {
    service.seek(cluster, consumerGroupName, seekRequest).map { _ =>
      Ok
    }
  }

  private def validateSeekRequest(seekRequest: ConsumerGroupSeekRequest) = {
    if ((seekRequest.partitions.isEmpty || seekRequest.partitions.get.isEmpty) &&
      (seekRequest.allPartitions.isEmpty || !seekRequest.allPartitions.get)) {
      throw new InvalidRequestException("partitions list cannot be empty when allPartition is empty/false")
    }
  }
}
