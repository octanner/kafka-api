package controllers

import javax.inject.Inject
import models.http.HttpModels.{ AclRequest, AclResponse }
import play.api.Logger
import play.api.libs.json.Json
import play.api.mvc.{ InjectedController, Result }
import services.AclService

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{ Failure, Success, Try }

class AclController @Inject() (service: AclService) extends InjectedController with RequestProcessor {
  val logger = Logger(this.getClass)

  def create(cluster: String) = Action.async { implicit request =>
    service.claimAcl(cluster).map { aclCreds =>
      Ok(Json.toJson(AclResponse(aclCreds)))
    }
  }

  def createAclForTopic(cluster: String) = Action.async { implicit request =>
    processRequest[AclRequest](createPermissions(cluster))
  }

  def getCredentials(cluster: String, user: String) = Action.async { implicit request =>
    service.getCredentials(cluster, user).map { credentials => Ok(Json.toJson(credentials)) }
  }

  def getAclsForTopic(cluster: String, topic: String) = Action.async { implicit request =>
    service.getAclsByTopic(cluster, topic).map { acls => Ok(Json.obj("acls" -> acls)) }
  }
  def deleteAcl(id: String) = Action.async { implicit request =>
    service.deleteAcl(id).map { _ => Ok }
  }

  private def createPermissions(cluster: String)(aclRequest: AclRequest): Future[Result] = {
    Try(service.createPermissions(cluster, aclRequest)) match {
      case Success(id)                          => Future(Ok(Json.obj("id" -> id)))
      case Failure(e: IllegalArgumentException) => Future(BadRequest(e.getMessage))
      case Failure(e)                           => throw e
    }
  }

}
