package models

import org.apache.kafka.common.acl.AclOperation
import play.api.libs.json.{ Reads, Writes }
import utils.EnumUtils

object AclRole extends Enumeration {
  type AclRole = Value

  protected case class Val(role: String) extends super.Val {
    def operation: AclOperation = AclOperation.fromString(role)
  }

  implicit def valueToAclRoleVal(x: Value): Val = x.asInstanceOf[Val]
  implicit val enumReads: Reads[AclRole] = EnumUtils.enumReads(AclRole)
  implicit def enumWrites: Writes[AclRole] = EnumUtils.enumWrites

  def get(role: String): Option[AclRole] = {
    AclRole.values.find(_.role == role.toUpperCase)
  }

  val PRODUCER = Val("WRITE")
  val CONSUMER = Val("READ")
}
