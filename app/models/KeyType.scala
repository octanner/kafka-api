package models

import play.api.libs.json.{ Reads, Writes }
import utils.EnumUtils

object KeyType extends Enumeration {
  type KeyType = Value

  implicit def valueToKeyTypeVal(x: Value): Val = x.asInstanceOf[Val]
  implicit val enumReads: Reads[KeyType] = EnumUtils.enumReads(KeyType)
  implicit def enumWrites: Writes[KeyType] = EnumUtils.enumWrites

  val NONE, STRING, AVRO = Value
}