package ch.eth.inf.infsec

import scala.language.implicitConversions

package object trace {
  case class Domain(integralValue: Long, stringValue: String) {
    override def toString: String = if (stringValue == null) integralValue.toString else "\"" + stringValue + "\""
  }

  object StringValue {
    def apply(value: String): Domain = Domain(-1, value)

    def unapply(domain: Domain): Option[String] = Option(domain.stringValue)
  }

  object IntegralValue {
    def apply(value: Long): Domain = Domain(value, null)

    def unapply(domain: Domain): Option[Long] = if (domain.stringValue == null) Some(domain.integralValue) else None
  }

  implicit def stringToDomain(value: String): Domain = StringValue(value)

  implicit def longToDomain(value: Long): Domain = IntegralValue(value)

  implicit def intToDomain(value: Int): Domain = IntegralValue(value)

  type Tuple = IndexedSeq[Domain]
  val emptyTuple: Tuple = Vector.empty

  def Tuple(xs: Domain*) = Vector(xs: _*)

  type Timestamp = Long

  trait Record extends Serializable{
    val timestamp: Timestamp
    val parameters: String
    val command: String
    val label: String
    val data: Tuple

    def isEndMarker: Boolean
    override def toString: String
  }

  case class CommandRecord(command: String, parameters: String) extends Record {
    def isEndMarker: Boolean = false
    val timestamp: Timestamp = 0l
    val label: Null = null
    val data: Null = null

    override def toString: String = label
  }

  case class EventRecord(timestamp: Timestamp, label: String, data: Tuple)  extends Record {
    def isEndMarker: Boolean = label.isEmpty
    val parameters: Null = null
    val command: Null = null

    override def toString: String =
      if (isEndMarker) s"@$timestamp <end>" else s"@$timestamp $label(${data.mkString(", ")})"
  }

  /*case class Record(timestamp: Timestamp, label: String, data: Tuple) extends Serializable {
    def isEndMarker: Boolean = label.isEmpty

    override def toString: String =
      if (isEndMarker) s"@$timestamp <end>" else s"@$timestamp $label(${data.mkString(", ")})"
  }*/

  object Record {
    def markEnd(timestamp: Timestamp): Record = EventRecord(timestamp, "", emptyTuple)
  }

  trait TraceFormat {
    def createParser(): Processor[String, Record] with Serializable
  }

}
