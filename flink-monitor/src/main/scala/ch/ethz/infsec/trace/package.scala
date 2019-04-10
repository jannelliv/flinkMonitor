package ch.ethz.infsec

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

  case class Record(timestamp: Timestamp, label: String, data: Tuple, command: String, parameters: String) {
    def isEndMarker: Boolean = {
      this match {
        case EventRecord(_,_,_) => label.isEmpty
        case CommandRecord(_,_) => false
        case _ => false
      }
    }
    override def toString: String = {
      this match {
        case EventRecord(_,_,_) => if (isEndMarker) s"@$timestamp <end>" else s"@$timestamp $label(${data.mkString(", ")})"
        case CommandRecord(_,_) => ""
        case _ => ""
      }
    }
  }

  class CommandRecord(cmd: String, param: String) {
    def isEndMarker: Boolean = false
    val command: String = cmd
    val parameters: String = param
    val timestamp: Timestamp = -1l

    override def toString: String = ""
  }

  class EventRecord(ts: Timestamp, lab: String, d: Tuple) {
    def isEndMarker: Boolean = label.isEmpty
    val timestamp: Timestamp = ts
    val label: String = lab
    val data: Tuple = d

    override def toString: String =
      if (isEndMarker) s"@$timestamp <end>" else s"@$timestamp $label(${data.mkString(", ")})"
  }



  //Can't use null values for unused parameters, otherwise Flink can't serialize
  object EventRecord{
    def apply(timestamp: Timestamp, label: String, data: Tuple): Record = Record(timestamp, label, data, "", "")

    def unapply(record: Record): Option[(Timestamp, String, Tuple)] = if(record.timestamp != -1l) Some((record.timestamp, record.label, record.data)) else None

  }

  object CommandRecord{
    def apply(command: String, parameters: String): Record = Record(-1l, "", emptyTuple, command, parameters)

    def unapply(record: Record): Option[(String, String)] = if(record.timestamp == -1l) Some((record.command, record.parameters)) else None
  }

  object Record {
    def markEnd(timestamp: Timestamp): Record = EventRecord(timestamp, "", emptyTuple)
  }

  trait TraceFormat {
    def createParser(): Processor[String, Record] with Serializable
  }

}
