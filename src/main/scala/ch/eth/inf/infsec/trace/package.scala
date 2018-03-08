package ch.eth.inf.infsec

import scala.language.implicitConversions

package object trace {

  trait Domain extends Serializable

  case class StringValue(value: String) extends Domain {
    override def toString: String = "\"" + value + "\""
  }

  case class IntegralValue(value: Long) extends Domain {
    override def toString: String = value.toString
  }

  implicit def stringToDomain(value: String): StringValue = StringValue(value)

  implicit def longToDomain(value: Long): IntegralValue = IntegralValue(value)

  implicit def intToDomain(value: Int): IntegralValue = IntegralValue(value)

  type Tuple = IndexedSeq[Domain]
  val emptyTuple: Tuple = Vector.empty

  def Tuple(xs: Domain*) = Vector(xs: _*)

  type Timestamp = Long

  case class Record(timestamp: Timestamp, label: String, data: Tuple) extends Serializable {
    def isEndMarker: Boolean = label.isEmpty

    override def toString: String =
      if (isEndMarker) s"@$timestamp <end>" else s"@$timestamp $label(${data.mkString(", ")})"
  }

  object Record {
    def markEnd(timestamp: Timestamp): Record = Record(timestamp, "", emptyTuple)
  }

  trait TraceFormat {
    def createParser(): Processor[String, Record] with Serializable
  }

}
