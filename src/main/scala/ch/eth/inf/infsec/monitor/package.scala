package ch.eth.inf.infsec

package object monitor {
  trait MonpolyRequest {
    val in: String
  }

  case class EventItem(in: String) extends MonpolyRequest
  case class CommandItem(in: String) extends MonpolyRequest
}
