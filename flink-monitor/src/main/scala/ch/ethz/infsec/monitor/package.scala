package ch.ethz.infsec

package object monitor {

  trait MonitorRequest

  trait MonpolyRequest extends  MonitorRequest{
    val in: String
  }

  case class EventItem(in: String) extends MonpolyRequest
  case class CommandItem(in: String) extends MonpolyRequest

  trait DejavuRequest extends MonitorRequest {
    val in: String
  }
  case class DejavuEventItem(in: String) extends DejavuRequest
  case class DejavuCommandItem(in: String) extends DejavuRequest


  implicit class IndexedRecord(t:(Int,trace.Record)) extends MonitorRequest

}
