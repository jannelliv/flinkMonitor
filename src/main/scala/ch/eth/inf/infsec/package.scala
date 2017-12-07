package ch.eth.inf

import scala.collection.mutable

package object infsec {
  // TODO(JS): Type of data domain?
  // TODO(JS): Consider using a more specialized container type, e.g. Array.
  type Tuple = IndexedSeq[Any]

  case class Event(timestamp: Long, structure: collection.Map[String, Iterable[Tuple]]) {
    override def toString: String = s"@$timestamp: $structure"
  }

  private val event = """@(\d+)(.*)""".r
  private val structure = """\s*?([A-Za-z]\w*)\s*((\(\s*?\)|\(\s*?\w+(\s*?\,\s*?\w+)*\s*?\))*)""".r

  def parseTuple(str:String):Set[Tuple] = {
    str.replaceAll("""\s""","").tail.init.split("""\)\(""").toSet.map((x:String)=>x.split(",").toIndexedSeq)
  }
  def parseLine(str: String):Option[Event] = {

    try {
      val event(ts, db) = str
      val relations = structure.findAllMatchIn(db)
      val map = new mutable.HashMap[String, Set[Tuple]]()
      relations.foreach(m=> map(m.group(1))=parseTuple(m.group(2)))
      Some(new Event(ts.toLong,map))
    } catch {
      case _:Exception => None
    }

  }

}

