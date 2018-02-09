package ch.eth.inf.infsec.trace

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Parses the CSV format used in the CRV '14 competition.
  *
  * E. Bartocci et al., “First international Competition on Runtime Verification: rules, benchmarks, tools,
  * and final results of CRV 2014,” Int J Softw Tools Technol Transfer, pp. 1–40, Apr. 2017.
  */
class CsvParser extends LineBasedEventParser {
  // NOTE(JS): We assume that event indexes are non-negative.
  protected var currentIndex: Long = -1
  protected var currentTimestamp: Long = 0
  protected val currentStructure = new mutable.HashMap[String, ArrayBuffer[Tuple]]()

  override def processLine(line: String): Unit = {
    val (index, timestamp, relation, tuple) = CsvParser.parseLine(line)
    if (index != currentIndex) {
      processEnd()
      currentIndex = index
      currentTimestamp = timestamp
    }
    currentStructure.getOrElseUpdate(relation, { new ArrayBuffer[Tuple]() }) += tuple
  }

  override def processEnd(): Unit =
    if (currentIndex >= 0) {
      buffer += Event(currentTimestamp, currentStructure.toMap)
      currentIndex = -1
      currentStructure.clear()
    }
}

object CsvParser {
  // TODO(JS): Handle attribute types other than Long and String.
  def parseValue(value: String): Any = {
    try {
      value.toLong
    } catch {
      case _: NumberFormatException => value
    }
  }

  def parseLine(line: String): (Long, Long, String, IndexedSeq[Any]) = {
    val fields = line.split(",").map(field => {
      val parts = field.split("=")
      if (parts.length == 2) parts(1).trim else field.trim
    })
    val relation = fields(0)
    val index = fields(1).toLong
    val timestamp = fields(2).toLong
    val tuple = fields.drop(3).map(parseValue).toIndexedSeq
    (index, timestamp, relation, tuple)
  }
}

object CsvFormat extends TraceFormat {
  override def createParser(): LineBasedEventParser = new CsvParser()
}
