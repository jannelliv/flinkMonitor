package ch.eth.inf.infsec.trace

import ch.eth.inf.infsec.{CloseableIterable, SequentiallyGroupBy}

import scala.io.Source

/**
  * Parses the CSV format used in the CRV '14 competition.
  *
  * E. Bartocci et al., “First international Competition on Runtime Verification: rules, benchmarks, tools,
  * and final results of CRV 2014,” Int J Softw Tools Technol Transfer, pp. 1–40, Apr. 2017.
  */
object CsvFormat extends LogReader {
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
    val timepoint = fields(1).toLong
    val timestamp = fields(2).toLong
    val tuple = fields.drop(3).map(parseValue).toIndexedSeq
    (timepoint, timestamp, relation, tuple)
  }

  def parseLines(lines: Iterator[String]): Iterator[Event] =
    lines.map(parseLine).sequentiallyGroupBy(_._1).map { case (_, tuples) =>
      val structure = tuples.groupBy(_._3).map { case (relation, data) => (relation, data.map(_._4)) }
      Event(tuples.head._2, structure)
    }

  override def readFile(fileName: String): CloseableIterable[Event] = new CloseableIterable[Event] {
    private val source = Source.fromFile(fileName)

    override def close(): Unit = source.close()

    override def iterator: Iterator[Event] = parseLines(source.getLines())
  }
}
