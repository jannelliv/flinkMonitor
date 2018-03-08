package ch.eth.inf.infsec.trace

import ch.eth.inf.infsec.Processor

import scala.collection.mutable.ArrayBuffer

/**
  * Parses the CSV format used in the CRV '14 competition.
  *
  * E. Bartocci et al., “First international Competition on Runtime Verification: rules, benchmarks, tools,
  * and final results of CRV 2014,” Int J Softw Tools Technol Transfer, pp. 1–40, Apr. 2017.
  */
class CsvParser extends Processor[String, Record] with Serializable {
  // NOTE(JS): We assume that timepoints are non-negative.
  protected var currentTimepoint: Long = -1
  protected var currentTimestamp: Long = 0

  override type State = (Timestamp, Timestamp)

  override def getState: (Timestamp, Timestamp) = (currentTimestamp, currentTimepoint)

  override def restoreState(state: Option[(Timestamp, Timestamp)]): Unit = state match {
    case Some((ts, tp)) =>
      currentTimestamp = ts
      currentTimepoint = tp
    case None =>
      currentTimestamp = -1
      currentTimepoint = 0
  }

  override def process(line: String, f: Record => Unit) {
    val (timepoint, timestamp, relation, tuple) = CsvParser.parseLine(line)
    if (timepoint != currentTimepoint) {
      terminate(f)
      currentTimepoint = timepoint
      currentTimestamp = timestamp
    }
    f(Record(timestamp, relation, tuple))
  }

  override def terminate(f: Record => Unit) {
    if (currentTimepoint >= 0) {
      f(Record.markEnd(currentTimestamp))
      currentTimepoint = -1
    }
  }
}

object CsvParser {
  // TODO(JS): Handle attribute types other than Long and String.
  def parseValue(value: String): Domain = {
    // We first check whether the value is a number before attempting to convert it.
    // Dispatching the type by catching the exception thrown by toLong is much slower!
    // TODO(JS): We could use a signature which states the type explicitly.
    // However, parse failures might be expected in real-world scenarios? They shouldn't slow us down.
    if (value.isEmpty)
      return ""
    var char = value.charAt(0)
    var isLong = (char >= '0' && char <= '9') || char == '-' || char == '+'
    var i = 1
    while (isLong && i < value.length) {
      char = value.charAt(i)
      isLong = char >= '0' && char <= '9'
      i += 1
    }

    if (isLong)
      value.toLong
    else
      value
  }

  def parseLine(line: String): (Long, Long, String, Tuple) = {
    // Beware, slightly optimized and thus ugly code ahead.
    // TODO(JS): Compare with an implementation based on parser combinators.
    // TODO(JS): Test all the corner cases.

    var relation: String = null
    var timepoint = 0L
    var timestamp = 0L
    val tuple = new ArrayBuffer[Domain](8)

    var startIndex = -1
    var currentIndex = 0

    while (line.charAt(currentIndex).isSpaceChar) currentIndex += 1
    startIndex = currentIndex
    currentIndex = line.indexOf(',', startIndex)
    relation = line.substring(startIndex, currentIndex).trim
    currentIndex += 1

    currentIndex = line.indexOf('=', currentIndex) + 1
    while (line.charAt(currentIndex).isSpaceChar) currentIndex += 1
    startIndex = currentIndex
    currentIndex = line.indexOf(',', startIndex)
    timepoint = line.substring(startIndex, currentIndex).trim.toLong
    currentIndex += 1

    currentIndex = line.indexOf('=', currentIndex) + 1
    while (line.charAt(currentIndex).isSpaceChar) currentIndex += 1
    startIndex = currentIndex
    currentIndex = line.indexOf(',', startIndex)
    if (currentIndex < 0)
      currentIndex = line.length
    timestamp = line.substring(startIndex, currentIndex).trim.toLong
    currentIndex += 1

    while (currentIndex < line.length) {
      currentIndex = line.indexOf('=', currentIndex)
      if (currentIndex >= 0) {
        currentIndex += 1
        while (currentIndex < line.length && line.charAt(currentIndex).isSpaceChar) currentIndex += 1
        startIndex = currentIndex
        currentIndex = line.indexOf(',', startIndex)
        if (currentIndex < 0)
          currentIndex = line.length
        tuple += parseValue(line.substring(startIndex, currentIndex).trim)
        currentIndex += 1
      }
    }

    (timepoint, timestamp, relation, tuple)
  }
}

object CsvFormat extends TraceFormat {
  override def createParser(): CsvParser = new CsvParser()
}
