package ch.eth.inf.infsec.benchmark

import ch.eth.inf.infsec.trace.{CsvFormat, Event, LineBasedEventParser}

import scala.io.Source

class ParsingBenchmark(input: Iterator[String], parser: LineBasedEventParser) {
  def step(): (List[Event], Long) = {
    if (input.hasNext) {
      val line = input.next()
      parser.processLine(line)
      var events: List[Event] = Nil
      for (event <- parser.bufferedEvents)
        events ::= event
      parser.clearBuffer()
      (events, line.length)  // NOTE(JS): Ideally we would like return the number of bytes, but this is close enough
    } else (null, 0)
  }
}

class NullParser extends LineBasedEventParser {
  override def processLine(line: String): Unit = buffer += Event(line.length, Map.empty)

  override def processEnd(): Unit = ()
}

object ParsingBenchmark {
  def main(args: Array[String]): Unit = {
    val batchSize = 100
    val reportInterval = 1000
    val warmupTime = 3000

    val formatName = args(0)
    val parser = formatName match {
      case "null" => new NullParser
      case "csv" => CsvFormat.createParser()
    }

    val inputFileName = args(1)
    val inputSource = Source.fromFile(inputFileName)
    try {
      val input = inputSource.getLines()
      val bench = new ParsingBenchmark(input, parser)
      val runner = new BenchmarkRunner(batchSize, reportInterval, warmupTime)
      runner.printComment(s"Parsing benchmark for format $formatName")
      runner.printComment(s"Input file: $inputFileName")
      runner.printComment(s"time [ns], records, characters, avg. records [1/s], avg. characters [1/s]")
      runner.measure(bench.step)
    } finally {
      inputSource.close()
    }
  }
}
