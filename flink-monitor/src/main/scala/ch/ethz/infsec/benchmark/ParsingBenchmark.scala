package ch.ethz.infsec.benchmark

import java.util.function.Consumer

import ch.ethz.infsec.monitor.Fact
import ch.ethz.infsec.trace.parser.{Crv2014CsvParser, MonpolyTraceParser, TraceParser}

import scala.io.Source

class ParsingBenchmark(input: Iterator[String], parser: TraceParser) {
  def step(): (List[Fact], Long) = {
    if (input.hasNext) {
      val line = input.next()
      var records: List[Fact] = Nil
      parser.parseLine(records ::= _, line)
      (records, line.length)  // NOTE(JS): Ideally we would like return the number of bytes, but this is close enough
    } else (null, 0)
  }
}

class NullParser extends TraceParser {
  override def parseLine(sink: Consumer[Fact], line: String): Unit = sink.accept(Fact.terminator(line.toLong))

  override def endOfInput(sink: Consumer[Fact]): Unit = ()

  override def inInitialState(): Boolean = true

  override def setTerminatorMode(mode: TraceParser.TerminatorMode): Unit = throw new Exception("not implemented")
}

object ParsingBenchmark {
  def main(args: Array[String]) {
    val batchSize = 100
    val reportInterval = 1000
    val warmupTime = 3000

    val formatName = args(0)
    val parser = formatName match {
      case "null" => new NullParser
      case "csv" => new Crv2014CsvParser
      case "monpoly" => new MonpolyTraceParser
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
      runner.measure(() => bench.step())
    } finally {
      inputSource.close()
    }
  }
}
