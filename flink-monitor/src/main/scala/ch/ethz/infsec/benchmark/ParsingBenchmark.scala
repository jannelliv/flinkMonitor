package ch.ethz.infsec.benchmark

import ch.ethz.infsec.trace.{CsvFormat, Record}
import ch.ethz.infsec.{Processor, StatelessProcessor}

import scala.io.Source

class ParsingBenchmark(input: Iterator[String], parser: Processor[String, Record]) {
  def step(): (List[Record], Long) = {
    if (input.hasNext) {
      val line = input.next()
      var records: List[Record] = Nil
      parser.process(line, records ::= _)
      (records, line.length)  // NOTE(JS): Ideally we would like return the number of bytes, but this is close enough
    } else (null, 0)
  }
}

class NullParser extends StatelessProcessor[String, Record] {
  override def process(in: String, f: Record => Unit): Unit = f(Record.markEnd(in.length))

  override def terminate(f: Record => Unit) { }
}

object ParsingBenchmark {
  def main(args: Array[String]) {
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
      runner.measure(() => bench.step())
    } finally {
      inputSource.close()
    }
  }
}
