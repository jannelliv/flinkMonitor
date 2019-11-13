package ch.ethz.infsec

import java.io.{BufferedWriter, FileWriter}
import org.apache.commons.math3.distribution.GeometricDistribution

import scala.collection.mutable
import scala.io.Source
import scala.util.Random

case class Config(numOutputs: Int = 4,
                  transformerId: Int = 1,
                  inputCsv: String = "input.csv",
                  outoutCsvPrefix: String = "output")


abstract class TransformerImpl(numPartitions: Int, output: Array[BufferedWriter]) {
  protected def sendEOFs() : Unit = {
    sendRecord(">EOF<", None)
    sendRecord(">TERMSTREAM<", None)
  }

  protected def sendRecord(line: String, partition: Option[Int]) : Unit = {
    partition match {
      case Some(i) =>
        require(i >= 0 && i < numPartitions)
        output(i).write(line + "\n")
      case None =>
        (0 until numPartitions).foreach(i => output(i).write(line + "\n"))
    }
  }
  def runTransformer() : Unit
}

class WatermarkOrderTransformer(numPartitions: Int, input: Source, output: Array[BufferedWriter]) extends TransformerImpl(numPartitions, output) {
  private def moveCurrTs(currTs: Long, elementsLeft: Array[Int]) : Int = {
    //Return the index of the first non-zero value of the array
    for (i <- currTs.toInt until elementsLeft.length) {
      if (elementsLeft(i) != 0) {
        return i
      }
    }
    //DONE
    elementsLeft.length
  }

  private def sendWatermark(tsVal: Int) : Unit = {
    sendRecord(">WATERMARK " + tsVal + "<", None)
  }

  private def writeToOutput[U <: mutable.Buffer[String]](factMap: mutable.Map[Int, U]) : Unit = {
    val sampler = new GeometricDistribution(0.5)
    val maxTs = factMap.keys.max

    //Lowest TS for which not all facts have been written to the output
    var currTs = factMap.keys.min

    //TS are the idx of the arr, the values are the numbers of facts left for that TS
    val elementsLeft = new Array[Int](maxTs + 1)

    //Init the number of elements for each key
    factMap.keys.foreach(k => elementsLeft(k) = factMap(k).length)

    while (currTs != maxTs + 1) {
      val currTsRet = moveCurrTs(currTs, elementsLeft)

      //currTS update, send a watermark
      if (currTsRet > currTs) {
        sendWatermark(currTsRet - 1)
      }

      currTs = currTsRet
      //Select a timestamp with a random offset (~ Geo) from the currentTS
      val tsSample = math.min(maxTs, currTs + sampler.sample())
      if (elementsLeft(tsSample) > 0) {
        val buf = factMap(tsSample)
        elementsLeft(tsSample) -= 1
        //Select a random fact in the the set of facts with TS tsSample
        val idxSample = Random.nextInt(buf.length)
        val line = buf.remove(idxSample)
        sendRecord(line, Some(Random.nextInt(numPartitions)))
      }
    }
    sendWatermark(maxTs + 1)
    sendEOFs()
  }

  override def runTransformer(): Unit = {
    //Split input into lines
    val event_lines = input.getLines().toArray
    //Extract the timestamps and group by timestamps
    val tsToLines = event_lines
      .map(k => {
        val ts = k.split(',')(2).split('=')(1).toInt
        (ts, k)
      })
      .groupBy(_._1)
      .mapValues(k => mutable.ArrayBuffer(k.map(_._2) :_* ))
    //Randomize the trace and write it to the output
    writeToOutput(mutable.Map() ++= tsToLines)
  }
}

class PerPartitionOrderTransformer(numPartitions: Int, input: Source, output: Array[BufferedWriter]) extends TransformerImpl(numPartitions, output)  {
  def runTransformer() : Unit = {
    for (line <- input.getLines()) {
      sendRecord(line, Some(Random.nextInt(numPartitions)))
    }
    sendEOFs()
  }
}

class WatermarkOrderEmissionTimeTransformer(numPartitions: Int, input: Source, output: Array[BufferedWriter]) extends TransformerImpl(numPartitions, output) {
  override def runTransformer(): Unit = {
    sys.exit(1)
  }
}


object TraceTransformer {
  def parseArgs(args: Array[String]) : Config = {
    import scopt.OParser
    val builder = OParser.builder[Config]
    import builder._
    val parser = OParser.sequence(
      programName("Trace transformer"),
      opt[Int]('v', "version")
          .required()
          .valueName("<int>")
          .validate(k => if (k >= 1 && k <= 4) Right(()) else Left("variant must be between 1 and 4"))
          .action((n, c) => c.copy(transformerId = n))
          .text("Id of the transformer that should be applied"),
      opt[Int]('n', "numoutputs")
        .optional()
        .valueName("<int>")
        .validate(k => if (k > 1) Right(()) else Left("n must be > 1"))
        .action((n, c) => c.copy(numOutputs = n))
        .text("Number of output partitions"),
      opt[String]('o', "outputprefix")
        .optional()
        .valueName("<prefix>")
        .action((prefix, c) => c.copy(outoutCsvPrefix = prefix))
        .text("Name of the output files"),
      arg[String]("<input csv>")
        .required()
        .action((filename, c) => c.copy(inputCsv = filename))
    )
    OParser.parse(parser, args, Config()) match {
      case Some(k) => k
      case None => sys.exit(1)
    }
  }

  def main(args: Array[String]) : Unit = {
    val config = parseArgs(args)
    val input = Source.fromFile(config.inputCsv)
    val output = (0 until config.numOutputs)
      .map(k =>
        new BufferedWriter(new FileWriter(config.outoutCsvPrefix + k.toString + ".csv"))
      ).toArray

    val transformer: TransformerImpl = config.transformerId match {
      case 1 => new PerPartitionOrderTransformer(config.numOutputs, input, output)
      case 3 => new WatermarkOrderTransformer(config.numOutputs, input, output)
      case 4 => new WatermarkOrderEmissionTimeTransformer(config.numOutputs, input, output)
      case _ => throw new Exception("should not happen")
    }

    transformer.runTransformer()

    input.close()
    output.foreach{k =>
      k.flush()
      k.close()
    }
  }
}
