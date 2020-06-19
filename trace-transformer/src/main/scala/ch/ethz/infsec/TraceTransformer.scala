package ch.ethz.infsec

import java.io.{BufferedWriter, FileWriter, OutputStreamWriter, Writer}

import org.apache.commons.math3.distribution.{EnumeratedIntegerDistribution, GeometricDistribution, RealDistribution, UniformRealDistribution}
import org.apache.commons.math3.random.{RandomGenerator, Well19937c}
import org.apache.commons.math3.special.Erf

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.util.Random

sealed case class Config(numOutputs: Int = 4,
                         transformerId: Int = 1,
                         inputCsv: String = "",
                         distribution: Option[String] = None,
                         outoutCsvPrefix: String = "",
                         sigma: Double = 2.0,
                         maxOOO: Int = 5,
                         seed: Long = Random.nextLong(),
                         watermarkPeriod: Int = 2,
                         start:Boolean=true)

abstract class TransformerImpl(rng: RandomGenerator, numPartitions: Int, partitionDist: EnumeratedIntegerDistribution, output: Array[Writer],start:Boolean) {
  protected def sendRecord(line: String, partition: Option[Int]): Unit = {
    partition match {
      case Some(i) =>
        require(i >= 0 && i < numPartitions)
        output(i).write(s"$line\n")
      case None =>
        (0 until numPartitions).foreach(i => output(i).write(s"$line\n"))
    }
  }

  def runTransformer(): Unit
}

object WatermarkOrderHelpers {
  def getTimeStamps(input: Source): Iterator[(Int, String)] = {
    input.getLines().map(k => {
      val ts = k.split(',')(2).split('=')(1).trim.toInt
      (ts, k)
    })
  }
}

class WatermarkOrderTransformer(rng: RandomGenerator,
                                numPartitions: Int,
                                partitionDist: EnumeratedIntegerDistribution,
                                input: Source,
                                output: Array[Writer],
                                start:Boolean)
  extends TransformerImpl(rng, numPartitions, partitionDist, output,start) {
  private def moveCurrTs(currTs: Long, elementsLeft: Array[Int]): Int = {
    //Return the index of the first non-zero value of the array
    for (i <- currTs.toInt until elementsLeft.length) {
      if (elementsLeft(i) != 0) {
        return i
      }
    }
    //DONE
    elementsLeft.length
  }

  private def sendWatermark(tsVal: Int): Unit = {
    sendRecord(s">WATERMARK $tsVal<", None)
  }

  private def writeToOutput[U <: mutable.Buffer[String]](factMap: mutable.Map[Int, U]): Unit = {
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
        val idxSample = rng.nextInt(buf.length)
        val line = buf.remove(idxSample)
        sendRecord(line, Some(partitionDist.sample()))
      }
    }
    sendWatermark(maxTs + 1)
  }

  override def runTransformer(): Unit = {
    val tsToLines = WatermarkOrderHelpers.getTimeStamps(input)
      .toArray
      .groupBy(_._1)
      .mapValues(k => mutable.ArrayBuffer(k.map(_._2): _*))
    //Randomize the trace and write it to the output
    writeToOutput(mutable.Map() ++= tsToLines)
  }
}

class PerPartitionOrderTransformer(rng: RandomGenerator,
                                   numPartitions: Int,
                                   partitionDist: EnumeratedIntegerDistribution,
                                   input: Source,
                                   output: Array[Writer],
                                   start:Boolean)
  extends TransformerImpl(rng, numPartitions, partitionDist: EnumeratedIntegerDistribution, output,start) {
  def runTransformer(): Unit = {
    if(start) {
      val first_elem = WatermarkOrderHelpers.getTimeStamps(input).take(1).toArray.head._1
      sendRecord(s">START $first_elem<", None)
    }
    for (line <- input.getLines()) {
      sendRecord(line, Some(partitionDist.sample()))
    }
  }
}

class TruncNormDistribution(rng: RandomGenerator, sigma: Double, a: Double, b: Double) {
  require(a < b)
  val lower: Double = cdf(a)
  val upper: Double = cdf(b)
  require(lower < upper)
  val unif: UniformRealDistribution = new UniformRealDistribution(rng, lower, upper)

  def cdf(x: Double) : Double = (1.0/2.0) * (1 + Erf.erf(x/(sigma * math.sqrt(2))))

  def quantile(x: Double) : Double = sigma * math.sqrt(2)*Erf.erfInv(2*x-1)

  def sample(): Double = quantile(unif.sample())
}

class WatermarkOrderEmissionTimeTransformer(rng: RandomGenerator,
                                            numPartitions: Int,
                                            partitionDist: EnumeratedIntegerDistribution,
                                            input: Source,
                                            output: Array[Writer],
                                            sigma: Double,
                                            maxOOO: Int,
                                            watermarkPeriod: Int,
                                            start:Boolean)
  extends TransformerImpl(rng, numPartitions, partitionDist: EnumeratedIntegerDistribution, output,start) {

  val dist: TruncNormDistribution = new TruncNormDistribution(rng, sigma, 0.0, maxOOO + 0.001)
  val emissionSeparator: String = "'"

  private def sample(): Int = math.round(dist.sample().floatValue())

  private def makeRecord(emissionTime: Int, line: String): String = {
    s"$emissionTime$emissionSeparator$line"
  }

  private def makeWatermark(emissionTime: Int, tsVal: Int): String = {
    s"$emissionTime$emissionSeparator>WATERMARK $tsVal<"
  }

  override def runTransformer(): Unit = {
    val tsAndLines = WatermarkOrderHelpers.getTimeStamps(input).toArray
    val currTime = (0 until numPartitions map (_ => 0)).toArray
    val outLines = (0 until numPartitions map (_ => ArrayBuffer[(Int, Int, String)]())).toArray
    val minTs = tsAndLines.minBy(_._1)._1
    val maxTs = tsAndLines.maxBy(_._1)._1
    if(start)
      sendRecord(s"0$emissionSeparator>START  $minTs<", None)
    for ((ts, line) <- tsAndLines) {
      val samp = sample()
      val emissiontime = ts + samp - minTs
      require(emissiontime >= 0.0)
      val partition = partitionDist.sample()
      if (emissiontime > currTime(partition))
        currTime(partition) = emissiontime
      val record = makeRecord(emissiontime, line)
      outLines(partition).append((emissiontime, ts, record))
    }

    outLines
      .transform(k => k.sortWith((e1, e2) => e1._1 < e2._1))
      .zipWithIndex
      .transform(k => {
        val allLines = k._1
        val partId = k._2
        var currTime = 0
        val maxTime = allLines.last._1
        val buf = new ArrayBuffer[(Int, Int, String)]()
        val tsLeft = mutable.Map() ++= allLines.groupBy(_._2).mapValues(_.length)
        for (e <- allLines) {
          val (emissionTime, timeStamp, _) = e
          buf.append(e)
          require(tsLeft(timeStamp) > 0)
          tsLeft(timeStamp) -= 1
          if (tsLeft(timeStamp) == 0)
            tsLeft -= timeStamp
          if (emissionTime >= currTime + watermarkPeriod) {
            currTime = emissionTime
            if (tsLeft.keySet.nonEmpty) {
              val minDoneTs = tsLeft.keySet.min - 1
              buf.append((currTime, minDoneTs, makeWatermark(currTime, minDoneTs)))
            }
          }
        }
        buf.append((maxTime, maxTs, makeWatermark(maxTime, maxTs)))
        (buf, partId)
      })
      .foreach(e => e._1
        .foreach(k => sendRecord(k._3, Some(e._2)))
      )
  }
}

object TraceTransformer {
  def parseArgs(args: Array[String]): Config = {
    import scopt.OParser
    val builder = OParser.builder[Config]
    import builder._
    val parser = OParser.sequence(
      programName("Trace transformer"),
      opt[Boolean]('s',"start-ts")
        .optional()
        .valueName("<bool>")
        .action((d, c) => c.copy(start = d))
        .text("write the start time-stamp to each partition"),
      opt[String]("distribution")
          .optional()
          .valueName("<p1,p2,...,pn>")
          .action((d, c) => c.copy(distribution = Some(d)))
          .text("probabilities for the categorical distribution"),
      opt[Int]('v', "version")
        .required()
        .valueName("<int>")
        .validate(k => if (k >= 1 && k <= 4) Right(()) else Left("variant must be between 1 and 4"))
        .action((n, c) => c.copy(transformerId = n))
        .text("Id of the transformer that should be applied"),
      opt[Int]('n', "numoutputs")
        .optional()
        .valueName("<int>")
        .validate(k => if (k > 0) Right(()) else Left("n must be > 0"))
        .action((n, c) => c.copy(numOutputs = n))
        .text("Number of output partitions"),
      opt[String]('o', "outputprefix")
        .optional()
        .valueName("<prefix>")
        .action((prefix, c) => c.copy(outoutCsvPrefix = prefix))
        .text("Name of the output files"),
      opt[Long]("seed")
        .optional()
        .valueName("<long>")
        .action((seed, c) => c.copy(seed = seed))
        .text("seed for the random number generators"),
      opt[Double](name = "sigma")
        .optional()
        .valueName("<double>")
        .validate(k => if (k > 0.0) Right(()) else Left("sigma must be >= 0"))
        .action((sigma, c) => c.copy(sigma = sigma))
        .text("when v = 4: sigma for the half-normal distribution"),
      opt[Int](name = "max_ooo")
        .optional()
        .valueName("<int>")
        .validate(k => if (k >= 0) Right(()) else Left("max_oo must be >= 0"))
        .action((maxOOO, c) => c.copy(maxOOO = maxOOO))
        .text("when v = 4: cutoff for the half-normal distribution"),
      opt[Int](name = "watermark_period")
        .optional()
        .valueName("<int>")
        .validate(k => if (k > 0) Right(()) else Left("watermark_period must be >= 0"))
        .action((watermarkPeriod, c) => c.copy(watermarkPeriod = watermarkPeriod))
        .text("when v = 4: period of the watermarks"),
      arg[String]("<input csv>")
        .optional()
        .action((filename, c) => c.copy(inputCsv = filename))
    )
    OParser.parse(parser, args, Config()) match {
      case Some(k) => k
      case None => sys.exit(1)
    }
  }

  def main(args: Array[String]): Unit = {
    val config = parseArgs(args)
    val rng = new Well19937c
    rng.setSeed(config.seed)
    val input = config.inputCsv match {
      case "" => Source.fromInputStream(System.in)
      case _  => Source.fromFile(config.inputCsv)
    }
    val output: Array[Writer] =
      if (config.numOutputs == 1)
        config.outoutCsvPrefix match {
          case "" => Array(new BufferedWriter(new OutputStreamWriter(System.out)))
          case _ => Array(new BufferedWriter(new FileWriter(config.outoutCsvPrefix + "0.csv")))
        }
      else
      (0 until config.numOutputs)
      .map(k =>
            new BufferedWriter(new FileWriter((if (config.outoutCsvPrefix=="") "output" else config.outoutCsvPrefix) + k.toString + ".csv"))
          ).toArray
    val numOutputs = config.numOutputs
    val singletons = (0 until numOutputs).toArray
    val probabilities = config.distribution match {
      case None => (0 until numOutputs).map(_ => 1.0 / numOutputs).toArray
      case Some(distString) =>
        val split = distString.split(",").map(k => k.toDouble)
        if (split.length != numOutputs)
          throw new RuntimeException(s"got ${split.length} probs but $numOutputs outputs, must match")
        val sum = split.sum
        if (sum > 1 + 1e-3 || sum < -1e-3)
          throw new RuntimeException("probabilities must sum up to 1")
        split
    }
    val partitionDist = new EnumeratedIntegerDistribution(rng, singletons, probabilities)
    val transformer: TransformerImpl = config.transformerId match {
      case 1 | 2 => new PerPartitionOrderTransformer(rng, config.numOutputs, partitionDist, input, output,config.start)
      case 3 => new WatermarkOrderTransformer(rng, config.numOutputs, partitionDist, input, output,config.start)
      case 4 => new WatermarkOrderEmissionTimeTransformer(rng,
                                                          config.numOutputs,
                                                          partitionDist,
                                                          input,
                                                          output,
                                                          config.sigma,
                                                          config.maxOOO,
                                                          config.watermarkPeriod,
                                                          config.start)
      case _ => throw new Exception("cannot not happen")
    }

    transformer.runTransformer()

    input.close()
    output.foreach { k =>
      k.flush()
      k.close()
    }
  }
}