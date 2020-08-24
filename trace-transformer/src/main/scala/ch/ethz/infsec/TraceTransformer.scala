package ch.ethz.infsec

import java.io._

import org.apache.commons.math3.distribution.{EnumeratedIntegerDistribution, UniformRealDistribution}
import org.apache.commons.math3.random.{RandomGenerator, Well19937c}
import org.apache.commons.math3.special.Erf

import scala.collection.mutable

sealed case class Config(numOutputs: Int = 4,
                         transformerId: Int = 1,
                         inputCsv: String = "",
                         distribution: Option[String] = None,
                         outputCsvPrefix: String = "",
                         sigma: Double = 2.0,
                         maxOOO: Int = 5,
                         seed: Long = 314159265,
                         watermarkPeriod: Int = 2,
                         start: Boolean = true)

abstract class AbstractTransformer(input: BufferedReader,
                                   output: Array[Writer],
                                   start: Boolean) {

  val numPartitions: Int = output.length

  protected def extractTimestamp(line: String): Long =
    line.split(',')(2).split('=')(1).trim.toLong

  protected def sendRecordToAll(line: String): Unit =
    output.foreach(_.write(line + "\n"))

  protected def sendRecordToOne(partition: Int, line: String): Unit =
    output(partition).write(line + "\n")

  protected var minTimestamp = 0L

  protected def formatStart(ts: Long): String

  protected def handleRecord(line: String): Unit

  protected def handleEnd(): Unit

  def runTransformer(): Unit = {
    val firstLine = input.readLine()
    if (firstLine != null) {
      minTimestamp = extractTimestamp(firstLine)
      if (start) {
        sendRecordToAll(formatStart(minTimestamp))
      }
      handleRecord(firstLine)

      var line: String = null
      while ({line = input.readLine(); line != null}) {
        handleRecord(line)
      }
    }
    handleEnd()
  }
}

class PerPartitionOrderTransformer(input: BufferedReader,
                                   partitionDist: EnumeratedIntegerDistribution,
                                   output: Array[Writer],
                                   start: Boolean)
  extends AbstractTransformer(input, output, start) {
  require(partitionDist.getSupportUpperBound < numPartitions)

  override protected def formatStart(ts: Long): String = s">START $ts<"

  override protected def handleRecord(line: String): Unit = sendRecordToOne(partitionDist.sample(), line)

  override protected def handleEnd(): Unit = ()
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

class WatermarkOrderEmissionTimeTransformer(input: BufferedReader,
                                            partitionDist: EnumeratedIntegerDistribution,
                                            timeDist: TruncNormDistribution,
                                            output: Array[Writer],
                                            watermarkPeriod: Int,
                                            start: Boolean)
  extends AbstractTransformer(input, output, start) {
  require(partitionDist.getSupportUpperBound < numPartitions)

  val emissionSeparator: String = "'"

  private def sampleDelta(): Long = math.round(timeDist.sample())

  private def makeRecord(emissionTime: Long, line: String): String = {
    s"$emissionTime$emissionSeparator$line"
  }

  private def makeWatermark(emissionTime: Long, tsVal: Long): String = {
    s"$emissionTime$emissionSeparator>WATERMARK $tsVal<"
  }

  override protected def formatStart(ts: Long): String =
    s"0$emissionSeparator>START $ts<"

  private val outputQueue = new mutable.PriorityQueue[(Long, Int, Long, String)]()(
    Ordering.by[(Long, Int, Long, String), Long](_._1).reverse)

  private val pendingTimestamps = Array.fill(numPartitions)(new mutable.TreeMap[Long, Int]())

  private val nextWatermarks = Array.fill(numPartitions)(watermarkPeriod.toLong)

  private var maxTimestamp = 0L

  private def outputUpto(limit: Long): Unit = {
    while (outputQueue.nonEmpty && outputQueue.head._1 <= limit) {
      val (emissionTime, partition, timestamp, line) = outputQueue.dequeue()

      val watermarkTime = pendingTimestamps(partition).firstKey - 1
      var nextWatermark = nextWatermarks(partition)
      while (nextWatermark <= emissionTime) {
        sendRecordToOne(partition, makeWatermark(nextWatermark, watermarkTime))
        nextWatermark = nextWatermark + watermarkPeriod
      }
      nextWatermarks(partition) = nextWatermark

      val record = makeRecord(emissionTime, line)
      sendRecordToOne(partition, record)

      val pending = pendingTimestamps(partition)(timestamp)
      if (pending == 1) {
        pendingTimestamps(partition) -= timestamp
      } else {
        pendingTimestamps(partition)(timestamp) = pending - 1
      }
    }
  }

  override protected def handleRecord(line: String): Unit = {
    val timestamp = extractTimestamp(line)
    maxTimestamp = timestamp
    val relativeTimestamp = timestamp - minTimestamp
    outputUpto(relativeTimestamp - 1)

    val emissionTime = relativeTimestamp + sampleDelta()
    require(emissionTime >= 0)
    val partition = partitionDist.sample()
    outputQueue.enqueue((emissionTime, partition, timestamp, line))

    val pendingMap = pendingTimestamps(partition)
    pendingMap(timestamp) = pendingMap.getOrElse(timestamp, 0) + 1
  }

  override protected def handleEnd(): Unit = {
    outputUpto(Long.MaxValue)
    for (partition <- 0 until numPartitions) {
      val watermark = makeWatermark(nextWatermarks(partition), maxTimestamp)
      sendRecordToOne(partition, watermark)
    }
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
        .action((prefix, c) => c.copy(outputCsvPrefix = prefix))
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
      case "" => new BufferedReader(new InputStreamReader(System.in))
      case _  => new BufferedReader(new FileReader(config.inputCsv))
    }
    val output: Array[Writer] =
      if (config.numOutputs == 1)
        config.outputCsvPrefix match {
          case "" => Array(new BufferedWriter(new OutputStreamWriter(System.out)))
          case _ => Array(new BufferedWriter(new FileWriter(config.outputCsvPrefix + "0.csv")))
        }
      else
      (0 until config.numOutputs)
      .map(k =>
            new BufferedWriter(new FileWriter((if (config.outputCsvPrefix=="") "output" else config.outputCsvPrefix) + k.toString + ".csv"))
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
    val transformer: AbstractTransformer = config.transformerId match {
      case 1 | 2 => new PerPartitionOrderTransformer(input, partitionDist, output, config.start)
      case 4 => new WatermarkOrderEmissionTimeTransformer(input,
                                                          partitionDist,
                                                          new TruncNormDistribution(rng, config.sigma, 0.0, config.maxOOO + 0.001),
                                                          output,
                                                          config.watermarkPeriod,
                                                          config.start)
      case _ => throw new Exception("unsupported transformer version " + config.transformerId)
    }

    transformer.runTransformer()

    input.close()
    output.foreach { k =>
      k.flush()
      k.close()
    }
  }
}
