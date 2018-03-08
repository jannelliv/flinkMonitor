package ch.eth.inf.infsec
package integration

import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}
import java.lang.ProcessBuilder.Redirect

import ch.eth.inf.infsec.StreamMonitoring.floorLog2
import ch.eth.inf.infsec.policy.Policy
import ch.eth.inf.infsec.slicer.{HypercubeSlicer, Statistics}
import ch.eth.inf.infsec.trace.{MonpolyFormat, Record}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.collection.JavaConversions
import scala.collection.mutable.ListBuffer
import scala.io.Source

class EchoSplitIntegration  extends FunSuite with Matchers with BeforeAndAfterAll{


  private var ncIN:Process = _
  private var ncOUT:Process = _
  private var flink:Process = _
  private val flinkSliceInputs  = new ListBuffer[String]()
  private val READTIMEOUT = 1000
  private val processors = 4


  val example = "ex1"
  //val log = "@0 q(1)\n@0 q(2)\n@1 p(2)\n@3 p(3)\n@4 p(1)\n@5 p(7)\n@10\n@1000\n@1001 p(2)\n@1005\n@1009\n@1100\n@1111 p(6)\n@1111\n@1122\n@1133"
  val log = Source.fromFile(s"src/test/resources/${example}.log").mkString
  val sig = s"src/test/resources/${example}.sig"
  val formula = s"src/test/resources/${example}.mfotl"
  val monitor = "src/test/scripts/echo"


  override def beforeAll(): Unit = {

    ncIN = new ProcessBuilder(JavaConversions.seqAsJavaList(Seq("nc", "-l", "9000")))
      .redirectError(Redirect.INHERIT)
      .start()

    ncOUT = new ProcessBuilder(JavaConversions.seqAsJavaList(Seq("nc", "-l", "9001")))
      .redirectError(Redirect.INHERIT)
      .start()


    flink = new ProcessBuilder(
      JavaConversions.seqAsJavaList(
        List("mvn", "exec:java", "-Dexec.mainClass=ch.eth.inf.infsec.StreamMonitoring",
          "-Dexec.args="+s" --processors ${processors}" +
            s" --sig ${sig}" +
            s" --formula ${formula}" +
            " --out 127.0.0.1:9001" +
            s" --monitor ${monitor}"
        )))
      .redirectError(Redirect.INHERIT)
      .start()

  }

  test("Echo Splits in Flink") {
    val inputFlink = new BufferedWriter(new OutputStreamWriter(ncIN.getOutputStream))
    val outputFlink = new BufferedReader(new InputStreamReader(ncOUT.getInputStream))

    val t:Thread = new ReadingThread(outputFlink,flinkSliceInputs,flink,READTIMEOUT)

    inputFlink.write(log)
    inputFlink.flush()

    t.start()
    t.join()

    flinkSliceInputs.synchronized{println("VERDICTS: " + flinkSliceInputs.toString)}

    //Parsing the log
    implicit val type1 = TypeInfo[Record]()
    implicit val type2 = TypeInfo[Option[Record]]()
    implicit val type3 = TypeInfo[(Int,Record)]()
    val parsedLog = MonpolyFormat.createParser().processAll(log.split("\n"))

    //Slicing the log
    val parsedFormula = Policy.read(Source.fromFile(formula).mkString) match {
      case Left(err) =>
        println("Cannot parse the formula: " + err)
        sys.exit(1)
      case Right(phi) => phi
    }
    val slicer = HypercubeSlicer.optimize(parsedFormula, floorLog2(processors).max(0), Statistics.constant)

    val splitLog = slicer.processAll(parsedLog)
    println("VERDICTS: " + splitLog.mkString(", "))

    //TODO: verify the splitting flinkSliceInputs vs splitLog

  }

  override def afterAll(): Unit = {
    if(ncIN!=null) ncIN.destroy()
    if(flink!=null) flink.destroy()
    if(ncOUT!=null) ncOUT.destroy()
  }

}
