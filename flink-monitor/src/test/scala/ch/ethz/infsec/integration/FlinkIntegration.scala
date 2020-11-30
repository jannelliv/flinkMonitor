package ch.ethz.infsec.integration

import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}
import java.lang.ProcessBuilder.Redirect

import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.JavaConverters
import scala.collection.mutable.ListBuffer
import scala.io.Source


class FlinkIntegration extends AnyFunSuite with Matchers with BeforeAndAfterAll{

  private var ncIN:Process = _
  private var ncOUT:Process = _
  private var flink:Process = _
  private var monpoly:Process = _

  private val monpolyVerdicts  = new ListBuffer[String]()
  private val flinkVerdicts  = new ListBuffer[String]()
  private val READTIMEOUT = 1000


  val example = "ex1"
  //val log = "@0 q(1)\n@0 q(2)\n@1 p(2)\n@3 p(3)\n@4 p(1)\n@5 p(7)\n@10\n@1000\n@1001 p(2)\n@1005\n@1009\n@1100\n@1111 p(6)\n@1111\n@1122\n@1133"
  val log = Source.fromFile(s"src/test/resources/${example}.log").mkString
  val sig = s"src/test/resources/${example}.sig"
  val formula = s"src/test/resources/${example}.mfotl"

  override def beforeAll() {

    ncIN = new ProcessBuilder(JavaConverters.seqAsJavaList(Seq("nc", "-l", "9000")))
      .redirectError(Redirect.INHERIT)
      .start()

    ncOUT = new ProcessBuilder(JavaConverters.seqAsJavaList(Seq("nc", "-l", "9001")))
      .redirectError(Redirect.INHERIT)
      .start()


    flink = new ProcessBuilder(
      JavaConverters.seqAsJavaList(
        List("mvn", "exec:java", "-Dexec.mainClass=ch.ethz.infsec.StreamMonitoring",
         "-Dexec.args="+" --processors 4" +
                        s" --sig ${sig}" +
                        s" --formula ${formula}" +
                        " --out 127.0.0.1:9001"
                        )))
      .redirectError(Redirect.INHERIT)
      .redirectOutput(Redirect.INHERIT)
      .start()

    monpoly = new ProcessBuilder(
      JavaConverters.seqAsJavaList(
        List("monpoly",
            "-sig",sig,
            "-formula",formula,
            "-negate")))
      .redirectError(Redirect.INHERIT)
      .redirectOutput(Redirect.INHERIT)
      .start()

  }

  test("Flink vs Monpoly") {

/*    //run monpoly
    val inputMonpoly = new BufferedWriter(new OutputStreamWriter(monpoly.getOutputStream))
    val outputMonpoly = new BufferedReader(new InputStreamReader(monpoly.getInputStream))

    var t = new ReadingThread(outputMonpoly, monpolyVerdicts, monpoly,READTIMEOUT)
    inputMonpoly.write(log)
    inputMonpoly.flush()
    inputMonpoly.close()
    t.start()
    t.join()*/

    //run flink
    val inputFlink = new BufferedWriter(new OutputStreamWriter(ncIN.getOutputStream))
    val outputFlink = new BufferedReader(new InputStreamReader(ncOUT.getInputStream))

    var t = new ReadingThread(outputFlink,flinkVerdicts,flink,READTIMEOUT)

    inputFlink.write(log)
    inputFlink.flush()

    t.start()
    t.join()

    monpolyVerdicts.synchronized{println("VERDICTS: " + monpolyVerdicts.toString)}
    flinkVerdicts.synchronized{println("VERDICTS: " + flinkVerdicts.toString)}

    flinkVerdicts.toSet shouldEqual monpolyVerdicts.toSet
  }


  override def afterAll() {
    if(ncIN!=null) ncIN.destroy()
    if(flink!=null) flink.destroy()
    if(ncOUT!=null) ncOUT.destroy()
    if(monpoly!=null) monpoly.destroy()

  }

}
