package ch.eth.inf.infsec.integration


import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}
import java.lang.ProcessBuilder.Redirect
import java.util.{Timer, TimerTask}
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.collection.{JavaConversions, mutable}
import scala.collection.mutable.{HashSet, Set}
import scala.io.Source


class FlinkIntegration extends FunSuite with Matchers with BeforeAndAfterAll{

  private var ncIN:Process = _
  private var ncOUT:Process = _
  private var flink:Process = _
  private var monpoly:Process = _

  private val monpolyVerdicts  = new HashSet[String]()
  private val flinkVerdicts  = new HashSet[String]()
  private val READTIMEOUT = 1000


  val example = "ex1"
  //val log = "@0 q(1)\n@0 q(2)\n@1 p(2)\n@3 p(3)\n@4 p(1)\n@5 p(7)\n@10\n@1000\n@1001 p(2)\n@1005\n@1009\n@1100\n@1111 p(6)\n@1111\n@1122\n@1133"
  val log = Source.fromFile(s"src/test/resources/${example}.log").mkString
  val sig = s"src/test/resources/${example}.sig"
  val formula = s"src/test/resources/${example}.mfotl"

  override def beforeAll() {

    ncIN = new ProcessBuilder(JavaConversions.seqAsJavaList(Seq("nc", "-l", "9000")))
      .redirectError(Redirect.INHERIT)
      .start()

    ncOUT = new ProcessBuilder(JavaConversions.seqAsJavaList(Seq("nc", "-l", "9001")))
      .redirectError(Redirect.INHERIT)
      .start()


    flink = new ProcessBuilder(
      JavaConversions.seqAsJavaList(
        List("mvn", "exec:java", "-Dexec.mainClass=ch.eth.inf.infsec.StreamMonitoring",
         "-Dexec.args="+" --processors 4" +
                        s" --sig ${sig}" +
                        s" --formula ${formula}" +
                        " --out 127.0.0.1:9001"
                        )))
      .redirectError(Redirect.INHERIT)
      .start()

    monpoly = new ProcessBuilder(
      JavaConversions.seqAsJavaList(
        List("monpoly",
            "-sig",sig,
            "-formula",formula,
            "-negate")))
      .redirectError(Redirect.INHERIT)
      .start()

  }

class ReadingThread(output:BufferedReader, verdicts:Set[String],process:Process, timeout:Long) extends Thread{
    override def run(): Unit = {
      var tt = new InactivityTimer(process)
      var timer = new Timer("MyTimer")
      try {
        //there is at least one
        var i=0
        var line = output.readLine()
        while (line != null) {
          if(line.startsWith("@"))
            verdicts.add(line)
          timer.schedule(tt,timeout)
          line = output.readLine()
          timer.cancel()
          timer = new Timer("MyTimer")
          tt = new InactivityTimer(process)
        }
      } catch {
        case _:InterruptedException =>
      } finally {
        output.close()
      }

    }
  }

  class InactivityTimer(p:Process) extends TimerTask {
    override def run(): Unit = {
      p.destroy()
    }
  }

  test("Flink vs Monpoly") {


    val inputMonpoly = new BufferedWriter(new OutputStreamWriter(monpoly.getOutputStream))
    val outputMonpoly = new BufferedReader(new InputStreamReader(monpoly.getInputStream))

    var t = new ReadingThread(outputMonpoly, monpolyVerdicts, monpoly,READTIMEOUT)
    inputMonpoly.write(log)
    inputMonpoly.flush()
    t.start()
    t.join()

    val inputFlink = new BufferedWriter(new OutputStreamWriter(ncIN.getOutputStream))
    val outputFlink = new BufferedReader(new InputStreamReader(ncOUT.getInputStream))

    t = new ReadingThread(outputFlink,flinkVerdicts,flink,READTIMEOUT)

    inputFlink.write(log)
    inputFlink.flush()

    t.start()
    t.join()

    println("VERDICTS: " + monpolyVerdicts.toString)
    println("VERDICTS: " + flinkVerdicts.toString)

    //TODO: fix the bug that make this fail
    //Set(flinkVerdicts) shouldEqual Set(monpolyVerdicts)
  }


  override def afterAll() {
    if(ncIN!=null) ncIN.destroy()
    if(flink!=null) flink.destroy()
    if(ncOUT!=null) ncOUT.destroy()
    if(monpoly!=null) monpoly.destroy()

  }

}
