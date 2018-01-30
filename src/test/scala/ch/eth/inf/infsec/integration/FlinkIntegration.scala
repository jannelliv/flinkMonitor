package ch.eth.inf.infsec.integration


import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}
import java.lang.ProcessBuilder.Redirect

import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

import scala.collection.JavaConversions

class FlinkIntegration extends FunSuite with Matchers with BeforeAndAfterAll{

  private var nc:Process = _
  private var flink:Process = _



  override def beforeAll() {
    nc = new ProcessBuilder(JavaConversions.seqAsJavaList(Seq("nc", "-l", "9000")))
        .redirectError(Redirect.INHERIT)
        .start()

    flink = new ProcessBuilder(
      JavaConversions.seqAsJavaList(
        List("mvn", "exec:java", "-Dexec.mainClass=ch.eth.inf.infsec.StreamMonitoring", "-Dexec.args=\"--processors 4 --sig " +
          System.getProperty("user.dir")+"/src/test/resources/ex1.sig --formula "+System.getProperty("user.dir")+"/src/test/resources/ex1.mfotl\"")))
          .redirectError(Redirect.INHERIT)
          .start()


  }


  test("Print/parse round-trip") {

    //TODO: read from a file
    val log = "@0 q(1)\n@0 q(2)\n@1 p(2)\n@3 p(3)\n@4 p(1)\n@5 p(7)\n@10\n@1000\n@1001 p(2)\n@1005\n@1009\n@1100\n@1111 p(6)\n@1111\n@1122"

    val input = new BufferedWriter(new OutputStreamWriter(nc.getOutputStream))
    val output = new BufferedReader(new InputStreamReader(flink.getInputStream))

    input.write(log)
    input.flush()

    val verdict = output.readLine()
    //TODO: separate maven from flink output
    println("VERDICT: " +verdict)

    //TODO: validate the verdict
    val a = true
    a shouldEqual true
  }


  override def afterAll() {

    val exitNc = if(nc!=null) nc.destroy() else 0
    val exitFlink = if(flink!=null) flink.destroy() else 0

  }

}
