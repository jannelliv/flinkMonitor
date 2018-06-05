package ch.eth.inf.infsec.monitor

import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}
import java.lang.ProcessBuilder.Redirect

import scala.collection.JavaConversions

abstract class AbstractExternalProcess[IN, OUT] extends ExternalProcess[IN, OUT] {
  val command: Seq[String]

  private var process: Process = _

  protected var writer: BufferedWriter = _
  protected var reader: BufferedReader = _

  override def start(): Unit = {
    require(process == null)

    process = new ProcessBuilder(JavaConversions.seqAsJavaList(command))
        .redirectError(Redirect.INHERIT)
        .start()
    writer = new BufferedWriter(new OutputStreamWriter(process.getOutputStream))
    reader = new BufferedReader(new InputStreamReader(process.getInputStream))
  }

  override def shutdown(): Unit = writer.close()

  override def join(): Unit = process.waitFor()

  override def destroy(): Unit = {
    try {
      process.destroy()
    } finally {
      process = null
      writer = null
      reader = null
    }
  }
}
