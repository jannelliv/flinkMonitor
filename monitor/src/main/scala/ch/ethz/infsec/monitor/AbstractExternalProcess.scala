package ch.ethz.infsec.monitor

import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}
import java.lang.ProcessBuilder.Redirect

import scala.collection.JavaConverters

abstract class AbstractExternalProcess[IN, OUT] extends ExternalProcess[IN, OUT] {
  @transient private var process: Process = _

  @transient protected var writer: BufferedWriter = _
  @transient protected var reader: BufferedReader = _

  override var identifier: Option[String] = None

  def open(command: Seq[String]): Unit = {
    require(process == null)

    val instantiatedCommand = identifier match {
      case Some(id) => command.map(_.replaceAll("\\{ID\\}", id))
      case None => command
    }

    println(instantiatedCommand.mkString(" "))
    process = new ProcessBuilder(JavaConverters.seqAsJavaList(instantiatedCommand))
        .redirectError(Redirect.INHERIT)
        .start()
    writer = new BufferedWriter(new OutputStreamWriter(process.getOutputStream))
    reader = new BufferedReader(new InputStreamReader(process.getInputStream))
  }

  override def shutdown(): Unit = writer.close()

  override def join(): Unit = process.waitFor()

  override def dispose(): Unit = {
    try {
      if (process != null)
        process.destroy()
    } finally {
      process = null
      writer = null
      reader = null
    }
  }
}
