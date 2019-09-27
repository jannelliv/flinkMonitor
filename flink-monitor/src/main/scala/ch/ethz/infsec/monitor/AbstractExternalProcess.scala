package ch.ethz.infsec.monitor

import java.io._
import java.lang.ProcessBuilder.Redirect

import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters

abstract class AbstractExternalProcess[IN, OUT] extends ExternalProcess[IN, OUT] {
  @transient protected var process: Process = _

  @transient protected var writer: BufferedWriter = _
  @transient protected var reader: BufferedReader = _

  @transient private var logger: Logger = _

  override var identifier: Option[String] = None

  protected def open(command: Seq[String]): Unit = {
    logger = LoggerFactory.getLogger(getClass)
    require(process == null)

    val instantiatedCommand = identifier match {
      case Some(id) => command.map(_.replaceAll("\\{ID\\}", id))
      case None => command
    }

    logger.info("Starting external process {}", instantiatedCommand)
    process = new ProcessBuilder(JavaConverters.seqAsJavaList(instantiatedCommand))
      .redirectError(Redirect.INHERIT)
      .start()

    writer = new BufferedWriter(new OutputStreamWriter(process.getOutputStream))
    reader = new BufferedReader(new InputStreamReader(process.getInputStream))
  }

  override def shutdown(): Unit = writer.close()

  protected def parseResult(line: String, sink: OUT => Unit): Boolean

  override def readResults(sink: OUT => Unit): Unit = {
    var more = true
    do {
      val line = reader.readLine()
      if (line == null) {
        more = false
      } else {
        more = parseResult(line, sink)
      }
    } while (more)
  }

  override def join(): Int = process.waitFor()

  override def dispose(): Unit = {
    if (process != null)
      process.destroy()
    process = null
    writer = null
    reader = null
  }

}
