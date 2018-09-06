package ch.eth.inf.infsec.trace

import ch.eth.inf.infsec.Processor
import org.slf4j.LoggerFactory
import ch.eth.inf.infsec.slicer.SlicerParser

class TraceMonitor(protected val processor: Processor[String, Record], rescale: Int => Unit) extends Processor[String, Record] with Serializable {
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val SLICER_COMMAND = "set_slicer"
  private val parser = new SlicerParser()

  override type State = this.processor.State

  override def getState: this.processor.State = processor.getState

  override def restoreState(state: Option[this.processor.State]): Unit = processor.restoreState(state)

  override def process(in: String, f: Record => Unit): Unit = {
    def processWrapper(record: Record): Unit = {
      record match {
        case CommandRecord(command, parameters) =>
          logger.info("Parsed command: " + command)
          if(command.startsWith(SLICER_COMMAND)) rescale(parser.getParallelism(parameters))
          f(CommandRecord(command, parameters))
        case EventRecord(timestamp, label, data) => f(EventRecord(timestamp, label, data))
        case _ =>
      }
    }
    processor.process(in, processWrapper)
  }

  override def terminate(f: Record => Unit): Unit = processor.terminate(f)
}
