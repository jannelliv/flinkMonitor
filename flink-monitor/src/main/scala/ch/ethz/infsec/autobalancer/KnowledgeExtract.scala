package ch.ethz.infsec.autobalancer

import ch.ethz.infsec.monitor.Fact
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

object KnowledgeExtract {
  val commandOutput: OutputTag[(Int, Fact)] = OutputTag[(Int, Fact)]("ke-comm-output")
}

class KnowledgeExtract(degree : Int) extends ProcessFunction[Fact, Fact] with Serializable {
  def printToCmdOut(in: Fact, context: ProcessFunction[Fact, Fact]#Context) : Unit = {
    context.output(KnowledgeExtract.commandOutput, (getRuntimeContext.getIndexOfThisSubtask, in))
  }

  override def processElement(in: Fact, context: ProcessFunction[Fact, Fact]#Context, collector: Collector[Fact]): Unit = {
    if (in.isMeta && (in.getName == "apt" || in.getName == "set_slicer"))
      printToCmdOut(in, context)
    collector.collect(in)
  }
}
