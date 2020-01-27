package ch.ethz.infsec.autobalancer

import ch.ethz.infsec.monitor.Fact
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._

import scala.collection.JavaConverters._

object KnowledgeExtract {
  val commandOutput: OutputTag[(Int, Fact)] = OutputTag[(Int, Fact)]("ke-comm-output")
}

class KnowledgeExtract(degree : Int) extends ProcessFunction[Fact, Fact] with Serializable {
  override def processElement(in: Fact, context: ProcessFunction[Fact, Fact]#Context, collector: Collector[Fact]): Unit = {
    if (in.isMeta && in.getName == "apt"){
      context.output(KnowledgeExtract.commandOutput, (getRuntimeContext.getIndexOfThisSubtask, in))
    } else {
      collector.collect(in)
    }
  }
}
