package ch.ethz.infsec.autobalancer

import ch.ethz.infsec.monitor.Fact
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala._
import scala.collection.JavaConverters._

object KnowledgeExtract {
  val commandOutput: OutputTag[(Int, Fact)] = OutputTag[(Int, Fact)]("ke-comm-output")
}

class KnowledgeExtract(degree : Int) extends ProcessFunction[Fact, Fact] with Serializable {
  var receivedGAPTR = 0
  var highestGAPTR = 0.0
  var receivedGSDMSR = 0
  var highestGSDMSR = 0
  var receivedShutdown = 0

  override def processElement(in: Fact, context: ProcessFunction[Fact, Fact]#Context, collector: Collector[Fact]): Unit = {
    if (in.isMeta){
        val name = in.getName
        val params = List(in.getArguments).mkString

      if (name == "gaptr") {
          //wait for all, combine and only send highest
          receivedGAPTR = receivedGAPTR + 1
          val candidateHighest = params.toDouble
          if (candidateHighest > highestGAPTR)
            highestGAPTR = candidateHighest
          if (receivedGAPTR >= degree) {
            val arg: java.util.List[Object] = List(highestGAPTR.asInstanceOf[Object]).asJava
            context.output(KnowledgeExtract.commandOutput, (getRuntimeContext.getIndexOfThisSubtask, Fact.meta(name, arg)))
            receivedGAPTR = 0
            highestGAPTR = 0.0
          }
        } else if (name == "gsdmsr") {
          //wait for all, combine and only send highest
          //todo: reconsider if we may want to send fitting instead
          //todo: reconsider if we may want it to be one command instead
          //todo: consider code-dedup
          receivedGSDMSR = receivedGSDMSR + 1
          val candidateHighest = params.toInt
          if (candidateHighest > highestGSDMSR)
            highestGSDMSR = candidateHighest
          if (receivedGSDMSR >= degree) {
            val arg: java.util.List[Object] = List(highestGSDMSR.asInstanceOf[Object]).asJava
            context.output(KnowledgeExtract.commandOutput, (getRuntimeContext.getIndexOfThisSubtask, Fact.meta(name, arg)))
            receivedGSDMSR = 0
            highestGSDMSR = 0
          }
        } else {
          throw new Exception("unexpected meta fact with name " + name)
        }
    }
    collector.collect(in)
  }
}
