package ch.ethz.infsec
package monitor

class EchoDejavuProcess(override val command: Seq[String]) extends DejavuProcess(command) {
  override protected def parseResult(line: String, sink: Fact => Unit): Boolean = {
    if (line.startsWith(DejavuProcess.SYNC)) {
      false
    } else {
      sink(Fact.make("", 0L, line))
      true
    }
  }
}
