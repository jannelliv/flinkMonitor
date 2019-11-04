package ch.ethz.infsec
package monitor

import ch.ethz.infsec.trace.formatter.DejavuTraceFormatter

class DejavuProcess(val command: Seq[String]) extends AbstractExternalProcess[Fact, Fact] {
  private val formatter = new DejavuTraceFormatter

  override def supportsStateAccess: Boolean = false

  override def open(): Unit = open(command)

  override def openWithState(initialState: Array[Byte]): Unit = throw new UnsupportedOperationException

  override def openAndMerge(initialStates: Iterable[Array[Byte]]): Unit = throw new UnsupportedOperationException

  override def enablesSyncBarrier(in: Fact): Boolean = true

  override def writeItem(fact: Fact): Unit = {
    if (fact.isMeta) {
      throw new IllegalArgumentException
    } else {
      formatter.printFact(writer.write(_), fact)
      if (fact.isTerminator)
        writer.flush()
    }
  }

  override def writeSyncBarrier(): Unit = {
    writer.write(DejavuProcess.SYNC)
    writer.flush()
  }

  override def initSnapshot(): Unit = throw new UnsupportedOperationException

  override protected def parseResult(line: String, sink: Fact => Unit): Boolean = {
    if (line.startsWith(DejavuProcess.VIOLATION_PREFIX)) {
      sink(Fact.make("", 0L, line.stripPrefix(DejavuProcess.VIOLATION_PREFIX).stripSuffix(":\n")))
      true
    } else if (line.startsWith(DejavuProcess.SYNC_REPLY)) {
      false
    } else {
      true
    }
  }

  override def readSnapshot(): Seq[Array[Byte]] = throw new UnsupportedOperationException

}

object DejavuProcess {
  val VIOLATION_PREFIX = "**** Property violated on event number "
  val SYNC = "SYNC!\n"
  val SYNC_REPLY = "**** SYNC!"
}
