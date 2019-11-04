package ch.ethz.infsec
package monitor

class EchoMonpolyProcess(override val command: Seq[String]) extends MonpolyProcess(command, None) {
  override def supportsStateAccess: Boolean = false

  override protected def parseResult(line: String, sink: Fact => Unit): Boolean = {
    if (line.startsWith(">get_pos<")) {
      commandAndTimeQueue.take() match {
        case Left(None) => false
        case Left(Some(command)) =>
          sink(command)
          true
        case Right(_) => true
      }
    } else {
      sink(Fact.make("", 0L, line))
      true
    }
  }

  override def openWithState(initialState: Array[Byte]): Unit = throw new UnsupportedOperationException

  override def openAndMerge(initialStates: Iterable[Array[Byte]]): Unit = throw new UnsupportedOperationException

  override def initSnapshot(): Unit = throw new UnsupportedOperationException

  override def readSnapshot(): Seq[Array[Byte]] = throw new UnsupportedOperationException
}
