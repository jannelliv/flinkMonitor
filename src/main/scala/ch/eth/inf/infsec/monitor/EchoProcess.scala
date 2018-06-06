package ch.eth.inf.infsec.monitor

class EchoProcess(val command: Seq[String]) extends AbstractExternalProcess[String, String] {
  override def writeRequest(in: String): Unit = {
    writer.write(in)
    writer.flush()
  }

  override def initSnapshot(): Unit = ()

  override def initRestore(snapshot: Array[Byte]): Unit = ()

  override def readResults(): Seq[String] = {
    val line = reader.readLine()
    if (line == null)
      Seq.empty
    else
      Seq(line)
  }

  override def readSnapshot(): Array[Byte] = Array.emptyByteArray

  override def restored(): Unit = ()
}
