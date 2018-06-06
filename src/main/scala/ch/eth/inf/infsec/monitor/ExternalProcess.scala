package ch.eth.inf.infsec.monitor

import java.nio.file.Path

trait ExternalProcess[IN, OUT] extends Serializable {
  def setTempFile(path: Path): Unit = ()

  def start(): Unit

  // Input functions
  def writeRequest(in: IN): Unit
  def initSnapshot(): Unit
  def initRestore(snapshot: Array[Byte]): Unit
  def shutdown(): Unit

  // Output functions
  def readResults(): Seq[OUT]
  def readSnapshot(): Array[Byte]
  def restored(): Unit
  def join(): Unit

  def destroy(): Unit
}
