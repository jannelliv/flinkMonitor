package ch.ethz.infsec
package monitor

trait ExternalProcess[IN, OUT] extends Serializable {
  var identifier: Option[String]

  def supportsStateAccess: Boolean

  def open(): Unit
  def openWithState(initialState: Array[Byte]): Unit
  def openAndMerge(initialStates: Iterable[Array[Byte]]): Unit

  // Input functions
  def enablesSyncBarrier(in: IN): Boolean
  def writeItem(in: IN): Unit
  def writeSyncBarrier(): Unit
  def initSnapshot(): Unit
  def shutdown(): Unit

  // Output functions
  def readResults(sink: OUT => Unit): Unit
  def readSnapshot(): Seq[Array[Byte]]
  def join(): Int

  def dispose(): Unit
}
