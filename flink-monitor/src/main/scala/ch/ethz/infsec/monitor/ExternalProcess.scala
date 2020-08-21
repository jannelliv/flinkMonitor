package ch.ethz.infsec
package monitor

trait ExternalProcess extends Serializable {
  var identifier: Option[String]

  def supportsStateAccess: Boolean

  def open(): Unit
  def openWithState(initialState: Array[Byte]): Unit
  def openAndMerge(initialStates: Iterable[Array[Byte]]): Unit

  // Input functions
  def enablesSyncBarrier(in: Fact): Boolean
  def writeItem(in: Fact): Unit
  def writeSyncBarrier(): Unit
  def initSnapshot(): Unit
  def shutdown(): Unit

  // Output functions
  def readResults(sink: Fact => Unit): Unit
  def readSnapshot(): Seq[Array[Byte]]
  def join(): Int

  def dispose(): Unit
}
