package ch.ethz.infsec
package monitor

import scala.collection.mutable


trait ExternalProcess[+IN, OUT] extends Serializable {
  var identifier: Option[String]

  def open(): Unit
  def open(initialState: Array[Byte]): Unit
  def open(initialState: Iterable[(Int, Array[Byte])]): Unit

  // Input functions
  def writeRequest[UIN >: IN](in: UIN): Unit
  def initSnapshot(): Unit
  def initSnapshot(slicer: String): Unit
  def shutdown(): Unit

  // Output functions
  def readResults(buffer: mutable.Buffer[OUT]): Unit
  def drainResults(buffer: mutable.Buffer[OUT]): Unit
  def readSnapshot(): Array[Byte]
  def readSnapshots(): Iterable[(Int, Array[Byte])]
  def join(): Unit

  def dispose(): Unit

  val SYNC_BARRIER_IN:IN
  val SYNC_BARRIER_OUT:OUT=>Boolean

}

trait ExternalProcessFactory[IN, +PIN, POUT, OUT] extends Serializable{
  def createPre[T,UIN >: PIN]():Processor[Either[IN,T],Either[UIN,T]]
  def createProc[UIN >: PIN]():ExternalProcess[UIN,POUT]
  def createPost():Processor[POUT,OUT]
}

trait DirectExternalProcessFactory[IN, OUT] extends ExternalProcessFactory[IN, IN, OUT, OUT]{
  override def createPre[T,UIN >: IN]():Processor[Either[IN,T],Either[UIN,T]] = StatelessProcessor.identity[Either[IN,T]]
  override def createPost():Processor[OUT,OUT] = StatelessProcessor.identity[OUT]
}