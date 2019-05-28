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
}

trait ExternalProcessFactory[IN, +PIN, POUT, OUT] extends Serializable{
  def create[UIN >: PIN]():(Processor[IN,UIN], ExternalProcess[UIN,POUT], Processor[POUT,OUT]) = (createPre(),createProc(),createPost())
  protected def createPre[UIN >: PIN]():Processor[IN,UIN]
  protected def createProc[UIN >: PIN]():ExternalProcess[UIN,POUT]
  protected def createPost():Processor[POUT,OUT]
}

trait DirectExternalProcessFactory[IN, OUT] extends ExternalProcessFactory[IN, IN, OUT, OUT]{
  override protected def createPre[UIN >: IN]():Processor[IN,UIN] = StatelessProcessor.identity[IN]
  override protected def createPost():Processor[OUT,OUT] = StatelessProcessor.identity[OUT]
}