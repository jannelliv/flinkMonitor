package ch.eth.inf.infsec.monitor

import scala.collection.mutable

trait ExternalProcess[IN, OUT] extends Serializable {
  var identifier: Option[String]

  def open(): Unit
  def open(initialState: Array[Byte]): Unit
  def open(initialState: Iterable[(Int, Array[Byte])]): Unit

  // Input functions
  def writeRequest(in: IN): Unit
  def initSnapshot(): Unit
  def shutdown(): Unit

  // Output functions
  def readResults(buffer: mutable.Buffer[OUT]): Unit
  def drainResults(buffer: mutable.Buffer[OUT]): Unit
  def readSnapshot(): Array[Byte]
  def readSnapshots(): Iterable[(Int, Array[Byte])]
  def join(): Unit

  def dispose(): Unit
}
