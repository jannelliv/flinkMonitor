package ch.eth.inf.infsec.monitor

import scala.collection.mutable.ArrayBuffer

// TODO(JS): Split into two classes (Monpoly/echo)
class MonpolyProcess(val command: Seq[String], val isActuallyMonpoly: Boolean) extends AbstractExternalProcess[(Int, String), String] {
  private val GET_INDEX_COMMAND = ">get_pos<\n"
  private val GET_INDEX_PREFIX = "Current index: "

  override def writeRequest(in: (Int, String)): Unit = {
    writer.write(in._2)
    if (isActuallyMonpoly)
      writer.write(GET_INDEX_COMMAND)
    // TODO(JS): Do not flush if there are more requests in the queue
    writer.flush()
  }

  // TODO(JS)
  override def initSnapshot(): Unit = ()

  // TODO(JS)
  override def initRestore(snapshot: Array[Byte]): Unit =
    println("DEBUG [MonpolyProcess#initRestore] snapshot = " + snapshot.mkString(","))

  // TODO(JS): Move the reusable buffer to KeyedExternalProcessOperator.
  private var resultBuffer = new ArrayBuffer[String]()

  override def readResults(): Seq[String] = {
    resultBuffer.clear()
    var more = true
    do {
      val line = reader.readLine()
      if (line != null) {
        if (isActuallyMonpoly) {
          if (line.startsWith(GET_INDEX_PREFIX)) {
            more = false
          } else {
            // TODO(JS): Check that line is a verdict before adding to the buffer.
            resultBuffer += line
          }
        } else {
          resultBuffer += line
          more = false
        }
      }
    } while (more)
    resultBuffer
  }

  // TODO(JS)
  override def readSnapshot(): Array[Byte] = Array(0xde.toByte, 0xad.toByte, 0xbe.toByte, 0xef.toByte)

  // TODO(JS)
  override def restored(): Unit = ()
}
