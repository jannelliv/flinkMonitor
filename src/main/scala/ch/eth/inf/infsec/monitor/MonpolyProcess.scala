package ch.eth.inf.infsec.monitor

import java.nio.file.{Files, Path}

import scala.collection.mutable.ArrayBuffer

// TODO(JS): Split into two classes (Monpoly/echo)
class MonpolyProcess(val command: Seq[String], val isActuallyMonpoly: Boolean) extends AbstractExternalProcess[String, String] {
  private val GET_INDEX_COMMAND = ">get_pos<\n"
  private val GET_INDEX_PREFIX = "Current index: "

  @transient private var tempDirectory: Path = _
  @transient private var tempStateFile: Path = _

  override def start(): Unit = {
    super.start()
    tempDirectory = Files.createTempDirectory("monpoly-state")
    tempDirectory.toFile.deleteOnExit()
    tempStateFile = tempDirectory.resolve("state.bin")
    tempStateFile.toFile.deleteOnExit()
  }

  override def destroy(): Unit = {
    super.destroy()
    if (tempDirectory != null)
      Files.deleteIfExists(tempStateFile)
    if (tempStateFile != null)
      Files.deleteIfExists(tempDirectory)
  }

  override def writeRequest(in: String): Unit = {
    writer.write(in)
    if (isActuallyMonpoly)
      writer.write(GET_INDEX_COMMAND)
    // TODO(JS): Do not flush if there are more requests in the queue
    writer.flush()
  }

  override def initSnapshot(): Unit = {
    println("DEBUG [MonpolyProcess] initSnapshot")
    writer.write(">get_pos<\n")
    writer.write(">save_state \"" + tempStateFile + "\"<\n")
    writer.flush()
  }

  override def initRestore(snapshot: Array[Byte]): Unit = {
    println("DEBUG [MonpolyProcess] initRestore: writing to file")
    Files.write(tempStateFile, snapshot)
    println("DEBUG [MonpolyProcess] initRestore: sending command")
    writer.write(">get_pos<\n")
    writer.write(">restore_state \"" + tempStateFile + "\"<\n")
    writer.write(">get_pos<\n")
    writer.flush()
  }

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

  override def readSnapshot(): Array[Byte] = {
    var line0 = reader.readLine()
    println("DEBUG [MonpolyProcess] at snapshot: " + line0)

    println("DEBUG [MonpolyProcess] readSnapshot: reading reply")
    val line = reader.readLine()
    println("DEBUG [MonpolyProcess] readSnapshot: reply = " + line)
    if (line != "save_state OK")
      throw new Exception("Monitor process failed to save state.")
    val state = Files.readAllBytes(tempStateFile)
    println("DEBUG [MonpolyProcess] readSnapshot: state has been read")
    Files.deleteIfExists(tempStateFile)
    state
  }

  override def restored(): Unit = {
    var line0 = reader.readLine()
    println("DEBUG [MonpolyProcess] before restore: " + line0)

    println("DEBUG [MonpolyProcess] restored: reading reply")
    val line = reader.readLine()
    println("DEBUG [MonpolyProcess] restored: reply = " + line)
    if (line != "restore_state OK")
      throw new Exception("Monitor process failed to restore state.")

    var line1 = reader.readLine()
    println("DEBUG [MonpolyProcess] after restore: " + line1)
  }
}
