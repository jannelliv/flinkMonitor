package ch.eth.inf.infsec.monitor

import java.nio.file.{Files, Path}

import scala.collection.mutable.ArrayBuffer

class MonpolyProcess(val command: Seq[String]) extends AbstractExternalProcess[String, String] {
  private val GET_INDEX_COMMAND = ">get_pos<\n"
  private val GET_INDEX_PREFIX = "Current index: "

  private val SAVE_STATE_COMMAND = ">save_state \"%s\"<\n"
  private val SAVE_STATE_OK = "save_state OK"

  private val RESTORE_STATE_CMD = ">restore_state \"%s\"<\n"
  private val RESTORE_STATE_OK = "restore_state OK"

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
    writer.write(GET_INDEX_COMMAND)
    // TODO(JS): Do not flush if there are more requests in the queue
    writer.flush()
  }

  override def initSnapshot(): Unit = {
    writer.write(SAVE_STATE_COMMAND.format(tempStateFile.toString))
    writer.flush()
  }

  override def initRestore(snapshot: Array[Byte]): Unit = {
    Files.write(tempStateFile, snapshot)
    writer.write(RESTORE_STATE_CMD.format(tempStateFile.toString))
    writer.flush()
  }

  // TODO(JS): Move the reusable buffer to ExternalProcessOperator.
  private var resultBuffer = new ArrayBuffer[String]()

  override def readResults(): Seq[String] = {
    resultBuffer.clear()
    var more = true
    do {
      val line = reader.readLine()
      if (line != null) {
        if (line.startsWith(GET_INDEX_PREFIX)) {
          more = false
        } else {
          // TODO(JS): Check that line is a verdict before adding it to the buffer.
          resultBuffer += line
        }
      }
    } while (more)
    resultBuffer
  }

  override def readSnapshot(): Array[Byte] = {
    val line = reader.readLine()
    if (line != SAVE_STATE_OK)
      throw new Exception("Monitor process failed to save state.")
    val state = Files.readAllBytes(tempStateFile)
    Files.delete(tempStateFile)
    state
  }

  override def restored(): Unit = {
    val line = reader.readLine()
    if (line != RESTORE_STATE_OK)
      throw new Exception("Monitor process failed to restore state.")
  }
}
