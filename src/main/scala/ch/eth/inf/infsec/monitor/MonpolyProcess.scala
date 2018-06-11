package ch.eth.inf.infsec.monitor

import java.nio.file.{Files, Path}

import scala.collection.mutable

class MonpolyProcess(val command: Seq[String]) extends AbstractExternalProcess[String, String] {
  private val GET_INDEX_COMMAND = ">get_pos<\n"
  private val GET_INDEX_PREFIX = "Current index: "

  private val LOAD_STATE_OK = "Loaded state"

  // TODO(JS): We could pass the filename for saving as an argument, too.
  private val SAVE_STATE_COMMAND = ">save_state \"%s\"<\n"
  private val SAVE_STATE_OK = "Saved state"

  @transient private var tempDirectory: Path = _
  @transient private var tempStateFile: Path = _

  override def open(): Unit = {
    createTempFile()
    open(command)
  }

  override def open(initialState: Array[Byte]): Unit = {
    createTempFile()
    Files.write(tempStateFile, initialState)

    val loadCommand = command ++ List("-load", tempStateFile.toString)
    try {
      open(loadCommand)
      val reply = reader.readLine()
      if (reply != LOAD_STATE_OK)
        throw new Exception("Monitor process failed to load state. Reply: " + reply)
    } finally {
      Files.delete(tempStateFile)
    }
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

  override def readResults(buffer: mutable.Buffer[String]): Unit = {
    var more = true
    do {
      val line = reader.readLine()
      if (line != null) {
        if (line.startsWith(GET_INDEX_PREFIX)) {
          more = false
        } else {
          // TODO(JS): Check that line is a verdict before adding it to the buffer.
          buffer += line
        }
      }
    } while (more)
  }

  override def readSnapshot(): Array[Byte] = {
    val line = reader.readLine()
    if (line != SAVE_STATE_OK)
      throw new Exception("Monitor process failed to save state. Reply: " + line)
    val state = Files.readAllBytes(tempStateFile)
    Files.delete(tempStateFile)
    state
  }

  override def dispose(): Unit = {
    super.dispose()
    if (tempDirectory != null)
      Files.deleteIfExists(tempStateFile)
    if (tempStateFile != null)
      Files.deleteIfExists(tempDirectory)
  }

  private def createTempFile(): Unit = {
    tempDirectory = Files.createTempDirectory("monpoly-state")
    tempDirectory.toFile.deleteOnExit()
    tempStateFile = tempDirectory.resolve("state.bin")
    tempStateFile.toFile.deleteOnExit()
  }
}
