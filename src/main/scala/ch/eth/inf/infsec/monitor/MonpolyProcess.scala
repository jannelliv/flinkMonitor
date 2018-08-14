package ch.eth.inf.infsec.monitor

import java.nio.file.{Files, Path}
import java.io.File

import scala.collection.immutable.ListSet
import scala.collection.mutable

class MonpolyProcess(val command: Seq[String]) extends AbstractExternalProcess[String, String] {
  private val GET_INDEX_COMMAND = ">get_pos<\n"
  private val GET_INDEX_PREFIX = "Current index: "

  private val LOAD_STATE_OK = "Loaded state"
  private val COMBINED_STATE_OK = "Combined state"

  // TODO(JS): We could pass the filename for saving as an argument, too.
  private val SET_SLICER_COMMAND = ">set_slicer %s<\n"
  private val SPLIT_SAVE_COMMAND = ">split_save %s<\n"
  private val SAVE_STATE_COMMAND = ">save_state \"%s\"<\n"
  private val SAVE_STATE_OK = "Saved state"

  @transient private var tempDirectory: Path = _
  @transient private var tempStateFile: Path = _
  @transient private var tempStateFiles: ListSet[Path] = _

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

  override def open(initialStates: Iterable[(Int, Array[Byte])]): Unit = {
    createTempFiles(initialStates.size)

    var states = initialStates

    for(file <- tempStateFiles) {
      Files.write(file, states.head._2)
      states = states.tail
    }

    val loadCommand = command ++ List("-combine", tempStateFiles.map(_.toString).mkString(","))
    try {
      open(loadCommand)
      val reply = reader.readLine()
      if (reply != COMBINED_STATE_OK)
        throw new Exception("Monitor process failed to load state. Reply: " + reply)
    } finally {
      for(file <- tempStateFiles) {Files.delete(file) }
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

  override def initSnapshot(slicer: String): Unit = {
    writer.write(SET_SLICER_COMMAND.format(slicer))
    writer.write(SPLIT_SAVE_COMMAND.format(tempDirectory.toString + "/state"))
    println(SPLIT_SAVE_COMMAND.format(tempDirectory.toString + "/state"))
    writer.flush()
  }

  override def readResults(buffer: mutable.Buffer[String]): Unit = {
    var more = true
    do {
      val line = reader.readLine()
      if (line == null || line.startsWith(GET_INDEX_PREFIX)) {
          more = false
      } else {
        // TODO(JS): Check that line is a verdict before adding it to the buffer.
        buffer += line
      }
    } while (more)
  }

  override def drainResults(buffer: mutable.Buffer[String]): Unit = {
    var more = true
    do {
      val line = reader.readLine()
      if (line == null)
        more = false
      else
      // TODO(JS): Check that line is a verdict before adding it to the buffer.
        buffer += line
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


  override def readSnapshots(): Iterable[(Int, Array[Byte])] = {
    var line = reader.readLine()
    while(line == "") line = reader.readLine()
    if (line != SAVE_STATE_OK)
      throw new Exception("Monitor process failed to save state. Reply: " + line)

    var states = new ListSet[(Int, Array[Byte])]

    val files = getFilesOfStates(tempDirectory.toFile, "bin")
    for(file <- files){
      val tuple = (extractPartitionDigits(file.getName), Files.readAllBytes(file.toPath))
      states += tuple
    }

    states
  }

  private def createTempFiles(parallelism: Int): Unit = {
    tempStateFiles = new ListSet[Path]
    tempDirectory = Files.createTempDirectory("monpoly-state")
    tempDirectory.toFile.deleteOnExit()

    var i = 0
    while(i < parallelism){
      val tmp = tempDirectory.resolve("state-" + i + ".bin")
      tmp.toFile.deleteOnExit()
      tempStateFiles += tmp
      i += 1
    }
  }

  private def getFilesOfStates(dir: File, extension: String): List[File] = {
    if (dir.exists && dir.isDirectory)
      dir.listFiles.filter(_.isFile).toList.filter {file => file.getName.endsWith(extension)}
    else throw new Exception("File does not exist or is not a directory")
  }

  private def extractPartitionDigits(str: String): Int = {
      val digits = str.replaceAll("\\D+","")
      if(digits == "") 0
      else Integer.parseInt(digits)
  }
}
