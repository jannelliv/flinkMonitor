package ch.ethz.infsec.monitor

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.file.{Files, Path, Paths}

import ch.ethz.infsec.trace.formatter.MonpolyTraceFormatter
import ch.ethz.infsec.trace.parser.MonpolyVerdictParser
import org.slf4j.{Logger, LoggerFactory}

class MonpolyProcess(val command: Seq[String], val initialStateFile: Option[String], numSources: Int)
  extends AbstractExternalProcess {

  private val GET_INDEX_COMMAND = ">get_pos<\n"
  private val GET_INDEX_REPLY = "Current timepoint:"
  private val SET_SLICER_OK = "Slicing parameters updated"
  private val LOAD_STATE_OK = "Loaded state"
  private val SAVE_STATE_OK = "Saved state"

  @transient private var tempDirectory: Path = _
  @transient private var tempStateFile: Path = _
  @transient private var tempStateFiles: IndexedSeq[Path] = _

  @transient private var pendingSlicer: Boolean = false

  @transient private var logger: Logger = _

  @transient private var formatter: MonpolyTraceFormatter = _
  @transient private var parser: MonpolyVerdictParser = _

  // TODO(JS): Calculate process time average in MonPoly. Then we would not have to synchronize on every database.

  @transient protected var commandAndTimeQueue: java.util.concurrent.LinkedBlockingQueue[Either[Option[Fact], Long]] = _
  @transient private var processTimeMovingAverage = 0.0 // owned by reading interface
  @transient private var memory = ""
  @transient private var numMemReqs = 0
  @transient private var numAptReqs = 0

  override def supportsStateAccess: Boolean = true

  private def initializeCommon(): Unit = {
    logger = LoggerFactory.getLogger(getClass)
    parser = new MonpolyVerdictParser
    commandAndTimeQueue = new java.util.concurrent.LinkedBlockingQueue()

    tempDirectory = Files.createTempDirectory("monpoly-state")
    tempDirectory.toFile.deleteOnExit()
    tempStateFile = tempDirectory.resolve("state.bin")
    tempStateFile.toFile.deleteOnExit()
  }

  private def initializeFormatter(): Unit = {
    formatter = new MonpolyTraceFormatter
    formatter.setMarkDatabaseEnd(true)
  }

  private def openAndLoadState(path: String): Unit = {
    val loadCommand = command ++ List("-load", path)
    open(loadCommand)
    val reply = reader.readLine()
    if (reply != LOAD_STATE_OK)
      throw new Exception("Monitor process failed to load state. Reply: " + reply)
  }

  override def open(): Unit = {
    initializeCommon()
    initializeFormatter()
    initialStateFile match {
      case None =>
        open(command)
        logger.info("Opened Monpoly")
      case Some(path) =>
        openAndLoadState(path)
        logger.info("Opened Monpoly and loaded state from file: {}", path)
    }
  }

  override def openWithState(initialState: Array[Byte]): Unit = {
    initializeCommon()

    val inputStream = new ObjectInputStream(new ByteArrayInputStream(initialState))
    if (inputStream.readBoolean()) {
      logger.info("Restoring formatter from checkpoint")
      formatter = inputStream.readObject().asInstanceOf[MonpolyTraceFormatter]
    } else {
      initializeFormatter()
    }

    val payloadSize = inputStream.readInt()
    logger.info("Restoring MonPoly state with size = {}", payloadSize)
    val payload = new Array[Byte](payloadSize)
    inputStream.readFully(payload)
    Files.write(tempStateFile, payload)
    try {
      openAndLoadState(tempStateFile.toString)
    } finally {
      Files.delete(tempStateFile)
    }

    logger.info("Opened Monpoly with single state")
  }

  override def openAndMerge(initialStates: Iterable[Array[Byte]]): Unit = {
    initializeCommon()
    initializeFormatter()
    createTempFiles(initialStates.size)

    for ((path, state) <- tempStateFiles.zip(initialStates)) {
      val inputStream = new ObjectInputStream(new ByteArrayInputStream(state))
      if (inputStream.readBoolean()) {
        throw new Exception("Process state includes dirty formatter")
      }
      Files.write(path, state)
    }

    val loadCommand = command ++ List("-combine", tempStateFiles.map(_.toString).mkString(","))
    try {
      open(loadCommand)
      val reply = reader.readLine()
      if (reply != LOAD_STATE_OK)
        throw new Exception("Monitor process failed to load state. Reply: " + reply)
    } finally {
      for (file <- tempStateFiles) {
        Files.delete(file)
      }
    }

    logger.info("Opened Monpoly after rescale")
  }

  override def enablesSyncBarrier(in: Fact): Boolean = in.isTerminator || in.isMeta

  private def printFact(fact: Fact): Unit = formatter.printFact(writer.write(_), fact)

  override def writeItem(request: Fact): Unit = {
    if (pendingSlicer) {
      logger.warn("we got a message after pending slicer was set: {}", request)
    }

    if (request.isMeta) {
      if (request.getName == "set_slicer") {
        logger.info("Pending slicer set: {}", request.getArgument(0))
        pendingSlicer = true
        printFact(request)
      }
      commandAndTimeQueue.put(Left(Some(request)))
      writer.write(GET_INDEX_COMMAND)
      writer.flush()
    } else if (request.isTerminator) {
      commandAndTimeQueue.put(Right(System.nanoTime()))
      printFact(request)
      writer.write(GET_INDEX_COMMAND)
      writer.flush()
    } else {
      printFact(request)
    }
  }

  override def writeSyncBarrier(): Unit = {
    commandAndTimeQueue.put(Left(None))
    writer.write(GET_INDEX_COMMAND)
    writer.flush()
  }

  def readResidentialMemory(): Unit = {
    val pid = MonpolyProcessJavaHelper.getPidOfProcess(process)
    if (pid != -1) {
      val lines = Files.readAllLines(Paths.get("/proc/" + pid + "/status"))
      if (lines != null) {
        //java collection, so the java way
        var i = 0
        while (i < lines.size()) {
          val l = lines.get(i)
          if (l.startsWith("VmRSS:")) {
            memory = l.drop("VmRSS:".length).trim.dropRight("kB".length).trim
            return
          }
          i = i + 1
        }
      }
    }
  }

  override def initSnapshot(): Unit = {
    logger.info("we started snapshotting")
    readResidentialMemory()
    val command = if (pendingSlicer)
      Fact.meta("split_save", tempDirectory.resolve("state").toString)
    else
      Fact.meta("save_state", tempStateFile.toString)
    printFact(command)
    writer.flush()
  }

  private def updateProcessTime(x: Long): Unit = {
    processTimeMovingAverage = processTimeMovingAverage * 0.9 + x * 0.1
  }

  override protected def parseResult(line: String, sink: Fact => Unit): Boolean = {
    if (line.startsWith(GET_INDEX_REPLY)) {
      commandAndTimeQueue.take() match {
        case Left(None) => false
        case Left(Some(command)) =>
          command.getName match {
            case "get_apt" =>
              numAptReqs += 1
              if (numAptReqs == numSources) {
                numAptReqs = 0
                sink(Fact.meta("apt", Double.box(processTimeMovingAverage / 1e6)))
              }
            case "get_memory" =>
              numMemReqs += 1
              if (numMemReqs == numSources) {
                numMemReqs = 0
                if (memory == "") {
                  readResidentialMemory()
                }
                sink(Fact.meta("memory", memory))
              }
            case _ => sink(command)
          }
          true
        case Right(start) =>
          updateProcessTime(System.nanoTime() - start)
          true
      }
    } else if(line.startsWith(SET_SLICER_OK)) {
      true
    } else {
      parser.parseLine(sink(_), line)
      true
    }
  }

  override def readSnapshot(): Seq[Array[Byte]] = {
    logger.info("we finish snapshotting")
    assert(parser.inInitialState())

    val line = reader.readLine()
    if (line != SAVE_STATE_OK)
      throw new Exception("Monitor process failed to save state. Reply: " + line)

    if (pendingSlicer) {
      logger.info("pending slicer is set")
      assert(formatter.inInitialState())
      val states = tempStateFiles.map(path => {
        val payload = Files.readAllBytes(path)
        val byteArray = new ByteArrayOutputStream()
        val outputStream = new ObjectOutputStream(byteArray)
        outputStream.writeBoolean(false)
        outputStream.writeInt(payload.length)
        outputStream.write(payload)
        outputStream.close()
        byteArray.toByteArray
      })
      for (path <- tempStateFiles)
        Files.delete(path)
      tempStateFiles = null
      states
    } else {
      logger.info("pending slicer is not set, saving formatter state too")
      val payload = Files.readAllBytes(tempStateFile)
      logger.info("payload length = {}", payload.length)
      val byteArray = new ByteArrayOutputStream()
      val outputStream = new ObjectOutputStream(byteArray)
      outputStream.writeBoolean(true)
      outputStream.writeObject(formatter)
      outputStream.writeInt(payload.length)
      outputStream.write(payload)
      outputStream.close()

      Files.delete(tempStateFile)
      List(byteArray.toByteArray)
    }
  }

  override def dispose(): Unit = {
    super.dispose()
    if (tempStateFiles != null)
      for (file <- tempStateFiles)
        Files.deleteIfExists(file)
    if (tempStateFile != null)
      Files.deleteIfExists(tempStateFile)
    if (tempDirectory != null) {
      Files.deleteIfExists(tempDirectory)
    }
  }

  private def createTempFiles(parallelism: Int): Unit = {
    //println("init tempstatefiles")
    tempStateFiles = (0 until parallelism).map(i => {
      val path = tempDirectory.resolve("state-" + i + ".bin")
      path.toFile.deleteOnExit()
      path
    })
  }
}
