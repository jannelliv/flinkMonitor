package ch.eth.inf.infsec.monitor

import java.nio.file.{Files, Path, Paths}
import java.io.{File, FileWriter}
import java.util

import org.slf4j.LoggerFactory

import scala.collection.immutable.ListSet
import scala.collection.mutable

class MonpolyProcess(val command: Seq[String]) extends AbstractExternalProcess[MonpolyRequest, MonpolyRequest] {
  private val GET_INDEX_COMMAND = ">get_pos<\n"
  private val GET_INDEX_PREFIX = "Current timepoint:"
  private val SLICING_PREFIX = "Slicing "

  private val COMMAND_PREFIX = ">"
  private val COMMAND_SUFFIX = "<"

  private val LOAD_STATE_OK = "Loaded state"
  private val COMBINED_STATE_OK = "Combined state"

  // TODO(JS): We could pass the filename for saving as an argument, too.
  private val SET_SLICER_COMMAND = ">set_slicer %s<\n"
  private val SPLIT_SAVE_COMMAND = ">split_save %s<\n"
  private val SAVE_STATE_COMMAND = ">save_state %s<\n"
  private val SAVE_STATE_OK = "Saved state"

  @transient private var tempDirectory: Path = _
  @transient private var tempStateFile: Path = _
  @transient private var tempStateFiles: ListSet[Path] = _

  @transient private var pendingSlicer: Boolean = _

  private val logger = LoggerFactory.getLogger(getClass)

  private var lastShutdownInitiatedTime : Long = 0
  private var lastShutdownCompletedTime : Long = 0
  def handleReopenTime() : Unit = {
    lastShutdownCompletedTime = System.currentTimeMillis()
    //if we didn't do a  shutdown before, we want the delta to be 0
    if(lastShutdownInitiatedTime == 0) {
      lastShutdownCompletedTime = 0
    }
  }

  override def open(): Unit = {
    createTempFile()
    open(command)
    handleReopenTime
    println("Opened Monpoly")
  }

  override def open(initialState: Array[Byte]): Unit = {
    createTempFile()
    Files.write(tempStateFile, initialState)

    val loadCommand = command ++ List("-load", tempStateFile.toString)
    try {
      open(loadCommand)
      val reply =  reader.readLine()
      if (reply != LOAD_STATE_OK)
        throw new Exception("Monitor process failed to load state. Reply: " + reply)
    } finally {
      Files.delete(tempStateFile)
    }
    handleReopenTime
    println("Opened Monpoly with single state")
  }

  override def open(initialStates: Iterable[(Int, Array[Byte])]): Unit = {
    createTempFile()
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
      if (reply != LOAD_STATE_OK)
        throw new Exception("Monitor process failed to load state. Reply: " + reply)
    } finally {
      for(file <- tempStateFiles) {Files.delete(file) }
    }
    handleReopenTime
    println("Opened Monpoly after rescale")
  }

  private val indexCommandTimingBuffer = new java.util.concurrent.LinkedBlockingQueue[Either[CommandItem,Long]]()
  private var processTimeMovingAverage = 0.0
  def yes(x: Long): Unit = {
    processTimeMovingAverage = processTimeMovingAverage * 0.9 + x * 0.1
  }

/*  var started = false
  var tempF : FileWriter = null*/



  override def writeRequest(request: MonpolyRequest): Unit = {
/*    if(!started) {
      started = true
      tempF = new FileWriter("monpolyIn"+(process.hashCode()%10000)+".log",false)
    }
    tempF.write(request.in)
    tempF.flush()*/

    //canHandle represents wether this is something for monpoly or for further down in the pipeline
    var canHandle = false
    request match {
      case r@CommandItem(request.in) =>
        if(request.in.startsWith(">set_slicer")) {
          logger.info("Pending Slicer set")
          pendingSlicer = true
          canHandle = true
          var tempF = new FileWriter("monpolyError.log",true)
          tempF.write("we set slicer to true, slicer:"+request.in+"\n")
          tempF.flush()
          tempF.close()
        }else{
          indexCommandTimingBuffer.put(Left(r))
        }
      case EventItem(request.in) => canHandle = true
    }

    if(canHandle)
    {
      try {

        indexCommandTimingBuffer.put(Right(System.currentTimeMillis()))
        writer.write(request.in)
        writer.write(GET_INDEX_COMMAND)
        // TODO(JS): Do not flush if there are more requests in the queue
        writer.flush()
      }catch{
        case e:Throwable => {
          var tempF = new FileWriter("monpolyError.log",true)
          tempF.write("ran into error in monpoly at request: "+request.toString+"\nhad pending slicer set?"+pendingSlicer+"\n")
          tempF.flush()
          tempF.close()
          throw e
        }
      }
    }
  }

  var memory = ""

  def readResidentialMemory(): Unit = {
    var pid = MonpolyProcessJavaHelper.getPidOfProcess(process)
    if(pid != -1) {
      var lines = Files.readAllLines(Paths.get("/proc/"+pid+"/status"))
      if(lines != null) {
        //java collection, so the java way
        var i = 0
        while(i < lines.size()) {
          var l = lines.get(i)
          if(l.startsWith("VmRSS:")) {
            memory = l.drop("VmRSS:".length).trim.dropRight("kB".length).trim
            return
          }
          i = i+1
        }
      }
    }
  }

  def snapshotHelper(): Unit = {
    lastShutdownInitiatedTime = System.currentTimeMillis()
    readResidentialMemory()
    var tempF = new FileWriter("monpolyError.log",true)
    tempF.write("we started snapshotting\n")
    tempF.flush()
    tempF.close()
  }

  override def initSnapshot(): Unit = {
    snapshotHelper()
    var command = ""
    if(pendingSlicer) command = SPLIT_SAVE_COMMAND.format(tempDirectory.toString + "/state")
    else command = SAVE_STATE_COMMAND.format(tempStateFile.toString)
    writer.write(command)
    writer.flush()
  }

  override def initSnapshot(slicer: String): Unit = {
    snapshotHelper()
    writer.write(SET_SLICER_COMMAND.format(slicer))
    writer.write(SPLIT_SAVE_COMMAND.format(tempDirectory.toString + "/state"))
    writer.flush()
  }

/*  var started2 = false
  var tempF2 : FileWriter = null*/




  override def readResults(buffer: mutable.Buffer[MonpolyRequest]): Unit = {
/*    if(!started2) {
      started2 = true
      tempF2 = new FileWriter("monpolyOut"+(process.hashCode()%10000)+".log",false)
    }*/

    var more = true
    do {

      val line = reader.readLine()

/*      if(line != null) {
        tempF2.write(line+"\n")
        tempF2.flush()
      }*/

      while(!indexCommandTimingBuffer.isEmpty && indexCommandTimingBuffer.peek().isLeft)
      {
        val com = indexCommandTimingBuffer.poll().left.get
/*        tempF2.write("command got to other side: "+com+"\n")
        tempF2.flush()*/
        if(com.in.startsWith(">gapt"))
        {
/*          tempF2.write("response: "+">gaptr "+processTimeMovingAverage.toString()+"<\n")
          tempF2.flush()*/
          buffer += CommandItem(">gaptr "+processTimeMovingAverage.toString()+"<")
        }else if(com.in.startsWith(">gsdt")) {
/*          tempF2.write(">gsdtr " + (lastShutdownCompletedTime - lastShutdownInitiatedTime).toString() + "<\n")
          tempF2.flush()*/
          buffer += CommandItem(">gsdtr " + (lastShutdownCompletedTime - lastShutdownInitiatedTime).toString() + "<")
        }else if(com.in.startsWith(">gsdms")){
          if(memory == "")
            readResidentialMemory()
/*          tempF2.write(">gsdmsr " + memory + "<\n")
          tempF2.flush()*/
          buffer += CommandItem(">gsdmsr " + memory + "<")
        }else {
          buffer += com
        }
      }

      if (line == null) {
        more = false
      } else if (line.startsWith(GET_INDEX_PREFIX)) {
        more = false
/*        tempF2.write("non-empty buffer: "+(!indexCommandTimingBuffer.isEmpty)+"\n")
        if(indexCommandTimingBuffer.isEmpty) {
          tempF2.write("indexCommandTimingBuffer was empty? On line: "+line+"\n")
        }
        tempF2.flush()*/
        //todo: in theory this nonEmpty-check should not be necessary, but something's being strange still
        if(!indexCommandTimingBuffer.isEmpty) {
          val k = indexCommandTimingBuffer.poll()
/*          tempF2.write("k is right? " + k.isRight + "\n")
          tempF2.flush()*/
          yes(System.currentTimeMillis() - k.right.get)
        }
      }else{
          // TODO(JS): Check that line is a verdict before adding it to the buffer.
          buffer += EventItem(line)
      }
/*      tempF2.write("Content in buffer: \n")
      buffer.foreach(abc => tempF2.write(abc+"\n"))
      tempF2.flush()*/
    } while (more)
/*    tempF2.write("done with loop!\n")
    tempF2.flush()*/
  }

  override def drainResults(buffer: mutable.Buffer[MonpolyRequest]): Unit = {
    var more = true
    do {

      val line = reader.readLine()

      while(!indexCommandTimingBuffer.isEmpty && indexCommandTimingBuffer.peek().isLeft)
      {
        val com = indexCommandTimingBuffer.poll().left.get
        if(com.in.startsWith(">gapt"))
        {
          buffer += CommandItem(">gaptr "+processTimeMovingAverage.toString()+"<")
        }else{
          buffer += com
        }
      }

      if (line == null) {
        more = false
      } else if (line.startsWith(GET_INDEX_PREFIX)) {
        //the only change in drainResults compared to readResults should be that this line is commented out
        //more = false
        //todo: in theory this nonEmpty-check should not be necessary, but something's being strange still
        if(!indexCommandTimingBuffer.isEmpty) {
          val k = indexCommandTimingBuffer.poll()
          assert(k.isRight)
          yes(System.currentTimeMillis() - k.right.get)
        }
      }else{
        // TODO(JS): Check that line is a verdict before adding it to the buffer.
        buffer += EventItem(line)
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
    if(tempStateFiles != null)
      for(file <- tempStateFiles)
        Files.deleteIfExists(file)
    if (tempStateFile != null)
      Files.deleteIfExists(tempStateFile)
    if (tempDirectory != null) {
      deleteFilesOfFolder(tempDirectory.toFile)
      Files.deleteIfExists(tempDirectory)
    }
  }

  private def createTempFile(): Unit = {
    tempDirectory = Files.createTempDirectory("monpoly-state")
    tempDirectory.toFile.deleteOnExit()
    tempStateFile = tempDirectory.resolve("state.bin")
    tempStateFile.toFile.deleteOnExit()
  }


  override def readSnapshots(): Iterable[(Int, Array[Byte])] = {
    val line = reader.readLine()
    if(line != SAVE_STATE_OK)
      throw new Exception("Monitor process failed to save state. Reply: " + line)

    var states = new ListSet[(Int, Array[Byte])]

    if(!pendingSlicer){
      states += ((0, Files.readAllBytes(tempStateFile)))
      Files.delete(tempStateFile)
    }else {
      val files = getFilesOfStates(tempDirectory.toFile, "bin")
      for (file <- files) {
        val digits = extractPartitionDigits(file.getName)
        if (digits >= 0) states += ((extractPartitionDigits(file.getName), Files.readAllBytes(file.toPath)))
      }
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

  private def deleteFilesOfFolder(dir: File): Unit = {
    if (dir.exists && dir.isDirectory)
      dir.listFiles.map(f => Files.deleteIfExists(f.toPath))
    else throw new Exception("File does not exist or is not a directory")
  }

  private def getFilesOfStates(dir: File, extension: String): List[File] = {
    if (dir.exists && dir.isDirectory)
      dir.listFiles.filter(_.isFile).toList.filter {file => file.getName.endsWith(extension)}
    else throw new Exception("File does not exist or is not a directory")
  }

  private def extractPartitionDigits(str: String): Int = {
    val digits = str.replaceAll("\\D+","")
    if(digits == "") -1
    else Integer.parseInt(digits)
  }
}
