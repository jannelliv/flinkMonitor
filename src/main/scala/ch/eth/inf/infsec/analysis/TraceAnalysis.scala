package ch.eth.inf.infsec.analysis


import java.io.PrintWriter
import java.nio.file.{Files, Path, Paths}
import java.io.File
import org.apache.flink.api.java.utils.ParameterTool
import ch.eth.inf.infsec.SlicingSpecification
import ch.eth.inf.infsec.StreamMonitoring.formula
import ch.eth.inf.infsec.policy.{Formula, Policy}

import scala.collection.mutable
import scala.io.Source

object TraceAnalysis {
  class AnalysisPreparation(analysisDir: Path, outputDir: Path, formula: Formula, var degree: Int, windows: Int){
    private var tmpHeavyDir: Path = _
    private var tmpRatesDir: Path = _

    def setupWorkingDirectory(): Unit = {
      tmpHeavyDir = Files.createTempDirectory("heavy-dir")
      tmpHeavyDir.toFile.deleteOnExit()
      tmpRatesDir = Files.createTempDirectory("rates-dir")
      tmpRatesDir.toFile.deleteOnExit()
    }

    def clearWorkingDirectory(): Unit = {
      wipeTempFiles()
      if (tmpHeavyDir != null) Files.deleteIfExists(tmpHeavyDir)
      if (tmpRatesDir != null) Files.deleteIfExists(tmpRatesDir)
    }

    def wipeTempFiles(): Unit = {
      def wipeDirectory(dir: File): Unit = {
        if (dir != null && dir.exists () && dir.isDirectory)
          dir.listFiles ().map (_.delete ())
      }
      wipeDirectory(tmpHeavyDir.toFile)
      wipeDirectory(tmpRatesDir.toFile)
    }

    def writeTempFile(file: Path, lines: Iterable[String]): Unit = {
      val writer = new PrintWriter(Files.newBufferedWriter(file))
      for (line <- lines) {
        writer.println(line)
      }
      writer.close()
    }

    //def getAnalysisTraceFolders: Iterable[Path] = {
    //  if (analysisDir.exists && analysisDir.isDirectory)
    //    analysisDir.listFiles.filter(!_.isFile).map(_.toPath).toIterable
    //  else
    //    throw new Exception("%s does not exist or is not a directory".format(analysisDir.toString))
    //}

    def extractHeavyParts(heavy: Path, dir: Path, windowSize: Int): Unit = {
      def writeMapToFile(writer: PrintWriter, map: mutable.Map[(String, String, String), Int]): Unit = {
        map.foreach(t => if(t._2 > 0) writer.println("%s,%s,%s".format(t._1._1, t._1._2, t._1._3)))
        writer.close()
      }
      var ratesMap = new mutable.HashMap[(String, String, String), Int]().withDefaultValue(0)
      val reader = Files.newBufferedReader(heavy)

      var line: String = null
      var boundary = 0
      var startTs = 0
      var currTs = 0

      var writer: PrintWriter = null
      var arr: Array[String] = null

      line = reader.readLine()
      while (line != null) {
        arr = line.split(",")
        currTs = arr(0).toInt

        if(startTs == 0){
          startTs = currTs
          boundary = startTs + windowSize
          writer = new PrintWriter(Files.newBufferedWriter(createTmpFile(dir, startTs.toString)))
        }

        val value = ratesMap.getOrElseUpdate((arr(1), arr(2), arr(3)), 0)
        ratesMap.put((arr(1), arr(2), arr(3)), value + 1)

        if(currTs > boundary){
          boundary += windowSize
          writeMapToFile(writer, ratesMap)
          ratesMap = new mutable.HashMap[(String, String, String), Int]().withDefaultValue(0)
          writer = new PrintWriter(Files.newBufferedWriter(createTmpFile(dir, currTs.toString)))
        }

        line = reader.readLine()
      }
      writeMapToFile(writer, ratesMap)
      reader.close()
    }

    def extractTraceParts(input: Path, dir: Path, windowSize: Int): Unit = {
      var startTs = 0
      var currTs: Int = 0
      var boundary = 0
      val reader = Files.newBufferedReader(input)

      var line: String = null
      var writer: PrintWriter = null

      line = reader.readLine()
      while (line != null && line.trim != "") {
        currTs = line.split(",")(0).toInt

        if(startTs == 0){
          startTs = currTs
          boundary = startTs + windowSize
          writer = new PrintWriter(Files.newBufferedWriter(createTmpFile(dir, startTs.toString)))
        }

        if(currTs > boundary && line != null){
          boundary += windowSize
          writer.close()
          writer = new PrintWriter(Files.newBufferedWriter(createTmpFile(dir, currTs.toString)))
          writer.println(line)
        }else writer.println(line)

        line = reader.readLine()
      }

      writer.close()
      reader.close()
    }

    def extractParts(heavy: Path, rates: Path, windowSize: Int): Unit = {
      wipeTempFiles()

      extractHeavyParts(heavy, tmpHeavyDir, windowSize)
      extractTraceParts(rates, tmpRatesDir, windowSize)
    }

    def processParts: Iterable[String] = {
      val heavyFiles = getFiles(tmpHeavyDir.toFile).toArray
      val ratesFiles = getFiles(tmpRatesDir.toFile).toArray

      println("Heavy files: %d; Rates files: %d; Windows: %d".format(heavyFiles.length, ratesFiles.length, windows))
      if(heavyFiles.length != ratesFiles.length) throw new Exception("Mismatch in number of analysis files")
      if(heavyFiles.length != windows) throw new Exception("Mismatch in number of analysis files and windows")

      var slicers = new mutable.MutableList[String]
      for (i <- heavyFiles.indices ){
        slicers += getSlicer(heavyFiles(i).toString, getRatesFromFile(ratesFiles(i)))
      }
      slicers
    }

    def produceSlicersOfTrace(timestamp: Int, windowSize: Int, insertIntoTrace: Boolean): Unit = {
      val heavyTrace = Paths.get("%s/statistics/%s".format(analysisDir.toString, "heavy-trace-%d.csv".format(windows)))
      val ratesTrace = Paths.get("%s/statistics/%s".format(analysisDir.toString, "rates-trace-%d.csv".format(windows)))

      extractParts(heavyTrace, ratesTrace, windowSize)

      val slicers = processParts
      val defaultSlicers = new mutable.MutableList[String]
      val defaultSlicer = getDefaultSlicer()
      slicers.foreach(_ => defaultSlicers += defaultSlicer)
      //deleteIfExists(createFile(outputDir, "slicers"))
      //val outputFile = createFile(outputDir, "slicers")
      //writeTempFile(outputFile, slicers)

      if(slicers.size != windows) throw new Exception("Error: %d windows and %d slicers".format(windows, slicers.size))

      if (insertIntoTrace) {
        val logTrace:   Path = new File("%s/%s".format(analysisDir.toString, "ldcc_sample.csv")).toPath
        createSlicedCopyOfTrace(windowSize, logTrace, createFile(outputDir, "log-trace-predictive.csv"), slicers, predictive = true)
        createSlicedCopyOfTrace(windowSize, logTrace, createFile(outputDir, "log-trace-reactive.csv"), slicers, predictive = false)
        createSlicedCopyOfTrace(windowSize, logTrace, createFile(outputDir, "log-trace-static.csv"), defaultSlicers, predictive = true)
      }
    }

    def createSlicedCopyOfTrace(windowSize: Int, input: Path, output: Path, slicerIt: Iterable[String], predictive: Boolean): Unit = {
      var slicersInserted = 0
      var slicers = slicerIt
      var currTs = 0
      var startTs = 0
      var boundary = 0

      val reader = Files.newBufferedReader(input)
      val writer = new PrintWriter(Files.newBufferedWriter(output))

      var line = reader.readLine()
      while (line != null) {
        currTs = line.split(",")(2).split("=")(1).trim.toInt

        if(startTs == 0){
          startTs = currTs
          boundary = startTs + windowSize
          if(predictive)
            slicers = slicers.tail
        }

        if(currTs > boundary && line != null){
          boundary += windowSize
          if(slicers.nonEmpty && slicersInserted <= windows -2) {
            writer.println(">set_slicer %s<".format(slicers.head))
            println(">set_slicer %s<".format(slicers.head))
            slicersInserted += 1
            slicers = slicers.tail
          }
        }

        writer.println(line)
        line = reader.readLine()
      }

      if(slicersInserted != windows -1) throw new Exception("Mismatch between windows (%d) and slicers inserted (%d) should only be 1".format(windows, slicersInserted))
      writer.close()
      reader.close()
    }

    def produceSlicersForExperiment(startTs: Int, endTs: Int): Unit = {
      setupWorkingDirectory()

      val windowSize = (endTs - startTs) / this.windows

      produceSlicersOfTrace(endTs, windowSize, insertIntoTrace = true)

      clearWorkingDirectory()
    }

    def getSlicer(heavy: String, rates: String): String = {
      val arguments = Array[String]("--heavy", heavy, "--rates", rates)
      val params = ParameterTool.fromArgs(arguments)
      SlicingSpecification.mkSlicer(params, formula, degree).stringify()
    }

    def getDefaultSlicer(): String = {
      val params = ParameterTool.fromArgs(Array[String]())
      SlicingSpecification.mkSlicer(params, formula, degree).stringify()
    }
  }

  def createTmpFile(dir: Path, name: String): Path = {
    val file = dir.resolve(name)
    file.toFile.deleteOnExit()
    file
  }

  def createFile(dir: Path, name: String): Path = {
    val path = Paths.get("%s/%s".format(dir.toString, name))
    deleteIfExists(path)

    Files.createFile(path)
  }

  def deleteIfExists(path: Path): Unit = {
    if(Files.exists(path))
      Files.delete(path)
  }

  def getFiles(dir: File): Iterable[Path] = {
    if (dir.exists && dir.isDirectory)
      dir.listFiles.filter(_.isFile).sortWith(_.getName.toInt < _.getName.toInt).map(_.toPath).toIterable
    else
      throw new Exception("%s does not exist or is not a directory".format(dir.toString))
  }

  val arr = Array("select", "insert", "update", "delete", "script_start", "script_end", "script_svn", "script_md5", "commit")

  def getRatesFromFile(file: Path): String = {
    val ratesMap = new mutable.HashMap[String, Int]().withDefaultValue(0)
    arr.map(ratesMap.put(_, 0))

    val content = Files.readAllLines(file)
    var current = 0

    var elems: Array[String] = null
    var relation: String = null
    content.toArray.foreach(e => {
      elems = e.asInstanceOf[String].split(",")
      relation = elems(1)
      current = ratesMap.getOrElseUpdate(relation, 0)
      current += Integer.parseInt(elems(2))
      ratesMap.update(relation, current)
    })

   formatRatesMap(ratesMap)
  }

  def formatRatesMap(map: mutable.Map[String, Int]): String = {
    val sb = StringBuilder.newBuilder
    val it = map.iterator

    var entry: (String, Int) = null
    while(it.hasNext){
      entry = it.next()
      sb.append("%s=%d".format(entry._1, entry._2))
      if(it.hasNext) sb.append(",")
    }

    sb.toString()
  }

  def extractTs(path: Path): Int = {
    path.toString.split("/").last.toInt
  }

  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)

    prepareSimulation(params)
  }

  def prepareSimulation(params: ParameterTool): Unit = {
    val degrees = params.getInt("parallelism")
    val windows = params.getInt("windows")

    val inputDir = new File(params.get("inputDir")).toPath
    val outputDir = new File(params.get("outputDir")).toPath
    val f = params.get("formula")

    val formula = parseFormula("%s/nokia/%s.mfotl".format(params.get("inputDir"), f))

    println("Preparing files for configuration: parallelism=%d, windows=%d".format(degrees,windows))
    val prep = new AnalysisPreparation(inputDir, outputDir, formula, degrees, windows)
    prep.produceSlicersForExperiment(startTs = 1282921200, endTs = 1283101200)

  }

  def createDir(dir: Path, name: String): Path = {
    val path = Paths.get("%s/%s".format(dir.toString, name))
    deleteIfExists(path)

    Files.createDirectory(path)
  }

  def createDirIfNotExists(dir: Path, name: String): Path = {
    val path = Paths.get("%s/%s".format(dir.toString, name))
    if(!Files.exists(path))
      Files.createDirectory(path)
    else
      path
  }

  def parseFormula(file: String): Formula = {
    val formulaSource = Source.fromFile(file).mkString
    formula = Policy.read(formulaSource) match {
      case Left(_) =>
        println("Cannot parse formula")
        sys.exit(1)
      case Right(phi) => phi
    }
    formula
  }
}
