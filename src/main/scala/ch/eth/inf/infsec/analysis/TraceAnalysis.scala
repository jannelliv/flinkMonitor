package ch.eth.inf.infsec.analysis


import scala.collection.immutable.ListSet
import java.io.PrintWriter
import java.nio.file.{Files, Path}
import java.io.File
import java.util

import org.apache.flink.api.java.utils.ParameterTool
import ch.eth.inf.infsec.SlicingSpecification
import ch.eth.inf.infsec.StreamMonitoring.formula
import ch.eth.inf.infsec.policy.{Formula, Policy}

import scala.collection.mutable
import scala.io.Source

object TraceAnalysis {
  class AnalysisPreparation(analysisDirectory: String, formula: Formula, traceLength: Int,  window: Int, degree: Int){
    private val analysisDir: File = new File(analysisDirectory)
    private val traceLengthInSec: Int = traceLength * window
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

    def getAnalysisTraceFolders: Iterable[Path] = {
      if (analysisDir.exists && analysisDir.isDirectory)
        analysisDir.listFiles.filter(!_.isFile).map(_.toPath).toIterable
      else
        throw new Exception("%s does not exist or is not a directory".format(analysisDir.toString))
    }

    def extractHeavyParts(heavy: Path, dir: Path): Unit = {
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
          boundary = startTs + window
          writer = new PrintWriter(Files.newBufferedWriter(createTmpFile(dir, startTs.toString)))
        }

        val value = ratesMap.getOrElseUpdate((arr(1), arr(2), arr(3)), 0)
        ratesMap.put((arr(1), arr(2), arr(3)), value + 1)

        if(currTs > boundary){
          boundary += window
          writeMapToFile(writer, ratesMap)
          ratesMap = new mutable.HashMap[(String, String, String), Int]().withDefaultValue(0)
          writer = new PrintWriter(Files.newBufferedWriter(createTmpFile(dir, currTs.toString)))
        }

        line = reader.readLine()
      }

      writeMapToFile(writer, ratesMap)
      writer.close()
      reader.close()
    }

    def extractParts(heavy: Path, rates: Path, endTs: Int): Unit = {
      var line: String = null

      wipeTempFiles()
      def extractAllParts(input: Path, dir: Path): Unit = {
        var startTs = 0
        var currTs: Int = 0
        var boundary = 0
        val reader = Files.newBufferedReader(input)

        var writer: PrintWriter = null

        line = reader.readLine()
        while (line != null) {
          currTs = line.split(",")(0).toInt

          if(startTs == 0){
            startTs = currTs
            boundary = startTs + window
            writer = new PrintWriter(Files.newBufferedWriter(createTmpFile(dir, startTs.toString)))
          }

          if(currTs > boundary && line != null){
            boundary += window
            writer.close()
            writer = new PrintWriter(Files.newBufferedWriter(createTmpFile(dir, currTs.toString)))
            writer.println(line)
          }else writer.println(line)

          line = reader.readLine()
        }

        writer.close()
        reader.close()
      }

      extractHeavyParts(heavy, tmpHeavyDir)
      extractAllParts(rates, tmpRatesDir)
    }

    def processParts: Iterable[String] = {
      val heavyFiles = getFiles(tmpHeavyDir.toFile).toArray
      val ratesFiles = getFiles(tmpRatesDir.toFile).toArray

      if(heavyFiles.length != ratesFiles.length) throw new Exception("Mismatch in number of analysis files")

      var slicers = new mutable.MutableList[String]
      for (i <- heavyFiles.indices ){
        slicers += getSlicer(heavyFiles(i).toString, getRatesFromFile(ratesFiles(i)))
      }
      slicers
    }

    def produceSlicersOfTrace(folder: Path, insertIntoTrace: Boolean): Unit = {
      val heavyTrace: Path = new File("%s/%s".format(folder.toString, "heavy-trace-wrapped")).toPath
      val ratesTrace: Path = new File("%s/%s".format(folder.toString, "rates-trace")).toPath
      val timestamp = extractTs(folder)

      extractParts(heavyTrace, ratesTrace, timestamp)

      val slicers = processParts
      val outputFile = createTmpFile(folder, "slicers")
      writeTempFile(outputFile, slicers)

      if (insertIntoTrace) {
        val logTrace:   Path = new File("%s/%s".format(folder.toString, "log-trace.csv")).toPath
        createSlicedCopyOfTrace(logTrace, createFile(folder, "log-trace-sliced.csv"), slicers)
      }
    }

    def createSlicedCopyOfTrace(input: Path, output: Path, slicerIt: Iterable[String]): Unit = {
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
          boundary = startTs + window
          writer.println(slicers.head)
          slicers = slicers.tail
        }

        if(currTs > boundary && line != null){
          boundary += window
          if(!slicers.isEmpty) {
            writer.println(slicers.head)
            slicers = slicers.tail
          }
        }

        writer.println(line)
        line = reader.readLine()
      }

      writer.close()
      reader.close()
    }

    def produceSlicers(): Unit = {
      setupWorkingDirectory()

      val traces = getAnalysisTraceFolders
      traces.foreach(produceSlicersOfTrace(_, insertIntoTrace = true))

      clearWorkingDirectory()
    }

    def getSlicer(heavy: String, rates: String): String = {
      val arguments = Array[String]("--heavy", heavy, "--rates", rates)
      val params = ParameterTool.fromArgs(arguments)
      SlicingSpecification.mkSlicer(params, formula, degree).stringify()
    }
  }

  def createTmpFile(dir: Path, name: String): Path = {
    val file = dir.resolve(name)
    file.toFile.deleteOnExit()
    file
  }

  def createFile(dir: Path, name: String): Path = {
    val file = dir.resolve(name)
    file
  }

  def getFiles(dir: File): Iterable[Path] = {
    if (dir.exists && dir.isDirectory)
      dir.listFiles.filter(_.isFile).sortWith(_.getName.toInt < _.getName.toInt).map(_.toPath).toIterable
    else
      throw new Exception("%s does not exist or is not a directory".format(dir.toString))
  }

  val arr = Array("select", "insert", "updated", "delete", "script_start", "script_end", "script_svn", "script_md5", "commit")

  def getRatesFromFile(file: Path): String = {
    val ratesMap = new mutable.HashMap[String, Int]().withDefaultValue(0)
    arr.map(ratesMap.put(_, 0))

    val content = Files.readAllLines(file)
    var current = 0
    content.toArray.foreach(e => {
      val relation = e.asInstanceOf[String].split(",")(1)
      current = ratesMap.getOrElseUpdate(relation, 0)
      ratesMap.update(relation, current+1)
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

    val traceLength = params.getInt("traceLength")
    val window = params.getInt("window")
    val degree = params.getInt("degree")

    if(traceLength <= 0 || window <= 0 || degree <= 0) {
      println("Integer values must be positive")
      sys.exit(1)
    }

    val prep = new AnalysisPreparation(params.get("directory"), parseFormula(params.get("formula")) ,traceLength, window, degree)

    prep.produceSlicers()
  }

  def parseFormula(file: String): Formula = {
    val formulaSource = Source.fromFile(file).mkString
    formula = Policy.read(formulaSource) match {
      case Left(err) =>
        println("Cannot parse formula")
        sys.exit(1)
      case Right(phi) => phi
    }
    formula
  }
}
