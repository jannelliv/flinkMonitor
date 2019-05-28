package ch.ethz.infsec.integration

import java.io.BufferedReader
import java.util.{Timer, TimerTask}

import scala.collection.generic.Growable

class ReadingThread(output:BufferedReader, verdicts:Growable[String], process:Process, timeout:Long) extends Thread{
  override def run(): Unit = {
    //TODO: remove timers and try to close the inputreader

    try {
      //there is at least one
      var i=0
      var line = output.readLine()
      while (line != null) {
        //println(line)
        verdicts.synchronized{verdicts+=line}
        line = output.readLine()
      }
    } catch {
      case _:InterruptedException =>
    } finally {
      output.close()
    }

  }
}

class InactivityTimer(p:Process) extends TimerTask {
  override def run(): Unit = {
    p.destroy()
  }
}
