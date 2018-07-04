package ch.eth.inf.infsec.integration

import java.io.BufferedReader
import java.util.{Timer, TimerTask}

import scala.collection.generic.Growable

class ReadingThread(output:BufferedReader, verdicts:Growable[String], process:Process, timeout:Long) extends Thread{
  override def run(): Unit = {
    //TODO: remove timers and try to close the inputreader
    var tt = new InactivityTimer(process)
    var timer = new Timer("MyTimer")
    try {
      //there is at least one
      var i=0
      var line = output.readLine()
      while (line != null) {
        //println(line)
        verdicts.synchronized{verdicts+=line}
        timer.schedule(tt,timeout)
        line = output.readLine()
        timer.cancel()
        timer = new Timer("MyTimer")
        tt = new InactivityTimer(process)
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
