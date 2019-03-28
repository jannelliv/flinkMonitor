package ch.eth.inf.infsec.monitor

import java.io.{BufferedWriter, FileWriter, OutputStream, OutputStreamWriter}
import java.net.Socket

import ch.eth.inf.infsec.StatelessProcessor

class KnowledgeExtract(degree : Int) extends StatelessProcessor[MonpolyRequest,String] with Serializable {
  @transient private var connectedSocket: Socket = _
  @transient private var outputStream : BufferedWriter = _

  var receivedGAPTR = 0
  var highestGAPTR = 0.0
  var receivedGSDMSR = 0
  var highestGSDMSR = 0

  var started = false
  var tempF : FileWriter = null

  override def process(in: MonpolyRequest, f: String => Unit): Unit = {
    if(!started) {
      started = true
      tempF = new FileWriter("knowledgeExtractEvents.log",true)
    }
    tempF.write(in.in + "\n")
    tempF.flush()

    in match {
      case CommandItem(a) => {
        //todo: better parsing
        if(a.startsWith(">gaptr ")) {
          //wait for all, combine and only send highest
          receivedGAPTR = receivedGAPTR + 1
          val candidateHighest = a.drop(">gaptr ".length).trim.dropRight(1).toDouble
          if (candidateHighest > highestGAPTR)
            highestGAPTR = candidateHighest
          if (receivedGAPTR >= degree) {
            try {
              if (outputStream != null) {
                tempF.write("responding: gaptr " + highestGAPTR.toString + "\n")
                tempF.flush()
                outputStream.write("gaptr " + highestGAPTR.toString+"\n")
                outputStream.flush()
              }
            } catch { //todo: error handling
              case e: Exception => {
                tempF.write("ran into an issue when attempting transmitting: "+e+"\n")
              }
            }
            receivedGAPTR = 0
            highestGAPTR = 0.0
          }
        }else if(a.startsWith(">gsdmsr ")) {
          //wait for all, combine and only send highest
          //todo: reconsider if we may want to send fitting instead
          //todo: reconsider if we may want it to be one command instead
          //todo: consider code-dedup
          receivedGSDMSR = receivedGSDMSR + 1
          val candidateHighest = a.drop(">gsdmsr ".length).trim.dropRight(1).toInt
          if (candidateHighest > highestGSDMSR)
            highestGSDMSR = candidateHighest
          if (receivedGSDMSR >= degree) {
            try {
              if (outputStream != null) {
                tempF.write("responding: gsdmsr " + highestGSDMSR.toString + "\n")
                tempF.flush()
                outputStream.write("gsdmsr " + highestGSDMSR.toString+"\n")
                outputStream.flush()
              }
            } catch { //todo: error handling
              case e: Exception => {
                tempF.write("ran into an issue when attempting transmitting: "+e+"\n")
              }
            }
            receivedGSDMSR = 0
            highestGSDMSR = 0
          }
        }else if(a.startsWith(">OutsideInfluenceAddress ")) {
          //we are going to receive this message multiple times, so we ignore it if it happens more than once
          //todo: consider what to do if a connection fails
          if (connectedSocket == null || connectedSocket.isClosed) {
            var addr = a.split("\\s+")(1).trim.dropRight(1)
            var addrSplit = addr.split(":", 2)
            connectedSocket = new Socket(addrSplit(0), addrSplit(1).toInt)
            outputStream = new BufferedWriter(new OutputStreamWriter(connectedSocket.getOutputStream()))
          }
        }else {
          //todo: error
        }
    }
      case EventItem(b) => f(b)
    }
  }

  override def terminate(f: String => Unit): Unit = {
    if(connectedSocket != null)
      connectedSocket.close()
  }
}
