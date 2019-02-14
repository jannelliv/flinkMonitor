package ch.eth.inf.infsec.monitor

import java.io.{BufferedWriter, OutputStream, OutputStreamWriter}
import java.net.Socket

import ch.eth.inf.infsec.StatelessProcessor

class KnowledgeExtract(degree : Int) extends StatelessProcessor[MonpolyRequest,String] with Serializable {
  @transient private var connectedSocket: Socket = _
  @transient private var outputStream : BufferedWriter = _

  var receivedGAPTR = 0
  var highestGAPTR = 0.0
  var receivedGSDMSR = 0
  var highestGSDMSR = 0

  override def process(in: MonpolyRequest, f: String => Unit): Unit = {
    in match {
      case CommandItem(a) => {
        //todo: better parsing
        if(a.startsWith(">gaptr ")) {
          //wait for all, combine and only send highest
          receivedGAPTR = receivedGAPTR + 1
          val candidateHighest = a.drop(">gaptr ".length).dropRight(1).toDouble
          if (candidateHighest > highestGAPTR)
            highestGAPTR = candidateHighest
          if (receivedGAPTR >= degree) {
            try {
              if (outputStream != null)
                outputStream.write("gaptr " + highestGAPTR.toString)
            } catch { //todo: error handling
              case e: Exception => {}
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
          val candidateHighest = a.drop(">gsdmsr ".length).dropRight(1).toInt
          if (candidateHighest > highestGSDMSR)
            highestGSDMSR = candidateHighest
          if (receivedGSDMSR >= degree) {
            try {
              if (outputStream != null)
                outputStream.write("gsdmsr " + highestGSDMSR.toString)
            } catch { //todo: error handling
              case e: Exception => {}
            }
            receivedGSDMSR = 0
            highestGSDMSR = 0
          }
        }else if(a.startsWith(">OutsideInfluenceAddress ")) {
          var addr = a.split("\\s+")(1).dropRight(1)
          var addrSplit = a.split(":",2)
          connectedSocket = new Socket(addrSplit(0),addrSplit(1).toInt)
          outputStream = new BufferedWriter(new OutputStreamWriter(connectedSocket.getOutputStream()))
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
