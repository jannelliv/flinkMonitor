package ch.eth.inf.infsec.monitor

import java.io.{BufferedWriter, OutputStream, OutputStreamWriter}
import java.net.Socket

import ch.eth.inf.infsec.StatelessProcessor

class KnowledgeExtract(degree : Int) extends StatelessProcessor[MonpolyRequest,String] with Serializable {
  @transient private var connectedSocket: Socket = _
  @transient private var outputStream : BufferedWriter = _

  var received = 0
  var highest = 0.0

  override def process(in: MonpolyRequest, f: String => Unit): Unit = {
    in match {
      case CommandItem(a) => {
        //todo: better parsing
        if(a.startsWith(">gaptr ")) {
          //wait for all, combine and only send highest
          received = received +1
          val candidateHighest = a.drop(">gaptr ".length).dropRight(1).toDouble
          if(candidateHighest > highest)
            highest = candidateHighest
          if(received >= degree) {
            try {
              if (outputStream != null)
                outputStream.write("gaptr "+highest.toString)
            } catch { //todo: error handling
              case e: Exception => {}
            }
            received = 0
            highest = 0.0
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
