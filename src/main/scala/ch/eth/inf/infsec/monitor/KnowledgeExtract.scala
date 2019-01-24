package ch.eth.inf.infsec.monitor

import java.io.{BufferedWriter, OutputStream, OutputStreamWriter}
import java.net.Socket

import ch.eth.inf.infsec.StatelessProcessor

class KnowledgeExtract extends StatelessProcessor[MonpolyRequest,String] with Serializable {
  @transient private var connectedSocket: Socket = _
  @transient private var outputStream : BufferedWriter = _


  override def process(in: MonpolyRequest, f: String => Unit): Unit = {
    in match {
      case CommandItem(a) => {
        //todo: better parsing
        if(a.startsWith(">gaptr ")) {
            //todo: wait for all, then get highest?
            //or just send along immediately?
            //waiting for all and merging seems best
            //but that requires more info, so we just send immediately
          try {
            if (outputStream != null)
              outputStream.write(a.drop(1).dropRight(1))
          } catch { //todo: error handling
            case e: Exception => {}
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
