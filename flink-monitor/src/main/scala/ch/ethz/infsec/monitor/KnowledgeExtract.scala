package ch.ethz.infsec.monitor

import java.io.{BufferedWriter, FileWriter, OutputStreamWriter}
import java.net.{InetSocketAddress, Socket}

import ch.ethz.infsec.StatelessProcessor
import org.slf4j.LoggerFactory



class KnowledgeExtract(degree : Int) extends StatelessProcessor[MonitorResponse,String] with Serializable {
  @transient private var connectedSocket: Socket = _
  @transient private var outputStream : BufferedWriter = _
  private val logger = LoggerFactory.getLogger(this.getClass)

  var receivedGAPTR = 0
  var highestGAPTR = 0.0
  var receivedGSDMSR = 0
  var highestGSDMSR = 0
  var receivedShutdown = 0

  var started = false
  var tempF : FileWriter = null

  def stopJob() : Unit = {
    try {
      logger.info("Opening socket to 10103")
      val client = new Socket()
      client.connect(new InetSocketAddress("127.0.0.1", 10103))
      val output = client.getOutputStream

      val command = "cancel:0\n".toCharArray.map(_.toByte)

      output.write(command)
      output.flush()

      logger.info("Closing socket")
      output.close()
      client.close()
    } catch {
      case e: Exception => println(e)
    }
  }

  override def process(in: MonitorResponse, f: String => Unit): Unit = {
    if(!started) {
      started = true
      tempF = new FileWriter("knowledgeExtractEvents.log",true)
    }
    tempF.write(in.in + "\n")
    tempF.flush()

    in match {
      case BypassCommandItem(a) => {
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
        }else if(a.toLowerCase.startsWith(">endofstream")) {
          receivedShutdown += 1
          if(receivedShutdown == degree) {
            stopJob()
          }
        } else {
          //todo: error
        }
    }
      case VerdictItem(b) => f(b)
    }
  }

  override def terminate(f: String => Unit): Unit = {
    if(connectedSocket != null)
      connectedSocket.close()
  }
}
