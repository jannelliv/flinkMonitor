package ch.eth.inf.infsec.monitor

import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter}
import java.lang.ProcessBuilder.Redirect
import java.net.{InetAddress, ServerSocket, Socket}
import java.util.concurrent.LinkedBlockingQueue

import ch.eth.inf.infsec.StatelessProcessor
import ch.eth.inf.infsec.trace.{CommandRecord, Record}

import scala.collection.JavaConversions




//the fusion of a very simplified ExternalProcessOperator and a Processor
class OutsideInfluence extends StatelessProcessor[Record,Record] with Serializable {

  @transient private var serverSocket: ServerSocket = _
  @transient private var connectedSocket: Socket = _

  @transient private var reader: BufferedReader = _
  @transient private var readerQueue: LinkedBlockingQueue[String] = _


  @transient var portId : Long = 0
  @transient var machineIp : String = null
  @transient var connectThread : SocketHandlingThread = null


  def make() : Unit = {
    serverSocket = new ServerSocket()
    portId = serverSocket.getLocalPort()
    machineIp = InetAddress.getLocalHost().getHostAddress()


    connectThread = new SocketHandlingThread {
      override def startup() : Unit  = {
        connectedSocket = serverSocket.accept()
        serverSocket.close()
        serverSocket = null
        reader = new BufferedReader(new InputStreamReader(connectedSocket.getInputStream()))
        readerQueue = new LinkedBlockingQueue[String]()
      }
      override def handle() : Unit = {
        readerQueue.put(reader.readLine())
      }
    }
  }

  def sendSocketAddressCommand(f: Record => Unit) : Unit = {
    f(CommandRecord("OutsideInfluenceAddress",machineIp+":"+portId.toString))
  }

  override def process(in: Record, f: Record => Unit): Unit = {
    if(machineIp == null) {
      make()
      sendSocketAddressCommand(f)
    }
    if(readerQueue != null) {
      while (!readerQueue.isEmpty) {
        val str = readerQueue.take()
        val split = str.trim().split("\\s+", 2)
        if (split.length == 2)
          f(CommandRecord(split(0), split(1)))
        else {
          //todo: error handling
        }
      }
    }
    f(in)
  }

  override def terminate(f: Record => Unit): Unit = {
    if(connectThread != null)
      connectThread.interruptAndStop()
    if(serverSocket != null)
      serverSocket.close()
    if(connectedSocket != null)
      connectedSocket.close()

  }

  abstract class SocketHandlingThread extends Thread {
    @volatile var running = true

    def startup(): Unit
    def handle(): Unit

    override def run(): Unit = {
      try {
        startup()
        while (running)
          handle()
      } catch {
        case e: InterruptedException =>
/*          if (running)
            failOperator(e)*/
        case e: Throwable =>
//          failOperator(e)
      }
    }

    def interruptAndStop(): Unit = {
      running = false
      interrupt()
    }
  }
}
