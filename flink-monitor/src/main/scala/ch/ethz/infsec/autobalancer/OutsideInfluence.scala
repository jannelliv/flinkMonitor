package ch.ethz.infsec.autobalancer

import java.io._
import java.net.{InetAddress, ServerSocket, Socket}
import java.util.concurrent.LinkedBlockingQueue

import ch.ethz.infsec.monitor.Fact
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.util.Collector

class OutsideInfluence(doLog : Boolean) extends RichFlatMapFunction[Fact,Fact] with Serializable {

  @transient private var serverSocket: ServerSocket = _
  @transient private var connectedSocket: Socket = _

  @transient private var reader: BufferedReader = _
  @transient private var readerQueue: LinkedBlockingQueue[String] = _


  @transient var portId : Long = 0
  @transient var machineIp : String = null
  @transient var connectThread : SocketHandlingThread = null

  def make() : Unit = {
    serverSocket = new ServerSocket(0)
    machineIp = InetAddress.getLocalHost().getHostAddress()
    portId = serverSocket.getLocalPort()


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
      override def failOperator(e : Throwable) : Unit = {
        tempF.write("ran into issue in connectThread: "+e+"\n")
        tempF.flush()
      }
    }
    connectThread.start()
  }

  def sendSocketAddressCommand(f: Collector[Fact]) : Unit = {
    val param = machineIp+":"+portId.toString
    f.collect(Fact.meta("OutsideInfluenceAddress",param))
  }

    var started = false
    var tempF : FileWriter = null

  override def flatMap(in: Fact, f: Collector[Fact]): Unit = {
    if(!started) {
      started = true
      tempF = new FileWriter("OutsideInfluence.log",false)
    }
/*    tempF.write(in.in + "\n")
    tempF.flush()*/

    //TODO: Move to open(Config param):Unit, if possible
    if(machineIp == null) {
      make()
      sendSocketAddressCommand(f)
    }
    if(readerQueue != null) {
      while (!readerQueue.isEmpty) {
        val str = readerQueue.take()
        tempF.write("got message from queue: "+str+"\n")
        tempF.flush()
        val split = str.trim().split("\\s+", 2)
        if (split.length == 2)
          f.collect(Fact.meta(split(0), split(1)))
        else {
          //todo: error handling
        }
      }
    }
    f.collect(in)
  }

  override def close(): Unit = {
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
    def failOperator(exception: Throwable): Unit

    override def run(): Unit = {
      try {
        startup()
        while (running)
          handle()
      } catch {
        case e: InterruptedException =>
          if (running)
            failOperator(e)
        case e: Throwable =>
          failOperator(e)
      }
    }

    def interruptAndStop(): Unit = {
      running = false
      interrupt()
    }
  }
}
