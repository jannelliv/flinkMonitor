package ch.eth.inf.infsec

import java.io.{BufferedReader, InputStreamReader, PrintWriter}
import java.net.{InetSocketAddress, ServerSocket, Socket}

import javax.xml.bind.DatatypeConverter
import org.apache.flink.api.common.JobID
import org.apache.flink.client.cli.CliArgsException
import org.apache.flink.client.program.rest.RestClusterClient
import org.apache.flink.configuration.Configuration


object Rescaler extends Serializable {
  class Rescaler() extends Serializable {
    private var jobId: JobID = _
    private var server: ServerSocket = _
    private var client: RestClusterClient[String] = _
    private val config: Configuration = new Configuration()

    private var out: PrintWriter = _
    private var in: BufferedReader = _

    def init(jmAddress: String, jmPort: Int = 6123): Unit = {
      try {
        server = new ServerSocket(1112)

        config.setString("jobmanager.rpc.address", jmAddress)
        client = new RestClusterClient[String](config, "RemoteExecutor")

        run()
      } catch {
        case e: Exception => println(e)
      }
    }

    private def run(): Unit = {
      new Thread(new Runnable() {
        def run(): Unit = {
        while (true) {
          val clientSocket: Socket = server.accept()

          out = new PrintWriter(clientSocket.getOutputStream, true)
          in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream))
          var line: String = null
          line = in.readLine()
          println("Received jobId:" + line)
          var jobId: JobID = null
          try {
            jobId = parseJobId(line)
          } catch {
            case _: Exception => out.println("Error parsing jobId from %s \n".format(line))
          }
          out.println("Received & Parsed jobId\n")
          out.flush()
          processRescale(jobId, 4)
        }}}).start()
    }

    def processRescale(jobId: JobID, p: Int): Unit = {
      println("Sending RPC")
      client.rescaleJob(jobId, 4)
    }


    /* Extracted from Flink CliFrontend.java & JobID.java respectively  */
    def parseJobId(jobIdString: String): JobID = {
      var jobId: JobID = null
      try {
        jobId = fromHexString(jobIdString)
      } catch {
        case e: Exception => throw new CliArgsException(e.getMessage);
      }
      jobId
    }

    def fromHexString(hexString: String): JobID = {
      try {
         new JobID(DatatypeConverter.parseHexBinary(hexString))
      } catch {
        case e: Exception => throw new IllegalArgumentException("Cannot parse JobID from \"" + hexString + "\".", e)
      }
    }
  }

  def create(jmAddress: String, jmPort: Int = 6123): Unit = {
    val rescaler = new Rescaler()
    rescaler.init(jmAddress, jmPort)
  }

  def rescale(p: Int): Unit = {
    var responseLine: String = null
    try {
      val client = new Socket()
      client.bind(new InetSocketAddress("127.0.0.1", 1111))
      client.connect(new InetSocketAddress("127.0.0.1", 1112))
      val output = client.getOutputStream
      val input = new BufferedReader(new InputStreamReader(client.getInputStream))

      val command = "modify -p %d\n".format(p).toCharArray.map(_.toByte)

      output.write(command)
      output.flush()
      responseLine = input.readLine()
      println("Response: " + responseLine)

      input.close()
      output.close()
      client.close()
    } catch {
      case e: Exception => println(e)
    }
  }
}