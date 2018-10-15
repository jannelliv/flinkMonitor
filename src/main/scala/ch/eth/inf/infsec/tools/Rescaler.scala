package ch.eth.inf.infsec.tools

import java.io.{BufferedReader, InputStreamReader, PrintWriter}
import java.net.{InetSocketAddress, ServerSocket, Socket}

import javax.xml.bind.DatatypeConverter
import org.apache.flink.api.common.JobID
import org.apache.flink.client.cli.CliArgsException
import org.apache.flink.client.program.rest.RestClusterClient
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.client.JobStatusMessage
import org.apache.flink.runtime.jobgraph.JobStatus
import org.slf4j.LoggerFactory

import scala.collection.immutable

object Rescaler extends Serializable {
  class Rescaler extends Serializable {
    private val logger = LoggerFactory.getLogger(this.getClass)

    private var server: ServerSocket = _
    private var client: RestClusterClient[String] = _
    private val config: Configuration = new Configuration()

    private var clientSocket: Socket = _

    private var out: PrintWriter = _
    private var in: BufferedReader = _

    def init(jobName: String, jmAddress: String, jmPort: Int = 6123): Unit = {
      try {
        server = new ServerSocket(10103)

        config.setString("jobmanager.rpc.address", jmAddress)
        client = new RestClusterClient[String](config, "RemoteExecutor")

        run(jobName)
      } catch {
        case e: Exception => println(e)
      }
    }

    private def run(jobName: String): Unit = {
      var line: String = null
      new Thread(new Runnable() {
        def run(): Unit = {
        clientSocket = server.accept()
        in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream))

        while (true) {
          try {
            line = in.readLine()
            clientSocket.close()
          } catch {
            case e: Exception => run()
          }
          if(line == null) run()

          if(line.matches("^(\\w)+:(\\d)+$")){
            val tuple = line.split(":")
            tuple(0) match {
              case "parallelism" => processRescale(jobName, Integer.parseInt(tuple(1)))
              case _ => throw new Exception("Unrecognized command")
            }
          }
        }
        }}).start()
    }

    /** Parts of this code are dependent on the Flink implementation of the Rest client and its dependencies**/
    def processRescale(jobName: String, p: Int): Unit = {
      val jobId = getJobId(jobName)

      println("Attempting to rescale job with id: " + jobId.toString)
      logger.info("Attempting to rescale job with id: " + jobId.toString)
      val rescaleFuture = client.rescaleJob(jobId, p)
      try {
        rescaleFuture.get
      } catch {
        case _: Exception => throw new Exception("Could not rescale job " + jobId + '.')
      }
      logger.info("Rescaled job " + jobId + ". Its new parallelism is " + p + '.')
      println("Rescaled job " + jobId + ". Its new parallelism is " + p + '.')
    }

    def getJobId(jobName: String): JobID = {
      val jobDetailsFuture = client.listJobs()

      val jobDetails = jobDetailsFuture.get
      var runningJobs = new immutable.ListSet[JobStatusMessage]

      jobDetails.toArray.foreach(e => if(e.asInstanceOf[JobStatusMessage].getJobState == JobStatus.RUNNING) runningJobs += e.asInstanceOf[JobStatusMessage])
      runningJobs.filter(e => e.getJobName eq jobName)

      if(runningJobs.size != 1) throw new Exception("Flink job with name \"%s\" could not be found".format(jobName))
      runningJobs.head.getJobId
    }


    /* Legacy: Replaced by getJobId
     * Extracted from Flink CliFrontend.java & JobID.java respectively
     * */
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



  def create(jobName: String, jmAddress: String, jmPort: Int = 6123): Unit = {
    val rescaler = new Rescaler()
    rescaler.init(jobName, jmAddress, jmPort)
  }

  class RescaleInitiator extends Serializable {
    private val logger = LoggerFactory.getLogger(this.getClass)

    def rescale(p: Int): Unit = {
      try {
        logger.info("Opening socket to 10103")
        val client = new Socket()
        client.connect(new InetSocketAddress("127.0.0.1", 10103))
        val output = client.getOutputStream

        val command = "parallelism:%d\n".format(p).toCharArray.map(_.toByte)

        output.write(command)
        output.flush()

        logger.info("Closing socket")
        output.close()
        client.close()
      } catch {
        case e: Exception => println(e)
      }
    }
  }
}