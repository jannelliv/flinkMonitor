package ch.eth.inf.infsec;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.StoppableFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.IOUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A source function that reads strings from a socket. The source will read bytes from the socket
 * stream and convert them to characters, each byte individually. When the delimiter character is
 * received, the function will output the current string, and begin a new string.
 *
 * <p>The function strips trailing <i>carriage return</i> characters (\r) when the delimiter is the
 * newline character (\n).
 *
 * <p>The function can be set to reconnect to the server socket in case that the stream is closed on
 * the server side.
 */
@PublicEvolving
public class CustomSourceFunction implements SourceFunction<String>, CheckpointedFunction, StoppableFunction {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction.class);

    /** Default delay between successive connection attempts. */
    private static final int DEFAULT_CONNECTION_RETRY_SLEEP = 500;

    /** Default connection timeout when connecting to the server socket (infinite). */
    private static final int CONNECTION_TIMEOUT_TIME = 0;


    private final String hostname;
    private final int port;
    private final String delimiter;
    private final long maxNumRetries;
    private final long delayBetweenRetries;

    private transient Socket currentSocket;

    private volatile boolean isRunning = true;

    private boolean pendingSlicer = false;

    public CustomSourceFunction(String hostname, int port, String delimiter, long maxNumRetries) {
        this(hostname, port, delimiter, maxNumRetries, DEFAULT_CONNECTION_RETRY_SLEEP);
    }

    public CustomSourceFunction(String hostname, int port, String delimiter, long maxNumRetries, long delayBetweenRetries) {
        checkArgument(port > 0 && port < 65536, "port is out of range");
        checkArgument(maxNumRetries >= -1, "maxNumRetries must be zero or larger (num retries), or -1 (infinite retries)");
        checkArgument(delayBetweenRetries >= 0, "delayBetweenRetries must be zero or positive");

        this.hostname = checkNotNull(hostname, "hostname must not be null");
        this.port = port;
        this.delimiter = delimiter;
        this.maxNumRetries = maxNumRetries;
        this.delayBetweenRetries = delayBetweenRetries;
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        final StringBuilder buffer = new StringBuilder();
        long attempt = 0;

        while (isRunning) {

            try (Socket socket = new Socket()) {
                currentSocket = socket;

                LOG.info("Connecting to server socket " + hostname + ':' + port);
                socket.connect(new InetSocketAddress(hostname, port), CONNECTION_TIMEOUT_TIME);
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

                    char[] cbuf = new char[8192];
                    int bytesRead;
                    while (isRunning && (bytesRead = reader.read(cbuf)) != -1) {
                        buffer.append(cbuf, 0, bytesRead);
                        int delimPos;
                        while (buffer.length() >= delimiter.length() && (delimPos = buffer.indexOf(delimiter)) != -1) {
                            String record = buffer.substring(0, delimPos);
                            // truncate trailing carriage return
                            if (delimiter.equals("\n") && record.endsWith("\r")) {
                                record = record.substring(0, record.length() - 1);
                            }
                            ctx.collect(record);
                            buffer.delete(0, delimPos + delimiter.length());
                        }
                    }
                }
            }

            // if we dropped out of this loop due to an EOF, sleep and retry
            if (isRunning) {
                attempt++;
                if (maxNumRetries == -1 || attempt < maxNumRetries) {
                    LOG.warn("Lost connection to server socket. Retrying in " + delayBetweenRetries + " msecs...");
                    Thread.sleep(delayBetweenRetries);
                }
                else {
                    // this should probably be here, but some examples expect simple exists of the stream source
                    // throw new EOFException("Reached end of stream and reconnects are not enabled.");
                    break;
                }
            }
        }

        // collect trailing data
        if (buffer.length() > 0) {
            ctx.collect(buffer.toString());
        }
    }

    @Override
    public void cancel() {
        isRunning = false;

        // we need to close the socket as well, because the Thread.interrupt() function will
        // not wake the thread in the socketStream.read() method when blocked.
        Socket theSocket = this.currentSocket;
        if (theSocket != null) {
            IOUtils.closeSocket(theSocket);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        System.out.println("Custom source function: snapshot");
        LOG.info("Custom source function: snapshot");
        if(pendingSlicer){
            System.out.println("Custom source function: Stopped");
            LOG.info("Custom source function: Stopped");
            this.stop();
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }

    public void setPendingSlicer(){
        System.out.println("Custom source function: Pending slicer set");
        this.pendingSlicer = true;
    }

    @Override
    public void stop() {
        this.cancel();
    }
}
