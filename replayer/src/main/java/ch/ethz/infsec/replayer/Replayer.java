package ch.ethz.infsec.replayer;

import ch.ethz.infsec.monitor.Fact;
import ch.ethz.infsec.trace.Trace;
import ch.ethz.infsec.trace.formatter.Crv2014CsvFormatter;
import ch.ethz.infsec.trace.formatter.MonpolyTraceFormatter;
import ch.ethz.infsec.trace.formatter.TraceFormatter;
import ch.ethz.infsec.trace.parser.Crv2014CsvParser;
import ch.ethz.infsec.trace.parser.TraceParser;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;

public class Replayer {
    double timeMultiplier;
    long timestampInterval;

    BufferedReader input;
    TraceParser parser;
    TraceFormatter formatter;

    Output output;
    Reporter reporter;

    LinkedBlockingQueue<OutputItem> queue;
    Thread inputThread;
    Thread outputThread;
    Thread reporterThread;

    static abstract class OutputItem {
        final long emissionTime;

        OutputItem(long emissionTime) {
            this.emissionTime = emissionTime;
        }

        abstract void write(Writer writer) throws IOException;

        abstract void reportDelivery(Reporter reporter, long startTime);
    }

    static final class TerminalItem extends OutputItem {
        final boolean resurrect;

        TerminalItem(boolean resurrect) {
            super(-1);
            this.resurrect = resurrect;
        }

        @Override
        void write(Writer writer) {
        }

        @Override
        void reportDelivery(Reporter reporter, long startTime) {
            if (!resurrect) {
                reporter.reportEnd();
            }
        }
    }

    static final class DatabaseBuffer extends OutputItem {
        private final String database;
        final int count;

        DatabaseBuffer(long emissionTime, String database, int count) {
            super(emissionTime);
            this.database = database;
            this.count = count;
        }

        @Override
        void write(Writer writer) throws IOException {
            writer.write(database);
        }

        @Override
        void reportDelivery(Reporter reporter, long startTime) {
            reporter.reportDelivery(this, startTime);
        }
    }

    static final class TimestampItem extends OutputItem {
        static final String TIMESTAMP_PREFIX = "###";

        private final String line;

        TimestampItem(long emissionTime, long startTimestamp) {
            super(emissionTime);

            long timestamp = startTimestamp + emissionTime;
            this.line = TIMESTAMP_PREFIX + Long.toString(timestamp) + "\n";
        }

        @Override
        public void write(Writer writer) throws IOException {
            writer.write(line);
        }

        @Override
        void reportDelivery(Reporter reporter, long startTime) {
        }
    }

    static final class CommandItem extends OutputItem {
        private final String command;

        CommandItem(String command) {
            super(-1);
            this.command = command;
        }

        boolean requiresReconnect() {
            return command.startsWith(">set_slicer ");
        }

        @Override
        public void write(Writer writer) throws IOException {
            writer.write(command + "\n");
        }

        @Override
        void reportDelivery(Reporter reporter, long startTime) {
        }
    }

    class InputWorker implements Runnable {
        private boolean successful = false;

        private long absoluteStartMillis = -1;
        private long firstTimestamp = -1;
        private int count = 0;
        private long nextTimestampToEmit;

        private StringBuilder stringBuilder = new StringBuilder();
        private ArrayList<DatabaseBuffer> readyBuffers = new ArrayList<>();

        public void run() {
            try {
                String line;
                while ((line = input.readLine()) != null) {
                    if (line.startsWith(">")) {
                        processCommand(line);
                    } else {
                        parser.parseLine(this::processFact, line);
                        emitBuffers();
                    }
                }
                parser.endOfInput(this::processFact);
                emitBuffers();
                if (timestampInterval > 0) {
                    queue.put(new TimestampItem(nextTimestampToEmit, absoluteStartMillis));
                }
                queue.put(new TerminalItem(false));
                successful = true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                e.printStackTrace(System.err);
            }
        }

        boolean isSuccessful() {
            return successful;
        }

        private void processCommand(String line) throws InterruptedException {
            CommandItem commandItem = new CommandItem(line);
            queue.put(commandItem);
            if (commandItem.requiresReconnect()) {
                queue.put(new TerminalItem(true));
            }
        }

        private void processFact(Fact fact) {
            formatter.printFact(stringBuilder, fact);
            ++count;

            if (Trace.isEventFact(fact)) {
                final long timestamp = Long.valueOf(fact.getTimestamp());
                if (firstTimestamp < 0) {
                    firstTimestamp = timestamp;
                }

                final long nextEmission;
                if (timeMultiplier > 0.0) {
                    nextEmission = Math.round((double) (timestamp - firstTimestamp) / timeMultiplier * 1000.0);
                } else {
                    nextEmission = 0;
                }

                readyBuffers.add(new DatabaseBuffer(nextEmission, stringBuilder.toString(), count));
                stringBuilder.setLength(0);
                count = 0;
            }
        }

        private void emitBuffers() throws InterruptedException {
            for (DatabaseBuffer buffer : readyBuffers) {
                if (timestampInterval > 0) {
                    if (absoluteStartMillis < 0) {
                        absoluteStartMillis = System.currentTimeMillis();
                        nextTimestampToEmit = 0;
                    }

                    while (nextTimestampToEmit < buffer.emissionTime) {
                        queue.put(new TimestampItem(nextTimestampToEmit, absoluteStartMillis));
                        nextTimestampToEmit += timestampInterval;
                    }
                }
                queue.put(buffer);
            }
            readyBuffers.clear();
        }
    }

    interface Reporter extends Runnable {
        void reportUnderrun();

        void reportDelivery(DatabaseBuffer databaseBuffer, long startTime);

        void reportEnd();
    }

    static class NullReporter implements Reporter {
        @Override
        public void reportUnderrun() {
        }

        @Override
        public void reportDelivery(DatabaseBuffer databaseBuffer, long startTime) {
        }

        @Override
        public void reportEnd() {
        }

        @Override
        public void run() {
        }
    }

    static class IntervalReporter implements Reporter {
        static final long INTERVAL_MILLIS = 1000L;

        private final boolean verbose;

        private volatile boolean running = true;
        private long startTime;
        private long lastReport;

        private int underruns = 0;
        private int indices = 0;
        private int indicesSinceLastReport = 0;
        private int totalEvents = 0;
        private int eventsSinceLastReport = 0;
        private long currentDelay = 0;
        private long delaySum = 0;
        private long maxDelay = 0;
        private long maxDelaySinceLastReport = 0;

        IntervalReporter(boolean verbose) {
            this.verbose = verbose;
        }

        @Override
        public synchronized void reportUnderrun() {
            ++underruns;
        }

        @Override
        public synchronized void reportDelivery(DatabaseBuffer databaseBuffer, long startTime) {
            long now = System.nanoTime();

            ++indices;
            ++indicesSinceLastReport;

            int events = databaseBuffer.count;
            totalEvents += events;
            eventsSinceLastReport += events;

            long elapsedMillis = (now - startTime) / 1_000_000L;
            currentDelay = Math.max(0, elapsedMillis - databaseBuffer.emissionTime);
            delaySum += currentDelay;
            maxDelay = Math.max(maxDelay, currentDelay);
            maxDelaySinceLastReport = Math.max(maxDelaySinceLastReport, currentDelay);
        }

        @Override
        public synchronized void reportEnd() {
            running = false;
        }

        private synchronized void doReport() {
            long now = System.nanoTime();

            double totalSeconds = (double) (now - startTime) / 1e9;
            double deltaSeconds = (double) (now - lastReport) / 1e9;

            double indexRate = (double) indicesSinceLastReport / deltaSeconds;
            double eventRate = (double) eventsSinceLastReport / deltaSeconds;
            double delaySeconds = (double) currentDelay / 1000.0;
            double totalAverageDelaySeconds = (double) delaySum / ((double) indices * 1000.0);
            double maxDelaySeconds = (double) maxDelay / 1000.0;
            double currentMaxDelaySeconds = (double) maxDelaySinceLastReport / 1000.0;

            if (verbose) {
                System.err.printf(
                        "%5.1fs: %8.1f indices/s, %8.1f events/s, %6.3fs delay, %6.3fs peak delay, %6.3fs max. delay, %6.3fs avg. delay, %9d indices, %9d events, %6d underruns\n",
                        totalSeconds, indexRate, eventRate, delaySeconds, currentMaxDelaySeconds, maxDelaySeconds, totalAverageDelaySeconds, indices, totalEvents, underruns);
            } else {
                System.err.printf("%5.1f   %8.1f %8.1f   %6.3f %6.3f %6.3f %6.3f\n",
                        totalSeconds, indexRate, eventRate, delaySeconds, currentMaxDelaySeconds, maxDelaySeconds, totalAverageDelaySeconds);
            }

            indicesSinceLastReport = 0;
            eventsSinceLastReport = 0;
            currentDelay = 0;
            maxDelaySinceLastReport = 0;

            lastReport = now;
        }

        @Override
        public void run() {
            try {
                long schedule;
                synchronized (this) {
                    startTime = System.nanoTime();
                    lastReport = startTime;
                    schedule = startTime;
                }

                while (running) {
                    schedule += INTERVAL_MILLIS * 1_000_000L;
                    long now = System.nanoTime();
                    long waitMillis = Math.max(0L, (schedule - now) / 1_000_000L);
                    Thread.sleep(waitMillis);
                    doReport();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    interface Output {
        void writeItemAndFlush(OutputItem item) throws IOException;

        void waitForReconnect() throws IOException;
    }

    static class StandardOutput implements Output {
        private final BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(System.out));

        @Override
        public void writeItemAndFlush(OutputItem item) throws IOException {
            item.write(writer);
            writer.flush();
        }

        @Override
        public void waitForReconnect() {
        }
    }

    static class SocketOutput implements Output {
        private final ServerSocket serverSocket;
        private final boolean reconnect;

        private Socket clientSocket = null;
        private BufferedWriter writer = null;

        SocketOutput(ServerSocket serverSocket, boolean reconnect) {
            this.serverSocket = serverSocket;
            this.reconnect = reconnect;
        }

        void acquireClient() throws IOException {
            if (clientSocket != null) {
                throw new IllegalStateException("Client has already been acquired");
            }
            clientSocket = serverSocket.accept();
            writer = new BufferedWriter(new OutputStreamWriter(clientSocket.getOutputStream()));
            System.err.printf("Client connected: %s\n", clientSocket.getInetAddress().getHostAddress());
        }

        private void closeClient() {
            if (clientSocket == null) {
                return;
            }
            try {
                writer.close();
                clientSocket.close();
            } catch (IOException ignored) {
            }
            writer = null;
            clientSocket = null;
        }

        @Override
        public void writeItemAndFlush(OutputItem item) throws IOException {
            boolean tryWriting = true;
            do {
                try {
                    item.write(writer);
                    writer.flush();
                    tryWriting = false;
                } catch (IOException e) {
                    System.err.println("Could not write event");
                    closeClient();
                    if (reconnect) {
                        System.err.println("Waiting for new client ...");
                        acquireClient();
                    } else {
                        throw e;
                    }
                }
            } while (tryWriting);
        }

        @Override
        public void waitForReconnect() throws IOException {
            if (reconnect) {
                closeClient();
                System.err.println("Waiting for new client ...");
                acquireClient();
            }
        }
    }

    class OutputWorker implements Runnable {
        private boolean successful = false;

        private void runInternal() throws InterruptedException, IOException {
            long startTime = 0L;
            boolean isFirst = true;
            OutputItem outputItem;
            do {
                do {
                    if (isFirst) {
                        outputItem = queue.take();
                        startTime = System.nanoTime();
                        isFirst = false;
                    } else {
                        outputItem = queue.poll();
                        if (outputItem == null) {
                            reporter.reportUnderrun();
                            outputItem = queue.take();
                        }
                    }

                    long now = System.nanoTime();
                    long elapsedMillis = (now - startTime) / 1_000_000L;
                    long waitMillis = outputItem.emissionTime - elapsedMillis;
                    if (waitMillis > 1L) {
                        Thread.sleep(waitMillis);
                    }

                    output.writeItemAndFlush(outputItem);
                    outputItem.reportDelivery(reporter, startTime);
                } while (!(outputItem instanceof TerminalItem));

                if (((TerminalItem) outputItem).resurrect) {
                    output.waitForReconnect();
                }
            } while (((TerminalItem) outputItem).resurrect);
            successful = true;
        }

        public void run() {
            try {
                runInternal();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                e.printStackTrace(System.err);
            }
            if (!successful) {
                inputThread.interrupt();
            }
        }

        boolean isSuccessful() {
            return successful;
        }
    }

    private static void invalidArgument() {
        System.err.print("Error: Invalid argument.\n" +
                "Usage: [-v|-vv] [-a <acceleration>] [-q <buffer size>] [-m] [-t <interval>] [-o <host>:<port>] [<file>]\n");
        System.exit(1);
    }

    public static void main(String[] args) {
        Replayer replayer = new Replayer();
        replayer.timeMultiplier = 1.0;
        replayer.timestampInterval = -1;
        replayer.reporter = new NullReporter();
        replayer.parser = new Crv2014CsvParser();
        replayer.formatter = new Crv2014CsvFormatter();

        String inputFilename = null;
        String outputHost = null;
        int outputPort = 0;
        int queueCapacity = 1024;
        boolean reconnect = false;

        try {
            for (int i = 0; i < args.length; ++i) {
                switch (args[i]) {
                    case "-v":
                        replayer.reporter = new IntervalReporter(false);
                        break;
                    case "-vv":
                        replayer.reporter = new IntervalReporter(true);
                        break;
                    case "-a":
                        if (++i == args.length) {
                            invalidArgument();
                        }
                        replayer.timeMultiplier = Double.parseDouble(args[i]);
                        break;
                    case "-q":
                        if (++i == args.length) {
                            invalidArgument();
                        }
                        queueCapacity = Integer.parseInt(args[i]);
                        break;
                    case "-m":
                        replayer.formatter = new MonpolyTraceFormatter();
                        break;
                    case "-t":
                        if (++i == args.length) {
                            invalidArgument();
                        }
                        replayer.timestampInterval = Long.parseLong(args[i]);
                        break;
                    case "-o":
                        if (++i == args.length) {
                            invalidArgument();
                        }
                        String parts[] = args[i].split(":", 2);
                        if (parts.length != 2) {
                            invalidArgument();
                        }
                        outputHost = parts[0];
                        outputPort = Integer.parseInt(parts[1]);
                        break;
                    case "-k":
                        reconnect = true;
                        break;
                    default:
                        if (args[i].startsWith("-") || inputFilename != null) {
                            invalidArgument();
                        }
                        inputFilename = args[i];
                        break;
                }
            }
        } catch (NumberFormatException e) {
            invalidArgument();
        }

        if (inputFilename == null) {
            replayer.input = new BufferedReader(new InputStreamReader(System.in));
        } else {
            try {
                replayer.input = new BufferedReader(new FileReader(inputFilename));
            } catch (FileNotFoundException e) {
                System.err.print("Error: " + e.getMessage() + "\n");
                System.exit(1);
                return;
            }
        }

        if (outputHost == null) {
            replayer.output = new StandardOutput();
        } else {
            try {
                int backlog = reconnect ? -1 : 1;
                ServerSocket serverSocket = new ServerSocket(outputPort, backlog, InetAddress.getByName(outputHost));
                SocketOutput socketOutput = new SocketOutput(serverSocket, reconnect);
                socketOutput.acquireClient();
                replayer.output = socketOutput;
            } catch (IOException e) {
                System.err.print("Error: " + e.getMessage() + "\n");
                System.exit(1);
                return;
            }
        }

        replayer.reporterThread = new Thread(replayer.reporter);
        replayer.reporterThread.setDaemon(true);
        replayer.reporterThread.start();

        replayer.queue = new LinkedBlockingQueue<>(queueCapacity);
        InputWorker inputWorker = replayer.new InputWorker();
        replayer.inputThread = new Thread(inputWorker);
        replayer.inputThread.start();
        OutputWorker outputWorker = replayer.new OutputWorker();
        replayer.outputThread = new Thread(outputWorker);
        replayer.outputThread.start();

        try {
            replayer.inputThread.join();
            if (!inputWorker.isSuccessful()) {
                replayer.outputThread.interrupt();
            }
            replayer.outputThread.join();
            replayer.reporterThread.join(2000);
        } catch (InterruptedException ignored) {
        }

        if (!(inputWorker.isSuccessful() && outputWorker.isSuccessful())) {
            System.exit(1);
        }
    }
}
