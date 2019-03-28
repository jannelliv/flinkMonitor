package ch.ethz.infsec;


import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class CsvReplayer {
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



    static abstract class DatabaseBuffer extends OutputItem {
        DatabaseBuffer(long emissionTime) {
            super(emissionTime);
        }

        abstract void addEvent(String csvLine, String relation, int indexBeforeData);

        abstract int getNumberOfEvents();

        @Override
        void reportDelivery(Reporter reporter, long startTime) {
            reporter.reportDelivery(this, startTime);
        }
    }

    //bit of a hack, sadly, because Reporter requires a DatabaseBuffer object for .reportDelivery
    static class SimpleMonpolyWriter extends DatabaseBuffer {

        String line = null;
        public SimpleMonpolyWriter(long emissionTime) {
            super(emissionTime);
        }
        @Override
        void write(Writer writer) throws IOException {
            writer.append(line).append('\n');
        }

        int events = -1;
        @Override
        int getNumberOfEvents(){
            if(events == -1) {
                int i = 0;
                for (int j = 0; j < line.length(); j++) {
                    if (line.charAt(j) == '(')
                        i++;
                }
                events = i;
            }
            return events;
        }

        @Override
        void addEvent(String csvLine, String relation, int indexBeforeData) {
            if(line == null) {
                line = csvLine;
            }else{
                throw new RuntimeException("multiple events per SimpleMonpolyWriter is not supported");
            }
        }

    }

    static final class MonpolyDatabaseBuffer extends DatabaseBuffer {
        private final long timestamp;
        private final Map<String, StringBuilder> relations = new HashMap<>();
        private int numberOfEvents = 0;

        MonpolyDatabaseBuffer(long timestamp, long emissionTime) {
            super(emissionTime);
            this.timestamp = timestamp;
        }

        @Override
        void addEvent(String csvLine, String relation, int indexBeforeData) {
            ++numberOfEvents;

            StringBuilder relationBuilder = relations.get(relation);
            if (relationBuilder == null) {
                relationBuilder = new StringBuilder();
                relations.put(relation, relationBuilder);
            }

            relationBuilder.append('(');
            int currentIndex = indexBeforeData;
            while (0 <= currentIndex && currentIndex < csvLine.length()) {
                currentIndex = csvLine.indexOf('=', currentIndex);
                if (currentIndex >= 0) {
                    currentIndex += 1;
                    while (currentIndex < csvLine.length() && Character.isSpaceChar(csvLine.charAt(currentIndex)))
                        currentIndex += 1;
                    int startIndex = currentIndex;
                    currentIndex = csvLine.indexOf(',', startIndex);
                    boolean more = currentIndex >= 0;
                    if (!more)
                        currentIndex = csvLine.length();
                    relationBuilder.append(csvLine.substring(startIndex, currentIndex).trim());
                    if (more)
                        relationBuilder.append(',');
                    currentIndex += 1;
                }
            }
            relationBuilder.append(')');
        }

        @Override
        int getNumberOfEvents() {
            return numberOfEvents;
        }

        @Override
        void write(Writer writer) throws IOException {
            writer.append('@').append(Long.toString(timestamp));
            for (HashMap.Entry<String, StringBuilder> entry : relations.entrySet()) {
                writer.append(' ').append(entry.getKey()).append(entry.getValue());
            }
            writer.append('\n');
        }
    }

    static final class CsvDatabaseBuffer extends DatabaseBuffer {
        private final ArrayList<String> lines = new ArrayList<>();

        CsvDatabaseBuffer(long emissionTime) {
            super(emissionTime);
        }

        @Override
        void addEvent(String csvLine, String relation, int indexBeforeData) {
            lines.add(csvLine);
        }

        @Override
        int getNumberOfEvents() {
            return lines.size();
        }

        @Override
        void write(Writer writer) throws IOException {
            for (String line : lines) {
                writer.append(line).append('\n');
            }
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

    interface DatabaseBufferFactory {
        DatabaseBuffer createDatabaseBuffer(long timestamp, long emissionTime);
    }
    static final class SimpleMonpolyDBFactory implements DatabaseBufferFactory {
        @Override
        public DatabaseBuffer createDatabaseBuffer(long timestamp, long emissionTime){
            return new SimpleMonpolyWriter(emissionTime);
        }
    }

    static final class MonpolyDatabaseBufferFactory implements DatabaseBufferFactory {
        @Override
        public DatabaseBuffer createDatabaseBuffer(long timestamp, long emissionTime) {
            return new MonpolyDatabaseBuffer(timestamp, emissionTime);
        }
    }

    static final class CsvDatabaseBufferFactory implements DatabaseBufferFactory {
        @Override
        public DatabaseBuffer createDatabaseBuffer(long timestamp, long emissionTime) {
            return new CsvDatabaseBuffer(emissionTime);
        }
    }

    static class InputWorker implements Runnable {
        private final BufferedReader input;
        private final double timeMultiplier;
        private final long timestampInterval;
        private final DatabaseBufferFactory factory;
        private final LinkedBlockingQueue<OutputItem> queue;

        private boolean successful = false;

        private DatabaseBuffer databaseBuffer = null;
        private long firstTimestamp = -1;
        private long currentTimepoint;

        private long absoluteStartMillis = -1;
        private long nextTimestampToEmit;

        InputWorker(BufferedReader input, double timeMultiplier, long timestampInterval, DatabaseBufferFactory factory, LinkedBlockingQueue<OutputItem> queue) {
            this.input = input;
            this.timeMultiplier = timeMultiplier;
            this.timestampInterval = timestampInterval;
            this.factory = factory;
            this.queue = queue;
        }

        public void run() {
            boolean monpolyFormat = factory instanceof SimpleMonpolyDBFactory;
            try {
                String line;
                while ((line = input.readLine()) != null) {
                    if (line.startsWith(">")) {
                        processCommand(line);
                    } else if (monpolyFormat) {
                        processMonpolyRecord(line);
                    }else{
                        processRecord(line);
                    }
                }
                emitBuffer();
                if (timestampInterval > 0) {
                    queue.put(new TimestampItem(nextTimestampToEmit, absoluteStartMillis));
                }
                queue.put(new TerminalItem(false));
                successful = true;
            } catch (IOException e) {
                e.printStackTrace(System.err);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        boolean isSuccessful() {
            return successful;
        }

        private void processCommand(String line) throws InterruptedException {
            emitBuffer();
            CommandItem commandItem = new CommandItem(line);
            queue.put(commandItem);
            if (commandItem.requiresReconnect()) {
                queue.put(new TerminalItem(true));
            }
        }

        private void processMonpolyRecord(String line) throws InterruptedException {
            long timestamp = Long.valueOf(line.substring(1,line.indexOf(' ')));
            if (firstTimestamp < 0) {
                firstTimestamp = timestamp;
                databaseBuffer = factory.createDatabaseBuffer(timestamp, 0);
            }
            databaseBuffer.addEvent(line, "",0);
            emitBuffer();

            long nextEmission;
            if (timeMultiplier > 0.0) {
                nextEmission = Math.round((double) (timestamp - firstTimestamp) / timeMultiplier * 1000.0);
            } else {
                nextEmission = 0;
            }
            databaseBuffer = factory.createDatabaseBuffer(timestamp, nextEmission);
        }

        private void processRecord(String line) throws InterruptedException {
            String relation;
            long timepoint;
            long timestamp;

            int startIndex;
            int currentIndex = 0;

            while (Character.isSpaceChar(line.charAt(currentIndex))) currentIndex += 1;
            startIndex = currentIndex;
            currentIndex = line.indexOf(',', startIndex);
            relation = line.substring(startIndex, currentIndex).trim();
            currentIndex += 1;

            currentIndex = line.indexOf('=', currentIndex) + 1;
            while (Character.isSpaceChar(line.charAt(currentIndex))) currentIndex += 1;
            startIndex = currentIndex;
            currentIndex = line.indexOf(',', startIndex);
            timepoint = Long.valueOf(line.substring(startIndex, currentIndex).trim());
            currentIndex += 1;

            currentIndex = line.indexOf('=', currentIndex) + 1;
            while (Character.isSpaceChar(line.charAt(currentIndex))) currentIndex += 1;
            startIndex = currentIndex;
            currentIndex = line.indexOf(',', startIndex);
            if (currentIndex < 0)
                currentIndex = line.length();
            timestamp = Long.valueOf(line.substring(startIndex, currentIndex).trim());
            currentIndex += 1;

            if (firstTimestamp < 0) {
                firstTimestamp = timestamp;
                currentTimepoint = timepoint;
                databaseBuffer = factory.createDatabaseBuffer(timestamp, 0);
            }
            if (timepoint != currentTimepoint) {
                emitBuffer();

                long nextEmission;
                if (timeMultiplier > 0.0) {
                    nextEmission = Math.round((double) (timestamp - firstTimestamp) / timeMultiplier * 1000.0);
                } else {
                    nextEmission = 0;
                }
                databaseBuffer = factory.createDatabaseBuffer(timestamp, nextEmission);
                currentTimepoint = timepoint;
            }

            databaseBuffer.addEvent(line, relation, currentIndex);
        }

        private void emitBuffer() throws InterruptedException {
            if (databaseBuffer == null) {
                return;
            }
            long emissionTime = databaseBuffer.emissionTime;

            if (timestampInterval > 0) {
                if (absoluteStartMillis < 0) {
                    absoluteStartMillis = System.currentTimeMillis();
                    nextTimestampToEmit = 0;
                }

                while (nextTimestampToEmit < emissionTime) {
                    queue.put(new TimestampItem(nextTimestampToEmit, absoluteStartMillis));
                    nextTimestampToEmit += timestampInterval;
                }
            }

            queue.put(databaseBuffer);
            databaseBuffer = null;
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

            int events = databaseBuffer.getNumberOfEvents();
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
            } catch (IOException ignored) {}
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

    static class OutputWorker implements Runnable {
        private final Output output;
        private final LinkedBlockingQueue<OutputItem> queue;
        private final Reporter reporter;
        private final Thread inputThread;

        private boolean successful = false;

        OutputWorker(Output output, LinkedBlockingQueue<OutputItem> queue, Reporter reporter, Thread inputThread) {
            this.output = output;
            this.queue = queue;
            this.reporter = reporter;
            this.inputThread = inputThread;
        }

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
            } catch (IOException e) {
                e.printStackTrace(System.err);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
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
                "Usage: [-v|-vv] [-a <acceleration>] [-q <buffer size>] [-m|-ms] [-t <interval>] [-o <host>:<port>] [<file>]\n");
        System.exit(1);
    }

    public static void main(String[] args) {
        String inputFilename = null;
        double timeMultiplier = 1.0;
        DatabaseBufferFactory databaseBufferFactory = new CsvDatabaseBufferFactory();
        long timestampInterval = -1;
        String outputHost = null;
        int outputPort = 0;
        Reporter reporter = new NullReporter();
        int queueCapacity = 1024;
        boolean reconnect = false;

        try {
            for (int i = 0; i < args.length; ++i) {
                switch (args[i]) {
                    case "-v":
                        reporter = new IntervalReporter(false);
                        break;
                    case "-vv":
                        reporter = new IntervalReporter(true);
                        break;
                    case "-a":
                        if (++i == args.length) {
                            invalidArgument();
                        }
                        timeMultiplier = Double.parseDouble(args[i]);
                        break;
                    case "-q":
                        if (++i == args.length) {
                            invalidArgument();
                        }
                        queueCapacity = Integer.parseInt(args[i]);
                        break;
                    case "-m":
                        databaseBufferFactory = new MonpolyDatabaseBufferFactory();
                        break;
                    case "-ms":
                        databaseBufferFactory = new SimpleMonpolyDBFactory();
                        break;
                    case "-t":
                        if (++i == args.length) {
                            invalidArgument();
                        }
                        timestampInterval = Long.parseLong(args[i]);
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

        BufferedReader inputReader;
        if (inputFilename == null) {
            inputReader = new BufferedReader(new InputStreamReader(System.in));
        } else {
            try {
                inputReader = new BufferedReader(new FileReader(inputFilename));
            } catch (FileNotFoundException e) {
                System.err.print("Error: " + e.getMessage() + "\n");
                System.exit(1);
                return;
            }
        }

        Output output;
        if (outputHost == null) {
            output = new StandardOutput();
        } else {
            try {
                int backlog = reconnect ? -1 : 1;
                ServerSocket serverSocket = new ServerSocket(outputPort, backlog, InetAddress.getByName(outputHost));
                SocketOutput socketOutput = new SocketOutput(serverSocket, reconnect);
                socketOutput.acquireClient();
                output = socketOutput;
            } catch (IOException e) {
                System.err.print("Error: " + e.getMessage() + "\n");
                System.exit(1);
                return;
            }
        }

        Thread reporterThread = new Thread(reporter);
        reporterThread.setDaemon(true);
        reporterThread.start();

        LinkedBlockingQueue<OutputItem> queue = new LinkedBlockingQueue<>(queueCapacity);
        InputWorker inputWorker = new InputWorker(inputReader, timeMultiplier, timestampInterval, databaseBufferFactory, queue);
        Thread inputThread = new Thread(inputWorker);
        inputThread.start();
        OutputWorker outputWorker = new OutputWorker(output, queue, reporter, inputThread);
        Thread outputThread = new Thread(outputWorker);
        outputThread.start();

        try {
            inputThread.join();
            if (!inputWorker.isSuccessful()) {
                outputThread.interrupt();
            }
            outputThread.join();
            reporterThread.join(2000);
        } catch (InterruptedException ignored) {
        }

        if (!(inputWorker.isSuccessful() && outputWorker.isSuccessful())) {
            System.exit(1);
        }
    }
}
