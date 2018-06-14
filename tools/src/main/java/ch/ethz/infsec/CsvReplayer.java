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
    static abstract class DatabaseBuffer {
        final long emissionTime;
        boolean isLast = false;

        DatabaseBuffer(long emissionTime) {
            this.emissionTime = emissionTime;
        }

        abstract void addEvent(String csvLine, String relation, int indexBeforeData);

        abstract int getNumberOfEvents();

        abstract void write(Writer writer) throws IOException;
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

    interface DatabaseBufferFactory {
        DatabaseBuffer createDatabaseBuffer(long timestamp, long emissionTime);
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
        private final DatabaseBufferFactory factory;
        private final LinkedBlockingQueue<DatabaseBuffer> queue;

        private boolean successful = false;

        private DatabaseBuffer databaseBuffer = null;
        private long firstTimestamp;
        private long currentTimepoint;

        InputWorker(BufferedReader input, double timeMultiplier, DatabaseBufferFactory factory, LinkedBlockingQueue<DatabaseBuffer> queue) {
            this.input = input;
            this.timeMultiplier = timeMultiplier;
            this.factory = factory;
            this.queue = queue;
        }

        public void run() {
            try {
                String line;
                while ((line = input.readLine()) != null) {
                    processRecord(line);
                }

                if (databaseBuffer == null) {
                    databaseBuffer = factory.createDatabaseBuffer(0, -1);
                }
                databaseBuffer.isLast = true;
                queue.put(databaseBuffer);

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

            if (databaseBuffer == null) {
                databaseBuffer = factory.createDatabaseBuffer(timestamp, 0);
                firstTimestamp = timestamp;
                currentTimepoint = timepoint;
            } else if (timepoint != currentTimepoint) {
                queue.put(databaseBuffer);

                long nextEmission = Math.round((double) (timestamp - firstTimestamp) / timeMultiplier * 1000.0);
                databaseBuffer = factory.createDatabaseBuffer(timestamp, nextEmission);
                currentTimepoint = timepoint;
            }

            databaseBuffer.addEvent(line, relation, currentIndex);
        }
    }

    static class OutputWorker implements Runnable {
        private final BufferedWriter output;
        private final boolean doReports;
        private final LinkedBlockingQueue<DatabaseBuffer> queue;
        private final Thread inputThread;

        private int underruns = 0;
        private int indices = 0;
        private int totalEvents = 0;
        private int maxEvents = 0;
        private long maxSkew = 0;
        private long lastReport = 0;

        private boolean successful = false;

        OutputWorker(BufferedWriter output, boolean doReports, LinkedBlockingQueue<DatabaseBuffer> queue, Thread inputThread) {
            this.output = output;
            this.doReports = doReports;
            this.queue = queue;
            this.inputThread = inputThread;
        }

        private void printReport(long elapsedMillis, long skew) {
            System.err.printf(
                    "%6.1fs: %9d indices, %9d events, %9d max. events, %6.3fs skew, %6.3fs max. skew, %6d underruns\n",
                    (double) elapsedMillis / 1000.0,
                    indices,
                    totalEvents,
                    maxEvents,
                    (double) skew / 1000.0,
                    (double) maxSkew / 1000.0,
                    underruns
            );
        }

        public void run() {
            final long startTime = System.nanoTime();

            try {
                boolean isFirst = true;
                DatabaseBuffer databaseBuffer;
                do {
                    databaseBuffer = queue.poll();
                    if (databaseBuffer == null) {
                        if (!isFirst) {
                            ++underruns;
                        }
                        databaseBuffer = queue.take();
                    }
                    isFirst = false;

                    int numberOfEvents = databaseBuffer.getNumberOfEvents();
                    totalEvents += numberOfEvents;
                    maxEvents = Math.max(maxEvents, numberOfEvents);
                    ++indices;

                    long now = System.nanoTime();
                    long elapsedMillis = (now - startTime) / 1000000L;
                    long waitMillis = databaseBuffer.emissionTime - elapsedMillis;
                    long skew = Math.max(0, -waitMillis);

                    maxSkew = Math.max(maxSkew, skew);

                    if (doReports && elapsedMillis - lastReport > 1000) {
                        lastReport = elapsedMillis;
                        printReport(elapsedMillis, skew);
                    }

                    if (waitMillis > 1L) {
                        Thread.sleep(waitMillis);
                    }

                    databaseBuffer.write(output);
                    output.flush();
                } while (!databaseBuffer.isLast);

                successful = true;
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
                "Usage: [-v] [-a <acceleration>] [-q <buffer size>] [-m] [-o <host>:<port>] <file>\n");
        System.exit(1);
    }

    public static void main(String[] args) {
        String inputFilename = null;
        double timeMultiplier = 1.0;
        DatabaseBufferFactory databaseBufferFactory = new CsvDatabaseBufferFactory();
        String outputHost = null;
        int outputPort = 0;
        boolean doReport = false;
        int queueCapacity = 64;

        try {
            for (int i = 0; i < args.length; ++i) {
                switch (args[i]) {
                    case "-v":
                        doReport = true;
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
            invalidArgument();
            return;
        }

        BufferedReader inputReader;
        try {
            inputReader = new BufferedReader(new FileReader(inputFilename));
        } catch (FileNotFoundException e) {
            System.err.print("Error: " + e.getMessage() + "\n");
            System.exit(1);
            return;
        }

        BufferedWriter outputWriter;
        if (outputHost == null) {
            outputWriter = new BufferedWriter(new OutputStreamWriter(System.out));
        } else {
            try {
                ServerSocket serverSocket = new ServerSocket(outputPort, 1, InetAddress.getByName(outputHost));
                Socket outputSocket = serverSocket.accept();
                System.err.printf("Client connected: %s\n", outputSocket.getInetAddress().getHostAddress());
                outputWriter = new BufferedWriter(new OutputStreamWriter(outputSocket.getOutputStream()));
            } catch (IOException e) {
                System.err.print("Error: " + e.getMessage() + "\n");
                System.exit(1);
                return;
            }
        }

        LinkedBlockingQueue<DatabaseBuffer> queue = new LinkedBlockingQueue<>(queueCapacity);
        InputWorker inputWorker = new InputWorker(inputReader, timeMultiplier, databaseBufferFactory, queue);
        Thread inputThread = new Thread(inputWorker);
        inputThread.start();
        OutputWorker outputWorker = new OutputWorker(outputWriter, doReport, queue, inputThread);
        Thread outputThread = new Thread(outputWorker);
        outputThread.start();

        try {
            inputThread.join();
            if (!inputWorker.isSuccessful()) {
                outputThread.interrupt();
            }
            outputThread.join();
        } catch (InterruptedException ignored) {
        }

        if (!(inputWorker.isSuccessful() && outputWorker.isSuccessful())) {
            System.exit(1);
        }
    }
}
