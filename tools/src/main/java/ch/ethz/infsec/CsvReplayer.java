package ch.ethz.infsec;

import java.io.*;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class CsvReplayer {
    static abstract class EventBuffer {
        final long emissionTime;
        boolean isLast = false;

        EventBuffer(long emissionTime) {
            this.emissionTime = emissionTime;
        }

        abstract void addRecord(String line, String relation, int indexBeforeData);

        abstract int getNumberOfRecords();

        abstract void write(Writer writer) throws IOException;
    }

    static final class MonpolyEventBuffer extends EventBuffer {
        private final long timestamp;
        private final Map<String, StringBuilder> relations = new HashMap<>();
        private int numberOfRecords = 0;

        MonpolyEventBuffer(long timestamp, long emissionTime) {
            super(emissionTime);
            this.timestamp = timestamp;
        }

        @Override
        void addRecord(String line, String relation, int indexBeforeData) {
            ++numberOfRecords;

            StringBuilder builder = relations.get(relation);
            if (builder == null) {
                builder = new StringBuilder();
                relations.put(relation, builder);
            }

            builder.append('(');
            int currentIndex = indexBeforeData;
            while (currentIndex < line.length()) {
                currentIndex = line.indexOf('=', currentIndex);
                if (currentIndex >= 0) {
                    currentIndex += 1;
                    while (currentIndex < line.length() && Character.isSpaceChar(line.charAt(currentIndex)))
                        currentIndex += 1;
                    int startIndex = currentIndex;
                    currentIndex = line.indexOf(',', startIndex);
                    boolean hasMore = currentIndex >= 0;
                    if (!hasMore)
                        currentIndex = line.length();
                    builder.append(line.substring(startIndex, currentIndex).trim());
                    if (hasMore)
                        builder.append(',');
                    currentIndex += 1;
                }
            }
            builder.append(')');
        }

        @Override
        int getNumberOfRecords() {
            return numberOfRecords;
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

    static final class CsvEventBuffer extends EventBuffer {
        private final ArrayList<String> lines = new ArrayList<>();

        CsvEventBuffer(long emissionTime) {
            super(emissionTime);
        }

        @Override
        void addRecord(String line, String relation, int indexBeforeData) {
            lines.add(line);
        }

        @Override
        int getNumberOfRecords() {
            return lines.size();
        }

        @Override
        void write(Writer writer) throws IOException {
            for (String line : lines) {
                writer.append(line).append('\n');
            }
        }
    }

    interface EventBufferFactory {
        EventBuffer createEventBuffer(long timestamp, long emissionTime);
    }

    static final class MonpolyEventBufferFactory implements EventBufferFactory {
        @Override
        public EventBuffer createEventBuffer(long timestamp, long emissionTime) {
            return new MonpolyEventBuffer(timestamp, emissionTime);
        }
    }

    static final class CsvEventBufferFactory implements EventBufferFactory {
        @Override
        public EventBuffer createEventBuffer(long timestamp, long emissionTime) {
            return new CsvEventBuffer(emissionTime);
        }
    }

    static class InputWorker implements Runnable {
        private final BufferedReader input;
        private final double timeMultiplier;
        private final EventBufferFactory factory;
        private final LinkedBlockingQueue<EventBuffer> queue;

        private boolean successful = false;

        private EventBuffer eventBuffer = null;
        private long firstTimestamp;
        private long currentTimepoint;

        InputWorker(BufferedReader input, double timeMultiplier, EventBufferFactory factory, LinkedBlockingQueue<EventBuffer> queue) {
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

                if (eventBuffer == null) {
                    eventBuffer = factory.createEventBuffer(0, -1);
                }
                eventBuffer.isLast = true;
                queue.put(eventBuffer);

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

            if (eventBuffer == null) {
                eventBuffer = factory.createEventBuffer(timestamp, 0);
                firstTimestamp = timestamp;
                currentTimepoint = timepoint;
            } else if (timepoint != currentTimepoint) {
                queue.put(eventBuffer);

                long nextEmission = Math.round((double) (timestamp - firstTimestamp) / timeMultiplier * 1000.0);
                eventBuffer = factory.createEventBuffer(timestamp, nextEmission);
                currentTimepoint = timepoint;
            }

            eventBuffer.addRecord(line, relation, currentIndex);
        }
    }

    static class OutputWorker implements Runnable {
        private final BufferedWriter output;
        private final boolean doReports;
        private final LinkedBlockingQueue<EventBuffer> queue;

        private int underruns = 0;
        private int totalRecords = 0;
        private int maxRecords = 0;
        private int events = 0;
        private long maxSkew = 0;
        private long lastReport = 0;

        private boolean successful = false;

        OutputWorker(BufferedWriter output, boolean doReports, LinkedBlockingQueue<EventBuffer> queue) {
            this.output = output;
            this.doReports = doReports;
            this.queue = queue;
        }

        private void printReport(long elapsedMillis, long skew) {
            System.err.printf(
                    "%6.1fs: %9d events, %9d records, %9d max. records, %6.3fs skew, %6.3fs max. skew, %6d underruns\n",
                    (double) elapsedMillis / 1000.0,
                    events,
                    totalRecords,
                    maxRecords,
                    (double) skew / 1000.0,
                    (double) maxSkew / 1000.0,
                    underruns
            );
        }

        public void run() {
            final long startTime = System.nanoTime();

            try {
                boolean isFirst = true;
                EventBuffer eventBuffer;
                do {
                    eventBuffer = queue.poll();
                    if (eventBuffer == null) {
                        if (!isFirst) {
                            ++underruns;
                        }
                        eventBuffer = queue.take();
                    }
                    isFirst = false;

                    int numberOfRecords = eventBuffer.getNumberOfRecords();
                    totalRecords += numberOfRecords;
                    maxRecords = Math.max(maxRecords, numberOfRecords);
                    ++events;

                    long now = System.nanoTime();
                    long elapsedMillis = (now - startTime) / 1000000L;
                    long waitMillis = eventBuffer.emissionTime - elapsedMillis;
                    long skew = Math.max(0, -waitMillis);

                    maxSkew = Math.max(maxSkew, skew);

                    if (doReports && elapsedMillis - lastReport > 1000) {
                        lastReport = elapsedMillis;
                        printReport(elapsedMillis, skew);
                    }

                    if (waitMillis > 1L) {
                        Thread.sleep(waitMillis);
                    }

                    eventBuffer.write(output);
                    output.flush();
                } while (!eventBuffer.isLast);

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
    }

    private static void invalidArgument() {
        System.err.print("Error: Invalid argument.\n" +
                "Usage: [-v] [-a <acceleration>] [-q <buffer size>] [-m] [-o <host>:<port>] <file>\n");
        System.exit(2);
    }

    public static void main(String[] args) {
        String inputFilename = null;
        double timeMultiplier = 1.0;
        EventBufferFactory eventBufferFactory = new CsvEventBufferFactory();
        String outputHost = null;
        int outputPort = 0;
        boolean doReport = false;
        int queueCapacity = 64;

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
                    eventBufferFactory = new MonpolyEventBufferFactory();
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

        Socket outputSocket;
        BufferedWriter outputWriter;
        if (outputHost == null) {
            outputWriter = new BufferedWriter(new OutputStreamWriter(System.out));
        } else {
            try {
                outputSocket = new Socket(outputHost, outputPort);
                outputWriter = new BufferedWriter(new OutputStreamWriter(outputSocket.getOutputStream()));
            } catch (IOException e) {
                System.err.print("Error: " + e.getMessage() + "\n");
                System.exit(1);
                return;
            }
        }

        LinkedBlockingQueue<EventBuffer> queue = new LinkedBlockingQueue<>(queueCapacity);
        InputWorker inputWorker = new InputWorker(inputReader, timeMultiplier, eventBufferFactory, queue);
        OutputWorker outputWorker = new OutputWorker(outputWriter, doReport, queue);

        Thread inputThread = new Thread(inputWorker);
        inputThread.start();
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
