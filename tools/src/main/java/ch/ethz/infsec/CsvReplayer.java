package ch.ethz.infsec;

import java.io.*;
import java.net.Socket;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CsvReplayer {
    static final class EventBuffer {
        final StringBuilder builder = new StringBuilder();
        final long emissionTime;
        boolean isLast = false;

        EventBuffer(long emissionTime) {
            this.emissionTime = emissionTime;
        }
    }

    static class InputWorker implements Runnable {
        private static final Pattern recordPattern =
                Pattern.compile("(.+?), tp = \\d+, ts = (\\d+), (.*)");

        private final BufferedReader input;
        private final long windowSize;
        private final double eventRate;
        private final LinkedBlockingQueue<EventBuffer> queue;

        private final Random random = new Random(314159265359L);

        private boolean successful = false;

        private EventBuffer eventBuffer = null;
        private long windowStart;
        private long currentTimepoint;
        private long currentTimestamp;

        InputWorker(BufferedReader input, long windowSize, double eventRate, LinkedBlockingQueue<EventBuffer> queue) {
            this.input = input;
            this.windowSize = windowSize;
            this.eventRate = eventRate;
            this.queue = queue;
        }

        public void run() {
            try {
                String line;
                while ((line = input.readLine()) != null) {
                    Matcher matcher = recordPattern.matcher(line);
                    if (matcher.matches()) {
                        String relation = matcher.group(1);
                        long timestamp = Long.parseLong(matcher.group(2), 10);
                        String data = matcher.group(3);
                        processRecord(relation, timestamp, data);
                    }
                }

                if (eventBuffer == null) {
                    eventBuffer = new EventBuffer(-1);
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

        private long getNextEmission(long previous) {
            double u = random.nextDouble();
            double delta = -Math.log(1.0 - u) / eventRate;
            return previous + Math.round(delta * 1000.0);
        }

        private void processRecord(String relation, long timestamp, String data)
                throws InterruptedException {
            if (eventBuffer != null && timestamp < windowStart) {
                System.err.printf("Error: Timestamp %d is less than previous timestamp %d.\n",
                        timestamp, windowStart);
                throw new RuntimeException("Non-increasing timestamps detected");
            }

            if (eventBuffer == null) {
                eventBuffer = new EventBuffer(0);
                currentTimepoint = 0;
                currentTimestamp = 0;

                windowStart = timestamp;
            } else if (timestamp - windowStart >= windowSize) {
                queue.put(eventBuffer);

                long nextEmission = getNextEmission(eventBuffer.emissionTime);
                eventBuffer = new EventBuffer(nextEmission);
                ++currentTimepoint;
                currentTimestamp = nextEmission / 1000;

                windowStart = timestamp;
            }

            eventBuffer.builder
                    .append(relation)
                    .append(", tp = ")
                    .append(currentTimepoint)
                    .append(", ts = ")
                    .append(currentTimestamp)
                    .append(", ")
                    .append(data)
                    .append('\n');
        }
    }

    static class OutputWorker implements Runnable {
        private final BufferedWriter output;
        private final LinkedBlockingQueue<EventBuffer> queue;

        private boolean successful = false;

        OutputWorker(BufferedWriter output, LinkedBlockingQueue<EventBuffer> queue) {
            this.output = output;
            this.queue = queue;
        }

        public void run() {
            final long startTime = System.nanoTime();

            try {
                EventBuffer eventBuffer;
                do {
                    eventBuffer = queue.take();

                    long now = System.nanoTime();
                    long elapsedMillis = (now - startTime) / 1000000L;
                    long waitMillis = eventBuffer.emissionTime - elapsedMillis;
                    if (waitMillis > 1L) {
                        Thread.sleep(waitMillis);
                    }

                    output.write(eventBuffer.builder.toString());
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
                "Usage: [-w <window size>] [-r <event rate>] [-o host:port] <file>\n");
        System.exit(2);
    }

    public static void main(String[] args) {
        String inputFilename = null;
        long windowSize = 1;
        double eventRate = 1.0;
        String outputHost = null;
        int outputPort = 0;

        for (int i = 0; i < args.length; ++i) {
            switch (args[i]) {
                case "-w":
                    if (++i == args.length) {
                        invalidArgument();
                    }
                    windowSize = Long.parseLong(args[i]);
                    break;
                case "-r":
                    if (++i == args.length) {
                        invalidArgument();
                    }
                    eventRate = Double.parseDouble(args[i]);
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

        LinkedBlockingQueue<EventBuffer> queue = new LinkedBlockingQueue<>(64);
        InputWorker inputWorker = new InputWorker(inputReader, windowSize, eventRate, queue);
        OutputWorker outputWorker = new OutputWorker(outputWriter, queue);

        Thread inputThread = new Thread(inputWorker);
        inputThread.start();
        Thread outputThread = new Thread(outputWorker);
        outputThread.start();

        try {
            inputThread.join();
            outputThread.join();
        } catch (InterruptedException ignored) { }

        if (!(inputWorker.isSuccessful() && outputWorker.isSuccessful())) {
            System.exit(1);
        }
    }
}
