package ch.ethz.infsec;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.Random;

public class CsvStreamSynthesizer {
    private static void invalidArgument() {
        System.err.print("Error: Invalid argument.\n" +
                "Usage: [-l load] [-r rate] [-x violations] [-p positive] [-n negative] [-w window] [-t]\n" +
                "       [-d] [-s seconds] [-o host:port]\n");
        System.exit(2);
    }

    public static void main(String[] args) {
        int load = 10;
        int eventRate = 1;
        float positiveRatio = 0.33f;
        float negativeRatio = 0.33f;
        int windowSize = 10;
        float relativeViolations = 0.01f;
        boolean isTriangle = false;
        int streamLength = 60;
        boolean dump = false;
        String outputHost = null;
        int outputPort = 0;

        for (int i = 0; i < args.length; ++i) {
            if (args[i].startsWith("-") && !args[i].equals("-t") && !args[i].equals("-d") && i + 1 == args.length) {
                invalidArgument();
            }
            switch (args[i]) {
                case "-l":
                    load = Integer.parseInt(args[++i]);
                    break;
                case "-r":
                    eventRate = Integer.parseInt(args[++i]);
                    break;
                case "-p":
                    positiveRatio = Float.parseFloat(args[++i]);
                    break;
                case "-n":
                    negativeRatio = Float.parseFloat(args[++i]);
                    break;
                case "-w":
                    windowSize = Integer.parseInt(args[++i]);
                    break;
                case "-x":
                    relativeViolations = Float.parseFloat(args[++i]);
                    break;
                case "-t":
                    isTriangle = true;
                    break;
                case "-s":
                    streamLength = Integer.parseInt(args[++i]);
                    break;
                case "-d":
                    dump = true;
                    break;
                case "-o":
                    String parts[] = args[++i].split(":", 2);
                    if (parts.length != 2) {
                        invalidArgument();
                    }
                    outputHost = parts[0];
                    outputPort = Integer.parseInt(parts[1]);
                    break;
                default:
                    invalidArgument();
            }
        }

        Random random = new Random(3141592654L);
        PositiveNegativeGenerator generator = new PositiveNegativeGenerator(random, load, eventRate);
        generator.setRatios(positiveRatio, negativeRatio);
        generator.setWindows(windowSize, windowSize);
        generator.setViolationProbability(relativeViolations / (float)load);
        generator.setIsTriangle(isTriangle);

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

        try {
            if (dump) {
                int events = streamLength * eventRate;
                for (int i = 0; i < events; ++i) {
                    outputWriter.write(generator.nextEvent());
                }
                outputWriter.flush();
            } else {
                long startTime = System.nanoTime();
                long elapsedSeconds;
                do {
                    String event = generator.nextEvent();
                    long emissionTime = generator.getCurrentEmissionTime();

                    long now = System.nanoTime();
                    elapsedSeconds = (now - startTime) / 1000000000L;
                    long waitMillis = (emissionTime - (now - startTime)) / 1000000L;
                    if (waitMillis > 1L) {
                        Thread.sleep(waitMillis);
                    }

                    outputWriter.write(event);
                    outputWriter.flush();
                } while (elapsedSeconds < streamLength);
            }
        } catch (IOException e) {
            System.err.print("Error: " + e.getMessage() + "\n");
            System.exit(1);
        } catch (InterruptedException e) {
            System.exit(1);
        }
    }
}
