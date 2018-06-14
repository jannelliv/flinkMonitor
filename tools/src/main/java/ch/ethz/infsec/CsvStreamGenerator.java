package ch.ethz.infsec;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Random;

public class CsvStreamGenerator {
    private static void invalidArgument() {
        System.err.print("Error: Invalid argument.\n" +
                "Usage: [-e <event rate>] [-i <index rate>] [-x <violations>] [-w <window size>] [-t]\n" +
                "       [-p <positive ratio>] [-n <negative ratio>] <seconds>\n");
        System.exit(1);
    }

    public static void main(String[] args) {
        int eventRate = 10;
        int indexRate = 1;
        float relativeViolations = 0.01f;
        int windowSize = 10;
        boolean isTriangle = false;
        float positiveRatio = 0.33f;
        float negativeRatio = 0.33f;
        int streamLength = -1;

        try {
            for (int i = 0; i < args.length; ++i) {
                if (args[i].startsWith("-") && !args[i].equals("-v") && !args[i].equals("-t") && i + 1 == args.length) {
                    invalidArgument();
                }
                switch (args[i]) {
                    case "-e":
                        eventRate = Integer.parseInt(args[++i]);
                        break;
                    case "-i":
                        indexRate = Integer.parseInt(args[++i]);
                        break;
                    case "-x":
                        relativeViolations = Float.parseFloat(args[++i]);
                        break;
                    case "-w":
                        windowSize = Integer.parseInt(args[++i]);
                        break;
                    case "-t":
                        isTriangle = true;
                        break;
                    case "-p":
                        positiveRatio = Float.parseFloat(args[++i]);
                        break;
                    case "-n":
                        negativeRatio = Float.parseFloat(args[++i]);
                        break;
                    default:
                        if (streamLength > 0) {
                            invalidArgument();
                        }
                        streamLength = Integer.parseInt(args[i]);
                }
            }
        } catch (NumberFormatException e) {
            invalidArgument();
        }
        if (streamLength <= 0) {
            invalidArgument();
        }

        Random random = new Random(3141592654L);
        PositiveNegativeGenerator generator = new PositiveNegativeGenerator(random, eventRate, indexRate);
        generator.setRatios(positiveRatio, negativeRatio);
        generator.setWindows(windowSize, windowSize);
        generator.setViolationProbability(relativeViolations / (float) eventRate);
        generator.setIsTriangle(isTriangle);

        BufferedWriter outputWriter = new BufferedWriter(new OutputStreamWriter(System.out));
        int numberOfIndices = streamLength * indexRate;

        try {
            for (int i = 0; i < numberOfIndices; ++i) {
                outputWriter.write(generator.nextDatabase());
            }
            outputWriter.flush();
        } catch (IOException e) {
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }
}
