package ch.ethz.infsec;

import org.apache.commons.math3.random.JDKRandomGenerator;
import org.apache.commons.math3.random.RandomGenerator;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class CsvStreamGenerator {
    private static void invalidArgument() {
        System.err.print("Error: Invalid argument.\n" +
                "Usage: -S|-L|-T [-e <event rate>] [-i <index rate>] [-x <violations>] [-w <window size>]\n" +
                "       [-pA <A ratio>] [-pB <B ratio>] [-z <Zipf exponents>] <seconds>\n");
        System.exit(1);
    }

    public static void main(String[] args) {
        PositiveNegativeGenerator.VariableGraph variableGraph = null;
        int eventRate = 10;
        int indexRate = 1;
        float relativeViolations = 0.01f;
        int windowSize = 10;
        float baseRatio = 0.33f;
        float positiveRatio = 0.33f;
        double zipfExponents[] = {};
        int streamLength = -1;

        try {
            for (int i = 0; i < args.length; ++i) {
                switch (args[i]) {
                    case "-S":
                        variableGraph = PositiveNegativeGenerator.VariableGraph.STAR;
                        break;
                    case "-L":
                        variableGraph = PositiveNegativeGenerator.VariableGraph.LINEAR;
                        break;
                    case "-T":
                        variableGraph = PositiveNegativeGenerator.VariableGraph.TRIANGLE;
                        break;
                    case "-e":
                        if (i + 1 == args.length) {
                            invalidArgument();
                        }
                        eventRate = Integer.parseInt(args[++i]);
                        break;
                    case "-i":
                        if (i + 1 == args.length) {
                            invalidArgument();
                        }
                        indexRate = Integer.parseInt(args[++i]);
                        break;
                    case "-x":
                        if (i + 1 == args.length) {
                            invalidArgument();
                        }
                        relativeViolations = Float.parseFloat(args[++i]);
                        break;
                    case "-w":
                        if (i + 1 == args.length) {
                            invalidArgument();
                        }
                        windowSize = Integer.parseInt(args[++i]);
                        break;
                    case "-pA":
                        if (i + 1 == args.length) {
                            invalidArgument();
                        }
                        baseRatio = Float.parseFloat(args[++i]);
                        break;
                    case "-pB":
                        if (i + 1 == args.length) {
                            invalidArgument();
                        }
                        positiveRatio = Float.parseFloat(args[++i]);
                        break;
                    case "-z":
                        if (i + 1 == args.length) {
                            invalidArgument();
                        }
                        String exponents[] = args[++i].split(",");
                        zipfExponents = new double[exponents.length];
                        for (int j = 0; j < exponents.length; ++j) {
                            zipfExponents[j] = Double.parseDouble(exponents[j]);
                        }
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
        if (variableGraph == null || streamLength <= 0) {
            invalidArgument();
        }

        float violationProbability = relativeViolations / (float)eventRate;

        RandomGenerator random = new JDKRandomGenerator(314159265);
        PositiveNegativeGenerator generator = new PositiveNegativeGenerator(random, eventRate, indexRate);
        generator.setVariableGraph(variableGraph);
        for (int i = 0; i < zipfExponents.length; ++i) {
            if (zipfExponents[i] > 0.0) {
                generator.setZipfExponent(i, zipfExponents[i]);
            }
        }
        generator.setEventDistribution(baseRatio, positiveRatio, violationProbability);
        generator.setPositiveWindow(windowSize);
        generator.setNegativeWindow(windowSize);
        generator.initialize();

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
