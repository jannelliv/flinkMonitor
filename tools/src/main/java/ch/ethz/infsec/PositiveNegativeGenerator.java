package ch.ethz.infsec;

import org.apache.commons.math3.distribution.IntegerDistribution;
import org.apache.commons.math3.distribution.UniformIntegerDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.random.RandomGenerator;

import java.util.PriorityQueue;

class PositiveNegativeGenerator extends AbstractEventGenerator {
    private static final String BASE_RELATION = "A";
    private static final String POSITIVE_RELATION = "B";
    private static final String NEGATIVE_RELATION = "C";

    private static final String ATTRIBUTE_0 = "x";
    private static final String ATTRIBUTE_1 = "y";

    private static final int NEGATIVE_SKEW_SHIFT = 1_000_000;

    public enum VariableGraph { STAR, LINEAR, TRIANGLE }

    private VariableGraph variableGraph = VariableGraph.STAR;
    private IntegerDistribution distributions[];
    private IntegerDistribution violationDistributions[];
    private boolean hasSkew = false;

    private float baseRatio = 0.33f;
    private float positiveRatio = 0.33f;
    private float violationProbability = 0.01f;

    private int positiveWindow = 100;
    private int negativeWindow = 100;

    private float baseProbability;
    private float positiveProbability;

    private static final class ScheduledData implements Comparable<ScheduledData> {
        final long timestamp;
        final int value0;
        final int value1;

        ScheduledData(long timestamp, int value0, int value1) {
            this.timestamp = timestamp;
            this.value0 = value0;
            this.value1 = value1;
        }

        @Override
        public int compareTo(ScheduledData other) {
            return Long.compare(timestamp, other.timestamp);
        }
    }

    private final PriorityQueue<ScheduledData> positiveQueue = new PriorityQueue<>();
    private final PriorityQueue<ScheduledData> negativeQueue = new PriorityQueue<>();

    PositiveNegativeGenerator(RandomGenerator random, int eventRate, int indexRate) {
        super(random, eventRate, indexRate);

        distributions = new IntegerDistribution[4];
        violationDistributions = new IntegerDistribution[4];
        for (int i = 0; i < 4; ++i) {
            distributions[i] = new UniformIntegerDistribution(random, 0, 999_999_999);
            violationDistributions[i] = distributions[i];
        }
    }

    void setVariableGraph(VariableGraph variableGraph) {
        this.variableGraph = variableGraph;
    }

    void setZipfExponent(int variableIndex, double exponent) {
        distributions[variableIndex] = new ZipfDistribution(random, 1_000_000_000, exponent);
        hasSkew = true;
    }

    void setEventDistribution(float baseRatio, float positiveRatio, float violationProbability) {
        if (baseRatio + positiveRatio > 1.0f) {
            throw new IllegalArgumentException("The sum of base and positive ratios cannot be larger than 1.");
        }
        if (baseRatio > positiveRatio) {
            throw new IllegalArgumentException("The base ratio cannot be larger than the positive ratio.");
        }
        if (violationProbability > baseRatio) {
            throw new IllegalArgumentException("The violation probability cannot be larger than the base ratio.");
        }
        if (violationProbability > 1.0f - baseRatio - positiveRatio) {
            throw new IllegalArgumentException("The violation probability cannot be larger than the negative ratio.");
        }

        this.baseRatio = baseRatio;
        this.positiveRatio = positiveRatio;
        this.violationProbability = violationProbability;
    }

    void setPositiveWindow(int window) {
        positiveWindow = window;
    }

    void setNegativeWindow(int window) {
        negativeWindow = window;
    }

    @Override
    void initialize() {
        baseProbability = (baseRatio - violationProbability) / (1.0f - violationProbability);
        positiveProbability = positiveRatio / (1.0f - baseRatio);
    }

    private boolean flipCoin(float p) {
        return random.nextFloat() < p;
    }

    private int nextValue(int variableIndex) {
        return distributions[variableIndex].sample();
    }

    private int nextViolationValue(int variableIndex) {
        return violationDistributions[variableIndex].sample();
    }

    private int[] nextValues(boolean isViolation, boolean correlated) {
        int value0, value1, value2, value3;
        if (isViolation) {
            value0 = nextViolationValue(0);
            value1 = nextViolationValue(1);
            value2 = nextViolationValue(2);
            value3 = nextViolationValue(3);
        } else {
            value0 = nextValue(0);
            value1 = nextValue(1);
            value2 = nextValue(2);
            value3 = nextValue(3);
        }

        int negativeShift;
        if (!correlated && hasSkew) {
            negativeShift = NEGATIVE_SKEW_SHIFT;
        } else {
            negativeShift = 0;
        }

        switch (variableGraph) {
            case STAR:
                return new int[]{value0, value1, value0, value2, value0 + negativeShift, value3 + negativeShift};
            case LINEAR:
                return new int[]{value0, value1, value1, value2, value2 + negativeShift, value3 + negativeShift};
            case TRIANGLE:
                return new int[]{value0, value1, value1, value2, value2 + negativeShift, value0 + negativeShift};
            default:
                throw new IllegalStateException();
        }
    }

    private void appendEvent(StringBuilder builder, String relation, int value0, int value1) {
        appendEventStart(builder, relation);
        appendAttribute(builder, ATTRIBUTE_0, value0);
        appendAttribute(builder, ATTRIBUTE_1, value1);
    }

    private void appendEventUsingSchedule(StringBuilder builder, PriorityQueue<ScheduledData> queue, long timestamp, String relation, int index0, int index1) {
        int value0, value1;
        ScheduledData scheduledData = queue.peek();
        if (scheduledData != null && scheduledData.timestamp <= timestamp) {
            queue.remove();
            value0 = scheduledData.value0;
            value1 = scheduledData.value1;
        } else {
            int[] values = nextValues(false, false);
            value0 = values[index0];
            value1 = values[index1];
        }
        appendEvent(builder, relation, value0, value1);
    }

    @Override
    void appendNextEvent(StringBuilder builder, long timestamp) {
        if (flipCoin(violationProbability)) {
            int[] values = nextValues(true, true);
            long positiveTimestamp = timestamp + random.nextInt(positiveWindow);
            long negativeTimestamp = positiveTimestamp + random.nextInt(negativeWindow);

            appendEvent(builder, BASE_RELATION, values[0], values[1]);
            positiveQueue.add(new ScheduledData(positiveTimestamp, values[2], values[3]));
            negativeQueue.add(new ScheduledData(negativeTimestamp, values[4], values[5]));

            return;
        }

        if (flipCoin(baseProbability)) {
            int[] values = nextValues(false, true);
            long positiveTimestamp = timestamp + random.nextInt(positiveWindow);

            appendEvent(builder, BASE_RELATION, values[0], values[1]);
            positiveQueue.add(new ScheduledData(positiveTimestamp, values[2], values[3]));

            return;
        }

        if (flipCoin(positiveProbability)) {
            appendEventUsingSchedule(builder, positiveQueue, timestamp, POSITIVE_RELATION, 2, 3);
            return;
        }

        appendEventUsingSchedule(builder, negativeQueue, timestamp, NEGATIVE_RELATION, 4, 5);
    }
}
