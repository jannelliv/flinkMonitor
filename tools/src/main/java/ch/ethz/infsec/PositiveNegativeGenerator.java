package ch.ethz.infsec;

import org.apache.commons.math3.distribution.IntegerDistribution;
import org.apache.commons.math3.distribution.UniformIntegerDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.random.RandomGenerator;

import java.util.PriorityQueue;

public class PositiveNegativeGenerator extends AbstractEventGenerator {
    private static final String BASE_RELATION = "A";
    private static final String POSITIVE_RELATION = "B";
    private static final String NEGATIVE_RELATION = "C";

    private static final String ATTRIBUTE_0 = "x";
    private static final String ATTRIBUTE_1 = "y";

    public enum VariableGraph { STAR, LINEAR, TRIANGLE }

    private VariableGraph variableGraph = VariableGraph.STAR;
    private IntegerDistribution distributions[];

    private float positiveRatio = 0.33f;
    private float negativeRatio = 0.33f;

    private int positiveWindow = 100;
    private int negativeWindow = 100;

    private float violationProbability = 0.01f;

    private static final class ScheduledEvent implements Comparable<ScheduledEvent> {
        final String relation;
        final long timestamp;
        final int value0;
        final int value1;

        ScheduledEvent(String relation, long timestamp, int value0, int value1) {
            this.relation = relation;
            this.timestamp = timestamp;
            this.value0 = value0;
            this.value1 = value1;
        }

        @Override
        public int compareTo(ScheduledEvent other) {
            return Long.compare(timestamp, other.timestamp);
        }
    }

    private final PriorityQueue<ScheduledEvent> positiveQueue = new PriorityQueue<>();
    private int positiveQueueCapacity;

    private final PriorityQueue<ScheduledEvent> negativeQueue = new PriorityQueue<>();
    private int negativeQueueCapacity;

    private int positiveAtCurrentIndex;
    private int negativeAtCurrentIndex;

    PositiveNegativeGenerator(RandomGenerator random, int eventRate, int indexRate) {
        super(random, eventRate, indexRate);
        computeQueueCapacities();

        distributions = new IntegerDistribution[4];
        for (int i = 0; i < 4; ++i) {
            distributions[i] = new UniformIntegerDistribution(random, 0, 999999999);
        }
    }

    void setVariableGraph(VariableGraph variableGraph) {
        this.variableGraph = variableGraph;
    }

    void setZipfVariable(int variableIndex, double exponent) {
        distributions[variableIndex] = new ZipfDistribution(random, 999999999, exponent);
    }

    void setRatios(float positive, float negative) {
        if (positive + negative >= 1.0f) {
            throw new IllegalArgumentException();
        }
        positiveRatio = positive;
        negativeRatio = negative;
        computeQueueCapacities();
    }

    void setWindows(int positive, int negative) {
        positiveWindow = positive;
        negativeWindow = negative;
        computeQueueCapacities();
    }

    void setViolationProbability(float p) {
        violationProbability = p;
    }

    private void computeQueueCapacities() {
        positiveQueueCapacity = (int) Math.ceil((float) positiveWindow * (float) eventRate * positiveRatio);
        negativeQueueCapacity = (int) Math.ceil((float) negativeWindow * (float) eventRate * negativeRatio);
    }

    @Override
    protected void initializeIndex(long timestamp) {
        positiveAtCurrentIndex = 0;
        negativeAtCurrentIndex = 0;
    }

    private boolean appendScheduledEvent(PriorityQueue<ScheduledEvent> queue, StringBuilder builder, long timestamp) {
        ScheduledEvent scheduledEvent = queue.peek();
        if (scheduledEvent != null && scheduledEvent.timestamp <= timestamp) {
            queue.remove();
            appendEventStart(builder, scheduledEvent.relation, timestamp);
            appendAttribute(builder, ATTRIBUTE_0, scheduledEvent.value0);
            appendAttribute(builder, ATTRIBUTE_1, scheduledEvent.value1);
            return true;
        } else {
            return false;
        }
    }

    private void appendEvent(StringBuilder builder, String relation, long timestamp, int value0, int value1) {
        appendEventStart(builder, relation, timestamp);
        appendAttribute(builder, ATTRIBUTE_0, value0);
        appendAttribute(builder, ATTRIBUTE_1, value1);
    }

    private boolean flipCoin(float p) {
        return random.nextFloat() < p;
    }

    private boolean canSchedulePositive() {
        return positiveQueue.size() < positiveQueueCapacity;
    }

    private boolean canScheduleViolation() {
        return canSchedulePositive() && negativeQueue.size() < negativeQueueCapacity;
    }

    private int nextValue(int variableIndex) {
        return distributions[variableIndex].sample();
    }

    private int[] nextValues() {
        int value0 = nextValue(0);
        int value1 = nextValue(1);
        int value2 = nextValue(2);
        int value3 = nextValue(3);
        switch (variableGraph) {
            case STAR:
                return new int[]{value0, value1, value0, value2, value0, value3};
            case LINEAR:
                return new int[]{value0, value1, value1, value2, value2, value3};
            case TRIANGLE:
                return new int[]{value0, value1, value1, value2, value2, value0};
            default:
                throw new IllegalStateException();
        }
    }

    @Override
    void appendNextEvent(StringBuilder builder, long timestamp) {
        if (appendScheduledEvent(negativeQueue, builder, timestamp)) {
            ++negativeAtCurrentIndex;
            return;
        }
        if (appendScheduledEvent(positiveQueue, builder, timestamp)) {
            ++positiveAtCurrentIndex;
            return;
        }

        if (flipCoin(violationProbability) && canScheduleViolation()) {
            int[] values = nextValues();
            long positiveTimestamp = timestamp + random.nextInt(positiveWindow);
            long negativeTimestamp = positiveTimestamp + random.nextInt(negativeWindow);

            appendEvent(builder, BASE_RELATION, timestamp, values[0], values[1]);
            positiveQueue.add(new ScheduledEvent(POSITIVE_RELATION, positiveTimestamp, values[2], values[3]));
            negativeQueue.add(new ScheduledEvent(NEGATIVE_RELATION, negativeTimestamp, values[4], values[5]));

            return;
        }

        float positiveAdjusted = positiveRatio - (float) positiveAtCurrentIndex / (float) eventsPerIndex;
        if (flipCoin(positiveAdjusted)) {
            int[] values = nextValues();
            appendEvent(builder, POSITIVE_RELATION, timestamp, values[2], values[3]);
            return;
        }

        float negativeAdjusted = negativeRatio - (float) negativeAtCurrentIndex / (float) eventsPerIndex;
        if (flipCoin(negativeAdjusted)) {
            int[] values = nextValues();
            appendEvent(builder, NEGATIVE_RELATION, timestamp, values[4], values[5]);
            return;
        }

        int[] values = nextValues();
        appendEvent(builder, BASE_RELATION, timestamp, values[0], values[1]);

        if (canSchedulePositive()) {
            long positiveTimestamp = timestamp + random.nextInt(positiveWindow);
            positiveQueue.add(new ScheduledEvent(POSITIVE_RELATION, positiveTimestamp, values[2], values[3]));
        }
    }
}
