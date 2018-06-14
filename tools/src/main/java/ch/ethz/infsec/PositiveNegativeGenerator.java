package ch.ethz.infsec;

import java.util.PriorityQueue;
import java.util.Random;

public class PositiveNegativeGenerator extends AbstractEventGenerator {
    private static final String BASE_RELATION = "A";
    private static final String POSITIVE_RELATION = "B";
    private static final String NEGATIVE_RELATION = "C";

    private static final String ATTRIBUTE_0 = "x";
    private static final String ATTRIBUTE_1 = "y";

    private float positiveRatio = 0.33f;
    private float negativeRatio = 0.33f;

    private float inclusionProbability = 1.0f;
    private int positiveWindow = 100;

    private float violationProbability = 0.01f;
    private int negativeWindow = 100;
    private boolean isTriangle = false;

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

    PositiveNegativeGenerator(Random random, int eventRate, int indexRate) {
        super(random, eventRate, indexRate);
        computeQueueCapacities();
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

    void setInclusionProbability(float p) {
        inclusionProbability = p;
    }

    void setViolationProbability(float p) {
        violationProbability = p;
    }

    void setIsTriangle(boolean triangle) {
        isTriangle = triangle;
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

    private int nextValue0() {
        return random.nextInt(100000000);
    }

    private int nextValue1() {
        return random.nextInt(100000000);
    }

    private int nextValue2() {
        return random.nextInt(100000000);
    }

    private int nextValue3() {
        return random.nextInt(100000000);
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
            int value0 = nextValue0();
            int value1 = nextValue1();
            int value2 = nextValue2();
            int value3 = isTriangle ? value0 : nextValue3();

            long positiveTimestamp = timestamp + random.nextInt(positiveWindow);
            long negativeTimestamp = positiveTimestamp + random.nextInt(negativeWindow);

            appendEvent(builder, BASE_RELATION, timestamp, value0, value1);
            positiveQueue.add(new ScheduledEvent(POSITIVE_RELATION, positiveTimestamp, value1, value2));
            negativeQueue.add(new ScheduledEvent(NEGATIVE_RELATION, negativeTimestamp, value2, value3));

            return;
        }

        float positiveAdjusted = positiveRatio - (float) positiveAtCurrentIndex / (float) eventsPerIndex;
        if (flipCoin(positiveAdjusted)) {
            int value1 = nextValue1();
            int value2 = nextValue2();
            appendEvent(builder, POSITIVE_RELATION, timestamp, value1, value2);
            return;
        }

        float negativeAdjusted = negativeRatio - (float) negativeAtCurrentIndex / (float) eventsPerIndex;
        if (flipCoin(negativeAdjusted)) {
            int value2 = nextValue2();
            int value3 = nextValue3();
            appendEvent(builder, NEGATIVE_RELATION, timestamp, value2, value3);
            return;
        }

        int value0 = nextValue0();
        int value1 = nextValue1();
        appendEvent(builder, BASE_RELATION, timestamp, value0, value1);

        if (flipCoin(inclusionProbability) && canSchedulePositive()) {
            int value2 = nextValue2();
            long positiveTimestamp = timestamp + random.nextInt(positiveWindow);

            positiveQueue.add(new ScheduledEvent(POSITIVE_RELATION, positiveTimestamp, value1, value2));
        }
    }
}
