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

    private static final class ScheduledRecord implements Comparable<ScheduledRecord> {
        final String relation;
        final long timestamp;
        final int value0;
        final int value1;

        ScheduledRecord(String relation, long timestamp, int value0, int value1) {
            this.relation = relation;
            this.timestamp = timestamp;
            this.value0 = value0;
            this.value1 = value1;
        }

        @Override
        public int compareTo(ScheduledRecord other) {
            return Long.compare(timestamp, other.timestamp);
        }
    }

    private final PriorityQueue<ScheduledRecord> positiveQueue = new PriorityQueue<>();
    private int positiveQueueCapacity;

    private final PriorityQueue<ScheduledRecord> negativeQueue = new PriorityQueue<>();
    private int negativeQueueCapacity;

    private int positiveInCurrentEvent;
    private int negativeInCurrentEvent;

    PositiveNegativeGenerator(Random random, int load, int eventRate) {
        super(random, load, eventRate);
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
        positiveQueueCapacity = (int)Math.ceil((float)positiveWindow * (float)load * positiveRatio);
        negativeQueueCapacity = (int)Math.ceil((float)negativeWindow * (float)load * negativeRatio);
    }

    @Override
    protected void initializeEvent(long timestamp) {
        positiveInCurrentEvent = 0;
        negativeInCurrentEvent = 0;
    }

    private boolean appendScheduledRecord(PriorityQueue<ScheduledRecord> queue, StringBuilder builder, long timestamp) {
        ScheduledRecord scheduledRecord = queue.peek();
        if (scheduledRecord != null && scheduledRecord.timestamp <= timestamp) {
            queue.remove();
            appendRecordPrefix(builder, scheduledRecord.relation, timestamp);
            appendAttribute(builder, ATTRIBUTE_0, scheduledRecord.value0);
            appendAttribute(builder, ATTRIBUTE_1, scheduledRecord.value1);
            return true;
        } else {
            return false;
        }
    }

    private void appendRecord(StringBuilder builder, String relation, long timestamp, int value0, int value1) {
        appendRecordPrefix(builder, relation, timestamp);
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
    void appendNextRecord(StringBuilder builder, long timestamp) {
        if (appendScheduledRecord(negativeQueue, builder, timestamp)) {
            ++negativeInCurrentEvent;
            return;
        }
        if (appendScheduledRecord(positiveQueue, builder, timestamp)) {
            ++positiveInCurrentEvent;
            return;
        }

        if (flipCoin(violationProbability) && canScheduleViolation()) {
            int value0 = nextValue0();
            int value1 = nextValue1();
            int value2 = nextValue2();
            int value3 = isTriangle ? value0 : nextValue3();

            long positiveTimestamp = timestamp + random.nextInt(positiveWindow);
            long negativeTimestamp = positiveTimestamp + random.nextInt(negativeWindow);

            appendRecord(builder, BASE_RELATION, timestamp, value0, value1);
            positiveQueue.add(new ScheduledRecord(POSITIVE_RELATION, positiveTimestamp, value1, value2));
            negativeQueue.add(new ScheduledRecord(NEGATIVE_RELATION, negativeTimestamp, value2, value3));

            return;
        }

        float positiveAdjusted = positiveRatio - (float)positiveInCurrentEvent / (float)loadPerEvent;
        if (flipCoin(positiveAdjusted)) {
            int value1 = nextValue1();
            int value2 = nextValue2();
            appendRecord(builder, POSITIVE_RELATION, timestamp, value1, value2);
            return;
        }

        float negativeAdjusted = negativeRatio - (float)negativeInCurrentEvent / (float)loadPerEvent;
        if (flipCoin(negativeAdjusted)) {
            int value2 = nextValue2();
            int value3 = nextValue3();
            appendRecord(builder, NEGATIVE_RELATION, timestamp, value2, value3);
            return;
        }

        int value0 = nextValue0();
        int value1 = nextValue1();
        appendRecord(builder, BASE_RELATION, timestamp, value0, value1);

        if (flipCoin(inclusionProbability) && canSchedulePositive()) {
            int value2 = nextValue2();
            long positiveTimestamp = timestamp + random.nextInt(positiveWindow);

            positiveQueue.add(new ScheduledRecord(POSITIVE_RELATION, positiveTimestamp, value1, value2));
        }
    }
}
