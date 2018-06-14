package ch.ethz.infsec;

import java.util.Random;

abstract class AbstractEventGenerator {
    protected final Random random;
    protected final int load;
    protected final int loadPerEvent;
    protected final long timeIncrement;

    private long currentTimepoint = -1;
    private long currentEmissionTime = -1;
    private long currentTimestamp = 0;

    protected AbstractEventGenerator(Random random, int load, int eventRate) {
        if (load < eventRate || eventRate < 1) {
           throw new IllegalArgumentException();
        }

        this.random = random;
        this.load = load;
        this.loadPerEvent = load / eventRate;
        this.timeIncrement = 1000000000L / eventRate;
    }

    abstract void appendNextRecord(StringBuilder builder, long timestamp);

    String nextEvent() {
        ++currentTimepoint;
        if (currentEmissionTime < 0) {
            currentEmissionTime = 0;
        } else {
            currentEmissionTime += timeIncrement;
            currentTimestamp = currentEmissionTime / 1000000000L;
        }

        initializeEvent(currentTimestamp);

        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < loadPerEvent; ++i) {
            appendNextRecord(builder, currentTimestamp);
            builder.append('\n');
        }

        return builder.toString();
    }

    long getCurrentEmissionTime() {
        return currentEmissionTime;
    }

    protected void initializeEvent(long timestamp) {
    }

    protected void appendRecordPrefix(StringBuilder builder, String relation, long timestamp) {
        builder
                .append(relation)
                .append(", tp=")
                .append(currentTimepoint)
                .append(", ts=")
                .append(timestamp);
    }

    protected void appendAttribute(StringBuilder builder, String name, int value) {
        builder
                .append(", ")
                .append(name)
                .append('=')
                .append(value);
    }
    protected void appendAttribute(StringBuilder builder, String name, String value) {
        builder
                .append(", ")
                .append(name)
                .append('=')
                .append(value);
    }
}
