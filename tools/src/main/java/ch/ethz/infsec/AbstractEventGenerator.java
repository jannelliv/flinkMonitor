package ch.ethz.infsec;

import java.util.Random;

abstract class AbstractEventGenerator {
    final Random random;
    final int eventRate;
    final int eventsPerIndex;

    private final long timeIncrement;

    private long currentIndex = -1;
    private long currentEmissionTime = -1;
    private long currentTimestamp = 0;

    AbstractEventGenerator(Random random, int eventRate, int indexRate) {
        if (eventRate < indexRate || indexRate < 1) {
           throw new IllegalArgumentException();
        }

        this.random = random;
        this.eventRate = eventRate;
        this.eventsPerIndex = eventRate / indexRate;
        this.timeIncrement = 1000000000L / indexRate;
    }

    abstract void initializeIndex(long timestamp);

    abstract void appendNextEvent(StringBuilder builder, long timestamp);

    String nextDatabase() {
        ++currentIndex;
        if (currentEmissionTime < 0) {
            currentEmissionTime = 0;
        } else {
            currentEmissionTime += timeIncrement;
            currentTimestamp = currentEmissionTime / 1000000000L;
        }

        initializeIndex(currentTimestamp);

        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < eventsPerIndex; ++i) {
            appendNextEvent(builder, currentTimestamp);
            builder.append('\n');
        }

        return builder.toString();
    }

    long getCurrentEmissionTime() {
        return currentEmissionTime;
    }

    void appendEventStart(StringBuilder builder, String relation, long timestamp) {
        builder
                .append(relation)
                .append(", tp=")
                .append(currentIndex)
                .append(", ts=")
                .append(timestamp);
    }

    void appendAttribute(StringBuilder builder, String name, int value) {
        builder
                .append(", ")
                .append(name)
                .append('=')
                .append(value);
    }
}
