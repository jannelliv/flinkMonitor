package ch.ethz.infsec.trace.formatter;

import ch.ethz.infsec.monitor.Fact;

import java.io.IOException;
import java.io.Serializable;

public interface TraceFormatter extends Serializable {
    @FunctionalInterface
    interface TraceConsumer {
        void accept(String s) throws IOException;
    }

    void printFact(TraceConsumer sink, Fact fact) throws IOException;

    boolean getMarkDatabaseEnd();

    void setMarkDatabaseEnd(boolean markDatabaseEnd);

    boolean inInitialState();
}
