package ch.ethz.infsec.trace.formatter;

import ch.ethz.infsec.monitor.Fact;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public class DejavuTraceFormatter implements TraceFormatter, Serializable {

    // private static AtomicLong timestamps = new AtomicLong(0l);
    // private final long timestamp;
    private Fact relation = null;
    StringBuilder builder = new StringBuilder();

    @Override
    public void printFact(TraceConsumer sink, Fact fact) throws IOException {
        if (fact.isTerminator()) {
            if (relation != null) {
                builder.append(relation.getName());
                List<Object> args = relation.getArguments();
                for (Object a : args) {
                    builder.append(',');
                    builder.append(a);
                }
            }
            builder.append('\n');
            sink.accept(builder.toString());
            builder.setLength(0);
            relation = null;
        } else {
            if (relation != null) throw new IllegalStateException("More than one event per database");
            relation = fact;
        }
    }

    @Override
    public boolean getMarkDatabaseEnd() {
        return true;
    }

    @Override
    public void setMarkDatabaseEnd(boolean markDatabaseEnd) {
        // ignored: there are no databases in Dejavu's format
    }

    @Override
    public boolean inInitialState() {
        return builder.length() == 0;
    }
}

