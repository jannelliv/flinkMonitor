package ch.ethz.infsec.trace.formatter;

import ch.ethz.infsec.monitor.Fact;
import ch.ethz.infsec.trace.Trace;

import java.io.Serializable;
import java.util.List;

public class DejavuTraceFormatter implements TraceFormatter, Serializable{

    // private static AtomicLong timestamps = new AtomicLong(0l);
    // private final long timestamp;
    private Fact relation = null;

    @Override
    public void printFact(StringBuilder writer, Fact fact) {
        if (Trace.isEventFact(fact)) {
            if (relation!=null) {
                writer.append(relation.getName());
                List<String> args = relation.getArguments();
                for (String a: args) {
                    writer.append(',');
                    writer.append(a);
                }
            }
            writer.append('\n');
            relation=null;
        } else {
            if (relation!=null) throw new IllegalStateException("More than one event per database");
            relation=fact;
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
}

