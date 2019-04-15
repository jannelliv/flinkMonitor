package ch.ethz.infsec.trace.formatter;

import ch.ethz.infsec.monitor.Fact;
import ch.ethz.infsec.trace.Trace;

import java.io.Serializable;

public class Crv2014CsvFormatter implements TraceFormatter, Serializable {
    private static final long serialVersionUID = -1880412127233585471L;

    private long currentTimePoint;

    public Crv2014CsvFormatter() {
        this.currentTimePoint = 0;
    }

    @Override
    public void printFact(StringBuilder sink, Fact fact) {
        if (Trace.isEventFact(fact)) {
            ++currentTimePoint;
        } else {
            sink.append(fact.getName());
            sink.append(", tp=");
            sink.append(currentTimePoint);
            sink.append(", ts=");
            sink.append(fact.getTimestamp());

            final int arity = fact.getArity();
            for (int i = 0; i < arity; ++i) {
                sink.append(", x");
                sink.append(i);
                sink.append("=");
                sink.append(fact.getArgument(i));
            }
            sink.append("\n");
        }
    }
}
