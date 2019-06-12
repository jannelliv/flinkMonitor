package ch.ethz.infsec.trace.formatter;

import ch.ethz.infsec.monitor.Fact;
import ch.ethz.infsec.trace.Trace;

public class Crv2014CsvLinearizingFormatter  extends  Crv2014CsvFormatter {

    @Override
    public void printFact(StringBuilder sink, Fact fact) {
        if (!Trace.isEventFact(fact)) {
            sink.append(fact.getName());
            sink.append(", tp=");
            sink.append(currentTimePoint++);
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
