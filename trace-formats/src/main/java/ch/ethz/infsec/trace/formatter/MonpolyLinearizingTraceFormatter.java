package ch.ethz.infsec.trace.formatter;

import ch.ethz.infsec.monitor.Fact;
import ch.ethz.infsec.trace.Trace;

import java.util.List;

public class MonpolyLinearizingTraceFormatter extends  MonpolyTraceFormatter {

    @Override
    public void printFact(StringBuilder sink, Fact fact) {
        if (!Trace.isEventFact(fact)) {
            sink.append('@');
            printString(sink, fact.getTimestamp());
            sink.append(' ');
            printString(sink, fact.getName());
            List<String> arguments = fact.getArguments();
            sink.append('(');
            if (!arguments.isEmpty()) {
                printString(sink, arguments.get(0));
                for (int i = 1; i < arguments.size(); ++i) {
                    sink.append(',');
                    printString(sink, arguments.get(i));
                }
            }
            sink.append(')');
            sink.append("\n");
        }
    }
}
