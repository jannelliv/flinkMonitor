package ch.ethz.infsec.trace.formatter;

import ch.ethz.infsec.monitor.Fact;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

// NOTE(JS): Does not support commands/meta facts. Should be removed anyway.
public class MonpolyLinearizingTraceFormatter extends MonpolyTraceFormatter {

    @Override
    public void printFact(TraceConsumer sink, Fact fact) throws IOException {
        if (!fact.isTerminator()) {
            builder.append('@');
            printString(fact.getTimestamp());
            builder.append(' ');
            printString(fact.getName());
            List<Object> arguments = fact.getArguments();
            builder.append('(');
            if (!arguments.isEmpty()) {
                // TODO(JS): Add support for other domain types.
                printString((String) arguments.get(0));
                for (int i = 1; i < arguments.size(); ++i) {
                    builder.append(',');
                    printString((String) arguments.get(i));
                }
            }
            builder.append(')');
            if (getMarkDatabaseEnd()) {
                builder.append(';');
            }
            builder.append('\n');

            sink.accept(builder.toString());
            builder.setLength(0);
        }
    }
}
