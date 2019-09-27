package ch.ethz.infsec.trace.formatter;

import ch.ethz.infsec.monitor.Fact;

import java.io.IOException;

// NOTE(JS): Does not support commands/meta facts. Should be removed anyway.
public class Crv2014CsvLinearizingFormatter extends Crv2014CsvFormatter {

    @Override
    public void printFact(TraceConsumer sink, Fact fact) throws IOException {
        if (!fact.isTerminator()) {
            builder.append(fact.getName());
            builder.append(", tp=");
            builder.append(currentTimePoint++);
            builder.append(", ts=");
            builder.append(fact.getTimestamp());

            final int arity = fact.getArity();
            for (int i = 0; i < arity; ++i) {
                builder.append(", x");
                builder.append(i);
                builder.append("=");
                builder.append(fact.getArgument(i));
            }
            if (getMarkDatabaseEnd()) {
                builder.append("\n;;\n");
            } else {
                builder.append("\n");
            }

            sink.accept(builder.toString());
            builder.setLength(0);
        }
    }


}
