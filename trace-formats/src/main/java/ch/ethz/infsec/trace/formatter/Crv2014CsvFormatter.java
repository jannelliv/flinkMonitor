package ch.ethz.infsec.trace.formatter;

import ch.ethz.infsec.monitor.Fact;

import java.io.IOException;
import java.io.Serializable;

public class Crv2014CsvFormatter implements TraceFormatter, Serializable {
    private static final long serialVersionUID = -1880412127233585471L;

    private boolean markDatabaseEnd = false;
    long currentTimePoint;
    StringBuilder builder = new StringBuilder();

    public Crv2014CsvFormatter() {
        this.currentTimePoint = 0;
    }

    private void printCommandArgument(String s) {
        if (s.chars().anyMatch((c) -> c == ' ' || c == '"' || c == '<')) {
            builder.append('"');
            builder.append(s);
            builder.append('"');
        } else {
            builder.append(s);
        }
    }

    @Override
    public void printFact(TraceConsumer sink, Fact fact) throws IOException {
        if (fact.isMeta()) {
            builder.append('>');
            printCommandArgument(fact.getName());
            for (Object arg : fact.getArguments()) {
                builder.append(' ');
                printCommandArgument(arg.toString());
            }
            builder.append("<\n");
            sink.accept(builder.toString());
            builder.setLength(0);
        } else {
            if (fact.isTerminator()) {
                if (markDatabaseEnd) {
                    sink.accept(";;\n");
                }
                ++currentTimePoint;
            } else {
                builder.append(fact.getName());
                builder.append(", tp=");
                builder.append(currentTimePoint);
                builder.append(", ts=");
                builder.append(fact.getTimestamp());

                final int arity = fact.getArity();
                for (int i = 0; i < arity; ++i) {
                    builder.append(", x");
                    builder.append(i);
                    builder.append("=");
                    builder.append(fact.getArgument(i));
                }
                builder.append("\n");

                sink.accept(builder.toString());
                builder.setLength(0);
            }
        }
    }

    public boolean getMarkDatabaseEnd() {
        return markDatabaseEnd;
    }

    public void setMarkDatabaseEnd(boolean markDatabaseEnd) {
        this.markDatabaseEnd = markDatabaseEnd;
    }

    @Override
    public boolean inInitialState() {
        return currentTimePoint == 0 && builder.length() == 0;
    }
}
