package ch.ethz.infsec.trace.formatter;

import ch.ethz.infsec.monitor.Fact;

import java.io.IOException;
import java.io.Serializable;

public class MonpolyVerdictFormatter extends AbstractMonpolyFormatter implements TraceFormatter, Serializable {
    private static final long serialVersionUID = 400845287706301597L;

    @Override
    public void printFact(TraceConsumer sink, Fact fact) throws IOException {
        if (fact.isTerminator()) {
            if (builder.length() > 0) {
                builder.append('\n');
                sink.accept(builder.toString());
                builder.setLength(0);
            }
        } else {
            assert fact.getName().equals("");
            if (builder.length() == 0) {
                builder.append('@');
                builder.append(fact.getTimestamp());
                builder.append(". (time point ");
                builder.append(fact.getArgument(0));
                builder.append("): ");
            }
            if (fact.getArity() == 1) {
                builder.append("true");
            } else {
                builder.append('(');
                printString((String) fact.getArgument(1), builder);
                for (int i = 2; i < fact.getArity(); ++i) {
                    builder.append(',');
                    printString((String) fact.getArgument(i), builder);
                }
                builder.append(')');
            }
        }
    }

    @Override
    public boolean getMarkDatabaseEnd() {
        return true;
    }

    @Override
    public void setMarkDatabaseEnd(boolean markDatabaseEnd) {
        // ignored: there are no databases in verdicts
    }

    @Override
    public boolean inInitialState() {
        return builder.length() == 0;
    }
}
