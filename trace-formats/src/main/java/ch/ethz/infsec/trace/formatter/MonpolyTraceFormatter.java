package ch.ethz.infsec.trace.formatter;

import ch.ethz.infsec.monitor.Fact;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

// TODO(JS): Can we stream out partial databases?
public class MonpolyTraceFormatter extends AbstractMonpolyFormatter implements TraceFormatter, Serializable {
    private static final long serialVersionUID = -4194915337286486289L;

    private boolean markDatabaseEnd = false;
    private boolean initialState = true;
    private final LinkedHashMap<String, ArrayList<Fact>> currentDatabase;

    public MonpolyTraceFormatter() {
        this.currentDatabase = new LinkedHashMap<>();
    }

    private void addFact(Fact fact) {
        final String name = fact.getName();
        final ArrayList<Fact> table = currentDatabase.computeIfAbsent(name, k -> new ArrayList<>());
        table.add(fact);
        initialState = false;
    }

    private void printAndClearDatabase() {
        for (Map.Entry<String, ArrayList<Fact>> entry : currentDatabase.entrySet()) {
            final ArrayList<Fact> facts = entry.getValue();
            if (!facts.isEmpty()) {
                builder.append(' ');
                printString(entry.getKey());
                for (Fact fact : facts) {
                    final List<Object> arguments = fact.getArguments();
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
                }
            }
            facts.clear();
        }
        initialState = true;
    }

    @Override
    public void printFact(TraceConsumer sink, Fact fact) throws IOException {
        if (fact.isMeta()) {
            builder.append('>');
            printString(fact.getName());
            for (Object arg : fact.getArguments()) {
                builder.append(' ');
                printString(arg.toString());
            }
            builder.append("<\n");
            sink.accept(builder.toString());
            builder.setLength(0);
        } else {
            if (fact.isTerminator()) {
                builder.append('@');
                printString(fact.getTimestamp().toString());
                printAndClearDatabase();
                if (markDatabaseEnd) {
                    builder.append(';');
                }
                builder.append('\n');
                sink.accept(builder.toString());
                builder.setLength(0);
            } else {
                addFact(fact);
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
        return initialState;
    }
}
