package ch.ethz.infsec.trace.formatter;

import ch.ethz.infsec.monitor.Fact;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
                printString(entry.getKey(), builder);
                for (Fact fact : facts) {
                    final List<Object> arguments = fact.getArguments();
                    builder.append('(');
                    if (!arguments.isEmpty()) {
                        // TODO(JS): Add support for other domain types.
                        printString((String) arguments.get(0), builder);
                        for (int i = 1; i < arguments.size(); ++i) {
                            builder.append(',');
                            printString((String) arguments.get(i), builder);
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
            final StringBuilder tempBuilder = new StringBuilder();
            tempBuilder.append('>');
            printString(fact.getName(), tempBuilder);
            for (Object arg : fact.getArguments()) {
                tempBuilder.append(' ');
                if (fact.getName().equals("set_slicer"))
                    tempBuilder.append(arg.toString());
                else
                    printString(arg.toString(), tempBuilder);
            }
            tempBuilder.append("<\n");
            sink.accept(tempBuilder.toString());
        } else {
            if (fact.isTerminator()) {
                builder.append('@');
                printString(Long.toString(fact.getTimestamp()), builder);
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
