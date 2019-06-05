package ch.ethz.infsec.trace.formatter;

import ch.ethz.infsec.monitor.Fact;
import ch.ethz.infsec.trace.Trace;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MonpolyTraceFormatter implements TraceFormatter, Serializable {
    private static final long serialVersionUID = -4455544905696931178L;

    private final LinkedHashMap<String, ArrayList<Fact>> currentDatabase;

    public MonpolyTraceFormatter() {
        this.currentDatabase = new LinkedHashMap<>();
    }

    private void addFact(Fact fact) {
        final String name = fact.getName();
        final ArrayList<Fact> table = currentDatabase.computeIfAbsent(name, k -> new ArrayList<>());
        table.add(fact);
    }

    private boolean isSimpleStringChar(char c) {
        return (c >= '0' && c <= '9') ||
                (c >= 'A' && c <= 'Z') ||
                (c >= 'a' && c <= 'z') ||
                c == '_' || c == '[' || c == ']' || c == '/' ||
                c == ':' || c == '-' || c == '.' || c == '!';
    }

    private boolean isSimpleString(String value) {
        for (int i = 0; i < value.length(); ++i) {
            if (!isSimpleStringChar(value.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    protected void printString(StringBuilder sink, String value) {
        if (isSimpleString(value)) {
            sink.append(value);
        } else {
            sink.append('"');
            sink.append(value);
            sink.append('"');
        }
    }

    private void printAndClearDatabase(StringBuilder sink) {
        for (Map.Entry<String, ArrayList<Fact>> entry : currentDatabase.entrySet()) {
            final ArrayList<Fact> facts = entry.getValue();
            if (!facts.isEmpty()) {
                sink.append(' ');
                printString(sink, entry.getKey());
                for (Fact fact : facts) {
                    final List<String> arguments = fact.getArguments();
                    sink.append('(');
                    if (!arguments.isEmpty()) {
                        printString(sink, arguments.get(0));
                        for (int i = 1; i < arguments.size(); ++i) {
                            sink.append(',');
                            printString(sink, arguments.get(i));
                        }
                    }
                    sink.append(')');
                }
            }
            facts.clear();
        }
    }

    @Override
    public void printFact(StringBuilder sink, Fact fact) {
        if (Trace.isEventFact(fact)) {
            sink.append('@');
            printString(sink, fact.getTimestamp());
            printAndClearDatabase(sink);
            sink.append('\n');
        } else {
            addFact(fact);
        }
    }
}
