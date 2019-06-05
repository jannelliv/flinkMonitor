package ch.ethz.infsec.trace.formatter;


import ch.ethz.infsec.monitor.Fact;
import ch.ethz.infsec.trace.Trace;

import java.util.List;

public class DejavuLinearizingTraceFormatter extends DejavuTraceFormatter{

    @Override
    public void printFact(StringBuilder writer, Fact fact) {
        if (!Trace.isEventFact(fact)) {
            Fact relation = fact;
            writer.append(relation.getName());
            List<String> args = relation.getArguments();
            for (String a: args) {
                writer.append(',');
                writer.append(a);
            }
            writer.append("\n");
        }
    }

}

