package ch.ethz.infsec.trace.formatter;


import ch.ethz.infsec.monitor.Fact;

import java.io.IOException;
import java.util.List;

// NOTE(JS): Does not support commands/meta facts. Should be removed anyway.
public class DejavuLinearizingTraceFormatter extends DejavuTraceFormatter{

    @Override
    public void printFact(TraceConsumer sink, Fact fact) throws IOException {
        if (!fact.isTerminator()) {
            Fact relation = fact;
            builder.append(relation.getName());
            List<Object> args = relation.getArguments();
            for (Object a: args) {
                builder.append(',');
                builder.append(a);
            }
            builder.append("\n");
            sink.accept(builder.toString());
            builder.setLength(0);
        }
    }

}

