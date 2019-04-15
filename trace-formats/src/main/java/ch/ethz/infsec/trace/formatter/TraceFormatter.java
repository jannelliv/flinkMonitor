package ch.ethz.infsec.trace.formatter;

import ch.ethz.infsec.monitor.Fact;

public interface TraceFormatter {
    void printFact(StringBuilder sink, Fact fact);
}
