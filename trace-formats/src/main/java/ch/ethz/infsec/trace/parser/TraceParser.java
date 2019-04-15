package ch.ethz.infsec.trace.parser;

import ch.ethz.infsec.monitor.Fact;

import java.util.function.Consumer;

public interface TraceParser {
    void parseLine(Consumer<Fact> sink, String line) throws ParseException;
    void endOfInput(Consumer<Fact> sink) throws ParseException;
}
