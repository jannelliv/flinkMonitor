package ch.ethz.infsec.trace.parser;

import ch.ethz.infsec.monitor.Fact;

import java.io.Serializable;
import java.util.function.Consumer;

public interface TraceParser extends Serializable {
    enum TerminatorMode {
        NO_TERMINATORS,
        ONLY_TIMESTAMPS,
        ALL_TERMINATORS
    }
    void parseLine(Consumer<Fact> sink, String line) throws ParseException;
    void endOfInput(Consumer<Fact> sink) throws ParseException;
    void setTerminatorMode(TerminatorMode mode);
    boolean inInitialState();
}
