package ch.ethz.infsec.trace.parser;

import ch.ethz.infsec.monitor.Fact;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.function.Consumer;

public class DejavuTraceParser implements TraceParser, Serializable {

    private boolean timed=false;
    private long lastTimePoint=0l;
    private Long lastTimeStamp;

    public DejavuTraceParser(){

    }
    public DejavuTraceParser(boolean timed){
        this.timed = timed;
    }

    @Override
    public void parseLine(Consumer<Fact> sink, String line) throws ParseException {
        final String trimmed = line.trim();
        if (trimmed.isEmpty()) {
            return;
        }

        String[] elems = line.split(",");
        String name = elems[0].trim();
        int n = elems.length;
        lastTimeStamp = 0l;
        if (timed) {
            try {
                lastTimeStamp = Long.valueOf(elems[elems.length - 1].trim());
            } catch (NumberFormatException e){
                throw new ParseException("Timestamp format: "+line);
            }

            n=n-1;
        }
        final ArrayList<Object> arguments = new ArrayList<>();
        for (int i = 1; i < n ; i++) {
            arguments.add(elems[i].trim());
        }
        Fact ret = Fact.make(name,lastTimeStamp,arguments);
        ret.setTimepoint(this.lastTimePoint);
        sink.accept(ret);
        Fact fact = Fact.terminator(lastTimeStamp);
        fact.setTimepoint(lastTimePoint);
        sink.accept(fact);
        this.lastTimePoint=lastTimePoint+1;

    }

    @Override
    public void endOfInput(Consumer<Fact> sink) throws ParseException {

    }

    @Override
    public void setTerminatorMode(TerminatorMode mode) {
        throw new RuntimeException("not implemented");
    }

    @Override
    public boolean inInitialState() {
        return lastTimeStamp == null;
    }
}
