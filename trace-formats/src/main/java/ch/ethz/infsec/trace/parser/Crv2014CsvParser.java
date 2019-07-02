package ch.ethz.infsec.trace.parser;

import ch.ethz.infsec.monitor.Fact;
import ch.ethz.infsec.trace.Trace;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.function.Consumer;

public class Crv2014CsvParser implements TraceParser, Serializable {
    private static final long serialVersionUID = -919182766017476946L;

    private String lastTimePoint;
    private String lastTimestamp;
    private boolean alreadyTerminated;

    public Crv2014CsvParser() {
        this.lastTimePoint = null;
        this.lastTimestamp = null;
        this.alreadyTerminated = false;
    }

    private void terminateEvent(Consumer<Fact> sink) {
        if (lastTimePoint != null && !alreadyTerminated) {
            sink.accept(new Fact(Trace.EVENT_FACT, lastTimestamp, Collections.emptyList()));
        }
    }

    private void beginNewEvent(Consumer<Fact> sink, String newTimePoint, String newTimestamp) {
        terminateEvent(sink);
        lastTimePoint = newTimePoint;
        lastTimestamp = newTimestamp;
        alreadyTerminated = false;
    }

    @Override
    public void endOfInput(Consumer<Fact> sink) {
        beginNewEvent(sink, null, null);
    }

    @Override
    public void parseLine(Consumer<Fact> sink, String line) throws ParseException {
        final String trimmed = line.trim();
        if (trimmed.isEmpty()) {
            return;
        }
        if (trimmed.equals(";;")) {
            terminateEvent(sink);
            alreadyTerminated = true;
            return;
        }

        int start;
        int end;

        start = 0;
        end = line.indexOf(',', start);
        if (end < 0) {
            throw new ParseException(line);
        }
        final String factName = line.substring(start, end).trim();

        start = line.indexOf('=', end + 1) + 1;
        if (start <= 0) {
            throw new ParseException(line);
        }
        end = line.indexOf(',', start);
        if (end < 0) {
            throw new ParseException(line);
        }
        final String timePoint = line.substring(start, end).trim();

        start = line.indexOf('=', end + 1) + 1;
        if (start <= 0) {
            throw new ParseException(line);
        }
        end = line.indexOf(',', start);
        if (end < 0) {
            end = line.length();
        }
        final String timestamp = line.substring(start, end).trim();

        final ArrayList<String> arguments = new ArrayList<>();
        while (end < line.length()) {
            start = line.indexOf('=', end + 1) + 1;
            if (start <= 0) {
                throw new ParseException(line);
            }
            end = line.indexOf(',', start);
            if (end < 0) {
                end = line.length();
            }
            arguments.add(line.substring(start, end).trim());
        }

        if (!(timePoint.equals(lastTimePoint) && timestamp.equals(lastTimestamp))) {
            beginNewEvent(sink, timePoint, timestamp);
        }

        sink.accept(new Fact(factName, timestamp, arguments));
    }
}
