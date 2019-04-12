package ch.ethz.infsec.trace.parser;

import ch.ethz.infsec.monitor.Fact;
import ch.ethz.infsec.trace.Trace;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

public class Crv2014CsvParser implements Serializable {
    private static final long serialVersionUID = -919182766017476946L;

    private String lastTimePoint;
    private String lastTimestamp;

    public Crv2014CsvParser() {
        this.lastTimePoint = null;
        this.lastTimestamp = null;
    }

    private void terminateEvent(Collection<Fact> sink, String newTimePoint, String newTimestamp) {
        if (lastTimePoint != null) {
            sink.add(new Fact(Trace.EVENT_FACT, lastTimestamp, Collections.emptyList()));
        }
        lastTimePoint = newTimePoint;
        lastTimestamp = newTimestamp;
    }

    public void endOfInput(Collection<Fact> sink) {
        terminateEvent(sink, null, null);
    }

    public void parseLine(Collection<Fact> sink, String line) throws ParseException {
        line = line.trim();
        if (line.isEmpty()) {
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
            terminateEvent(sink, timePoint, timestamp);
        }

        sink.add(new Fact(factName, timestamp, arguments));
    }
}
