package ch.ethz.infsec.trace.parser;

import ch.ethz.infsec.monitor.Fact;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Crv2014CsvParser implements TraceParser, Serializable {
    private static final long serialVersionUID = -919182766017476946L;

    private String lastTimePoint;
    private String lastTimestamp;
    private boolean alreadyTerminated;

    public Crv2014CsvParser() {
        this.lastTimePoint = null;
        this.lastTimestamp = "-1";
        this.alreadyTerminated = false;
    }

    private void terminateEvent(Consumer<Fact> sink) {
        if (lastTimePoint != null && !alreadyTerminated) {
            sink.accept(Fact.terminator(lastTimestamp));
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

    private static final Pattern commandArgumentPattern = Pattern.compile("\\s*(<|[^<\" ]+|\"[^\"]*\")");

    @Override
    public void parseLine(Consumer<Fact> sink, String line) throws ParseException {
        assert !(lastTimestamp == null);
        final String trimmed = line.trim();
        if (trimmed.isEmpty()) {
            return;
        }

        if (trimmed.startsWith("EOF")) {
            sink.accept(Fact.meta("EOF"));
            return;
        }

        if (trimmed.equals(";;")) {
            terminateEvent(sink);
            alreadyTerminated = true;
            return;
        }
        if (trimmed.startsWith(">")) {
            terminateEvent(sink);
            alreadyTerminated = true;

            final Matcher matcher = commandArgumentPattern.matcher(trimmed.substring(1));
            String name;
            if (matcher.lookingAt()) {
                name = matcher.group(1);
                matcher.region(matcher.end(), trimmed.length() - 1);
            } else {
                throw new ParseException(trimmed);
            }
            final ArrayList<Object> arguments = new ArrayList<>();
            while (matcher.lookingAt() && !matcher.group(1).equals("<")) {
                final String matched = matcher.group(1);
                if (matched.startsWith("\"")) {
                    arguments.add(matched.substring(1, matched.length() - 1));
                } else {
                    arguments.add(matched);
                }
                matcher.region(matcher.end(), trimmed.length() - 1);
            }

            sink.accept(new Fact(name, null, arguments));
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

        final ArrayList<Object> arguments = new ArrayList<>();
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
            long lastTimeStampLong = Long.parseLong(lastTimestamp);
            long timeStampLong = Long.parseLong(timestamp);
            assert timeStampLong >= lastTimeStampLong;
            for (long ts = lastTimeStampLong + 1; ts <= timeStampLong; ++ts) {
                beginNewEvent(sink, timePoint, Long.toString(ts));
            }
        }
        sink.accept(new Fact(factName, timestamp, arguments));
    }

    @Override
    public boolean inInitialState() {
        return lastTimePoint == null || alreadyTerminated;
    }
}
