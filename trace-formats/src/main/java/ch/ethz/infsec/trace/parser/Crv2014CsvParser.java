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
    private boolean sendTerminators;

    public Crv2014CsvParser() {
        this.lastTimePoint = "0";
        this.lastTimestamp = "0";
        this.alreadyTerminated = false;
        this.sendTerminators = true;
    }

    private void terminateEvent(Consumer<Fact> sink) {
        if (lastTimePoint != null && !alreadyTerminated) {
            Fact fact = Fact.terminator(lastTimestamp);
            fact.setTimepoint(lastTimePoint);
            sink.accept(fact);
        }
    }

    private void beginNewEvent(Consumer<Fact> sink, String newTimePoint, String newTimestamp) {
        terminateEvent(sink);
        lastTimePoint = newTimePoint;
        lastTimestamp = newTimestamp;
        alreadyTerminated = false;
    }

    @Override
    public void dontSendTerminators(boolean set) {
        sendTerminators = !set;
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

        if (!(timePoint.equals(lastTimePoint) && timestamp.equals(lastTimestamp)) && sendTerminators) {
            beginNewEvent(sink, timePoint, timestamp);
        }
        Fact fact = new Fact(factName, timestamp, arguments);
        fact.setTimepoint(timePoint);
        sink.accept(fact);
    }

    @Override
    public boolean inInitialState() {
        return lastTimePoint == null || alreadyTerminated;
    }
}
