package ch.ethz.infsec.trace.parser;

import ch.ethz.infsec.monitor.Fact;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Objects;

public class Crv2014CsvParser implements Serializable {
    private static final long serialVersionUID = -919182766017476946L;

    private final String eventFact;
    private final ArrayDeque<Fact> buffer;

    private String currentLine;
    private String lastTimePoint;
    private String lastTimestamp;

    transient private int position;
    transient private int wordStart;
    transient private int wordEnd;

    public Crv2014CsvParser(String eventFact) {
        this.eventFact = Objects.requireNonNull(eventFact);
        this.buffer = new ArrayDeque<>();
        this.currentLine = null;
        this.lastTimePoint = null;
        this.lastTimestamp = null;
    }

    public void setInput(String line) {
        currentLine = line;
    }

    private void addEventFact() {
        if (lastTimePoint != null && lastTimestamp != null) {
            buffer.add(new Fact(eventFact, lastTimestamp, lastTimePoint));
        }
    }

    public void terminateEvent() {
        addEventFact();
        lastTimePoint = null;
        lastTimestamp = null;
    }

    private boolean isSpace(char c) {
        return c == ' ' || c == '\t';
    }

    private boolean isLineEnd(char c) {
        return c == '\r' || c == '\n';
    }

    private boolean isWordEnd(char c) {
        return c == '=' || c == ',' || isLineEnd(c);
    }

    private void readWord() {
        while (position < currentLine.length() && isSpace(currentLine.charAt(position))) {
            ++position;
        }
        wordStart = wordEnd = position;
        while (position < currentLine.length() && !isWordEnd(currentLine.charAt(position))) {
            if (isSpace(currentLine.charAt(position))) {
                ++position;
            } else {
                wordEnd = ++position;
            }
        }
    }

    private String currentWord() {
        return currentLine.substring(wordStart, wordEnd);
    }

    private void expectWord(String word) throws ParseException {
        readWord();
        if (!word.equals(currentWord())) {
            throw new ParseException(currentLine, wordStart);
        }
    }

    private void expectDelimiter(char delimiter) throws ParseException {
        if (position < currentLine.length() && currentLine.charAt(position) == delimiter) {
            ++position;
        } else {
            throw new ParseException(currentLine, position);
        }
    }

    private boolean hasMore() {
        return position < currentLine.length() && !isLineEnd(currentLine.charAt(position));
    }

    public Fact nextFact() throws ParseException {
        if (buffer.isEmpty() && currentLine != null) {
            position = 0;
            readWord();
            if (wordStart < wordEnd || hasMore()) {
                final String factName = currentWord();

                expectDelimiter(',');
                expectWord("tp");
                expectDelimiter('=');
                readWord();
                final String timePoint = currentWord();

                expectDelimiter(',');
                expectWord("ts");
                expectDelimiter('=');
                readWord();
                final String timestamp = currentWord();

                if (!(timestamp.equals(lastTimestamp) && timePoint.equals(lastTimePoint))) {
                    addEventFact();
                    lastTimePoint = timePoint;
                    lastTimestamp = timestamp;
                }

                if (hasMore()) {
                    final ArrayList<String> arguments = new ArrayList<>();
                    do {
                        expectDelimiter(',');
                        readWord();
                        expectDelimiter('=');
                        readWord();
                        arguments.add(currentWord());
                    } while (hasMore());
                    buffer.add(new Fact(factName, arguments));
                } else {
                    buffer.add(new Fact(factName));
                }
            }
            currentLine = null;
        }
        return buffer.isEmpty() ? null : buffer.pop();
    }
}
