package ch.ethz.infsec.trace.parser;

import ch.ethz.infsec.monitor.Fact;
import ch.ethz.infsec.trace.parser.MonpolyLexer.TokenType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.function.Consumer;

public class MonpolyVerdictParser implements TraceParser, Serializable {
    private static final long serialVersionUID = -2756131507581696530L;

    private enum ParserState {
        INITIAL,
        TIMESTAMP,
        TIMEPOINT_0,
        TIMEPOINT_1,
        TIMEPOINT_2,
        TIMEPOINT_3,
        TIMEPOINT_4,
        TIMEPOINT_5,
        TUPLE,
        FIELD_1,
        AFTER_FIELD,
        FIELD_N
    }

    private MonpolyLexer lexer;
    private ParserState parserState;
    private String timestamp;
    private String timepoint;
    private final ArrayList<String> fields;

    public MonpolyVerdictParser() {
        this.lexer = new MonpolyLexer();
        this.parserState = ParserState.INITIAL;
        this.timestamp = null;
        this.timepoint = null;
        this.fields = new ArrayList<>();
    }

    private void error() throws ParseException {
        lexer.reset();
        parserState = ParserState.INITIAL;
        timestamp = null;
        timepoint = null;
        fields.clear();
        throw new ParseException(lexer.currentInput());
    }

    private void beginVerdict() {
        timestamp = null;
        timepoint = null;
        fields.clear();
    }

    private void finishVerdict(Consumer<Fact> sink) {
        sink.accept(Fact.terminator(timestamp));
        timestamp = null;
        timepoint = null;
        fields.clear();
    }

    private void beginTuple() {
        fields.clear();
        fields.add(timepoint);
    }

    private void finishTuple(Consumer<Fact> sink) {
        sink.accept(new Fact("", timestamp, new ArrayList<>(fields)));
        fields.clear();
    }

    private void runParser(Consumer<Fact> sink) throws ParseException {
        do {
            TokenType tokenType = TokenType.INCOMPLETE;
            try {
                tokenType = lexer.nextToken();
            } catch (ParseException e) {
                error();
            }
            if (tokenType == TokenType.INCOMPLETE) {
                return;
            }
            switch (parserState) {
                case INITIAL:
                    if (tokenType == TokenType.AT) {
                        beginVerdict();
                        parserState = ParserState.TIMESTAMP;
                    } else if (tokenType != TokenType.END) {
                        error();
                    }
                    break;
                case TIMESTAMP:
                    if (tokenType == TokenType.STRING) {
                        timestamp = lexer.takeTokenValue();
                        if (timestamp.endsWith(".")) {
                            timestamp = timestamp.substring(0, timestamp.length() - 1);
                        } else {
                            error();
                        }
                        parserState = ParserState.TIMEPOINT_0;
                    } else {
                        error();
                    }
                    break;
                case TIMEPOINT_0:
                    if (tokenType == TokenType.LEFT_PAREN) {
                        parserState = ParserState.TIMEPOINT_1;
                    } else {
                        error();
                    }
                    break;
                case TIMEPOINT_1:
                    if (tokenType == TokenType.STRING && lexer.takeTokenValue().equals("time")) {
                        parserState = ParserState.TIMEPOINT_2;
                    } else {
                        error();
                    }
                    break;
                case TIMEPOINT_2:
                    if (tokenType == TokenType.STRING && lexer.takeTokenValue().equals("point")) {
                        parserState = ParserState.TIMEPOINT_3;
                    } else {
                        error();
                    }
                    break;
                case TIMEPOINT_3:
                    if (tokenType == TokenType.STRING) {
                        timepoint = lexer.takeTokenValue();
                        parserState = ParserState.TIMEPOINT_4;
                    } else {
                        error();
                    }
                    break;
                case TIMEPOINT_4:
                    if (tokenType == TokenType.RIGHT_PAREN) {
                        parserState = ParserState.TIMEPOINT_5;
                    } else {
                        error();
                    }
                    break;
                case TIMEPOINT_5:
                    if (tokenType == TokenType.STRING && lexer.takeTokenValue().equals(":")) {
                        parserState = ParserState.TUPLE;
                    } else {
                        error();
                    }
                    break;
                case TUPLE:
                    if (tokenType == TokenType.LEFT_PAREN) {
                        beginTuple();
                        parserState = ParserState.FIELD_1;
                    } else if (tokenType == TokenType.STRING) {
                        final String value = lexer.takeTokenValue();
                        if (value.equals("true")) {
                            beginTuple();
                            finishTuple(sink);
                        } else {
                            error();
                        }
                    } else if (tokenType == TokenType.END) {
                        finishVerdict(sink);
                        parserState = ParserState.INITIAL;
                        return;
                    } else {
                        error();
                    }
                    break;
                case FIELD_1:
                    if (tokenType == TokenType.STRING) {
                        fields.add(lexer.takeTokenValue());
                        parserState = ParserState.AFTER_FIELD;
                    } else if (tokenType == TokenType.RIGHT_PAREN) {
                        finishTuple(sink);
                        parserState = ParserState.TUPLE;
                    } else {
                        error();
                    }
                    break;
                case AFTER_FIELD:
                    if (tokenType == TokenType.COMMA) {
                        parserState = ParserState.FIELD_N;
                    } else if (tokenType == TokenType.RIGHT_PAREN) {
                        finishTuple(sink);
                        parserState = ParserState.TUPLE;
                    } else {
                        error();
                    }
                    break;
                case FIELD_N:
                    if (tokenType == TokenType.STRING) {
                        fields.add(lexer.takeTokenValue());
                        parserState = ParserState.AFTER_FIELD;
                    } else {
                        error();
                    }
                    break;
            }
        } while (true);
    }

    @Override
    public void parseLine(Consumer<Fact> sink, String line) throws ParseException {
        lexer.setCompleteInput(line);
        runParser(sink);
    }

    @Override
    public void endOfInput(Consumer<Fact> sink) throws ParseException {
        // Verdicts are self-contained lines.
    }

    @Override
    public boolean inInitialState() {
        return lexer.inInitialState() && parserState == ParserState.INITIAL;
    }
}
