package ch.ethz.infsec.trace.parser;

import ch.ethz.infsec.monitor.Fact;
import ch.ethz.infsec.trace.Trace;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.function.Consumer;

public class MonpolyTraceParser implements Serializable {
    private static final long serialVersionUID = 2830977317994540001L;

    private enum LexerState {
        INITIAL,
        SIMPLE_STRING,
        QUOTED_STRING,
        LINE_COMMENT
    }

    private enum TokenType {
        INCOMPLETE,
        AT,
        LEFT_PAREN,
        RIGHT_PAREN,
        COMMA,
        STRING,
        END
    }

    private enum ParserState {
        INITIAL,
        TIMESTAMP,
        TABLE,
        TUPLE,
        FIELD_1,
        AFTER_FIELD,
        FIELD_N
    }

    private transient String input;
    private transient int position;
    private transient boolean isEnd;
    private LexerState lexerState;
    private final StringBuilder tokenValue;
    private ParserState parserState;
    private String timestamp;
    private String relationName;
    private final ArrayList<String> fields;
    private final ArrayList<Fact> factBuffer;

    public MonpolyTraceParser() {
        this.lexerState = LexerState.INITIAL;
        this.tokenValue = new StringBuilder();
        this.parserState = ParserState.INITIAL;
        this.timestamp = null;
        this.relationName = null;
        this.fields = new ArrayList<>();
        this.factBuffer = new ArrayList<>();
    }

    private void error() throws ParseException {
        lexerState = LexerState.INITIAL;
        tokenValue.setLength(0);
        parserState = ParserState.INITIAL;
        timestamp = null;
        relationName = null;
        fields.clear();
        factBuffer.clear();
        throw new ParseException(input);
    }

    private boolean isSimpleStringChar(char c) {
        return (c >= '0' && c <= '9') ||
                (c >= 'A' && c <= 'Z') ||
                (c >= 'a' && c <= 'z') ||
                c == '_' || c == '[' || c == ']' || c == '/' ||
                c == ':' || c == '-' || c == '.' || c == '!';
    }

    private TokenType nextToken() throws ParseException {
        while (position < input.length()) {
            char c = input.charAt(position);
            switch (lexerState) {
                case INITIAL:
                    switch (c) {
                        case ' ':
                        case '\t':
                        case '\n':
                        case '\r':
                            ++position;
                            break;
                        case '@':
                            ++position;
                            return TokenType.AT;
                        case '(':
                            ++position;
                            return TokenType.LEFT_PAREN;
                        case ')':
                            ++position;
                            return TokenType.RIGHT_PAREN;
                        case ',':
                            ++position;
                            return TokenType.COMMA;
                        case '#':
                            lexerState = LexerState.LINE_COMMENT;
                            ++position;
                            break;
                        case '"':
                            tokenValue.setLength(0);
                            lexerState = LexerState.QUOTED_STRING;
                            ++position;
                            break;
                        default:
                            if (isSimpleStringChar(c)) {
                                tokenValue.setLength(0);
                                tokenValue.append(c);
                                lexerState = LexerState.SIMPLE_STRING;
                                ++position;
                            } else {
                                ++position;
                                error();
                            }
                    }
                    break;
                case SIMPLE_STRING:
                    if (isSimpleStringChar(c)) {
                        tokenValue.append(c);
                        ++position;
                    } else {
                        lexerState = LexerState.INITIAL;
                        return TokenType.STRING;
                    }
                    break;
                case QUOTED_STRING:
                    if (c == '"') {
                        lexerState = LexerState.INITIAL;
                        ++position;
                        return TokenType.STRING;
                    } else {
                        tokenValue.append(c);
                        ++position;
                    }
                    break;
                case LINE_COMMENT:
                    if (c == '\n' || c == '\r') {
                        lexerState = LexerState.INITIAL;
                    }
                    ++position;
                    break;
            }
        }
        if (isEnd) {
            switch (lexerState) {
                case INITIAL:
                case LINE_COMMENT:
                    lexerState = LexerState.INITIAL;
                    return TokenType.END;
                case SIMPLE_STRING:
                    lexerState = LexerState.INITIAL;
                    return TokenType.STRING;
                case QUOTED_STRING:
                    error();
            }
        }
        return TokenType.INCOMPLETE;
    }

    private String takeTokenValue() {
        final String value = tokenValue.toString();
        tokenValue.setLength(0);
        return value;
    }

    private void beginDatabase() {
        timestamp = null;
        relationName = null;
        fields.clear();
        factBuffer.clear();
    }

    private void finishDatabase(Consumer<Fact> sink) {
        factBuffer.forEach(sink);
        sink.accept(new Fact(Trace.EVENT_FACT, timestamp));
        timestamp = null;
        relationName = null;
        factBuffer.clear();
    }

    private void beginTuple() {
        fields.clear();
    }

    private void finishTuple() {
        factBuffer.add(new Fact(relationName, timestamp, new ArrayList<>(fields)));
        fields.clear();
    }

    private void runParser(Consumer<Fact> sink) throws ParseException {
        do {
            final TokenType tokenType = nextToken();
            if (tokenType == TokenType.INCOMPLETE) {
                return;
            }
            switch (parserState) {
                case INITIAL:
                    if (tokenType == TokenType.AT) {
                        beginDatabase();
                        parserState = ParserState.TIMESTAMP;
                    } else if (tokenType != TokenType.END) {
                        error();
                    }
                    break;
                case TIMESTAMP:
                    if (tokenType == TokenType.STRING) {
                        timestamp = takeTokenValue();
                        parserState = ParserState.TABLE;
                    } else {
                        error();
                    }
                    break;
                case TABLE:
                    if (tokenType == TokenType.STRING) {
                        relationName = takeTokenValue();
                        parserState = ParserState.TUPLE;
                    } else if (tokenType == TokenType.AT) {
                        finishDatabase(sink);
                        parserState = ParserState.TIMESTAMP;
                    } else if (tokenType == TokenType.END) {
                        finishDatabase(sink);
                        parserState = ParserState.INITIAL;
                        return;
                    } else {
                        error();
                    }
                    break;
                case TUPLE:
                    if (tokenType == TokenType.LEFT_PAREN) {
                        beginTuple();
                        parserState = ParserState.FIELD_1;
                    } else if (tokenType == TokenType.STRING) {
                        relationName = takeTokenValue();
                    } else if (tokenType == TokenType.AT) {
                        finishDatabase(sink);
                        parserState = ParserState.TIMESTAMP;
                    } else if (tokenType == TokenType.END) {
                        finishDatabase(sink);
                        parserState = ParserState.INITIAL;
                        return;
                    } else {
                        error();
                    }
                    break;
                case FIELD_1:
                    if (tokenType == TokenType.STRING) {
                        fields.add(takeTokenValue());
                        parserState = ParserState.AFTER_FIELD;
                    } else if (tokenType == TokenType.RIGHT_PAREN) {
                        finishTuple();
                        parserState = ParserState.TUPLE;
                    } else {
                        error();
                    }
                    break;
                case AFTER_FIELD:
                    if (tokenType == TokenType.COMMA) {
                        parserState = ParserState.FIELD_N;
                    } else if (tokenType == TokenType.RIGHT_PAREN) {
                        finishTuple();
                        parserState = ParserState.TUPLE;
                    } else {
                        error();
                    }
                    break;
                case FIELD_N:
                    if (tokenType == TokenType.STRING) {
                        fields.add(takeTokenValue());
                        parserState = ParserState.AFTER_FIELD;
                    } else {
                        error();
                    }
                    break;
            }
        } while (true);
    }

    public void endOfInput(Consumer<Fact> sink) throws ParseException {
        this.input = "";
        this.position = 0;
        this.isEnd = true;
        runParser(sink);
    }

    public void parse(Consumer<Fact> sink, String input) throws ParseException {
        this.input = input;
        this.position = 0;
        this.isEnd = false;
        runParser(sink);
    }
}
