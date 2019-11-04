package ch.ethz.infsec.trace.parser;

import ch.ethz.infsec.monitor.Fact;
import ch.ethz.infsec.trace.parser.MonpolyLexer.TokenType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.function.Consumer;

public class MonpolyTraceParser implements TraceParser, Serializable {
    private static final long serialVersionUID = 2830977317994540001L;

    private enum ParserState {
        INITIAL,
        TIMESTAMP,
        TABLE,
        TUPLE,
        FIELD_1,
        AFTER_FIELD,
        FIELD_N,
        COMMAND_ARG_1,
        COMMAND_ARG_N
    }

    private MonpolyLexer lexer;
    private ParserState parserState;
    private String timestamp;
    private String relationName;
    private final ArrayList<String> fields;
    private final ArrayList<Fact> factBuffer;

    public MonpolyTraceParser() {
        this.lexer = new MonpolyLexer();
        this.parserState = ParserState.INITIAL;
        this.timestamp = null;
        this.relationName = null;
        this.fields = new ArrayList<>();
        this.factBuffer = new ArrayList<>();
    }

    private void error() throws ParseException {
        lexer.reset();
        parserState = ParserState.INITIAL;
        timestamp = null;
        relationName = null;
        fields.clear();
        factBuffer.clear();
        throw new ParseException(lexer.currentInput());
    }

    private void beginDatabase() {
        timestamp = null;
        relationName = null;
        fields.clear();
        factBuffer.clear();
    }

    private void finishDatabase(Consumer<Fact> sink) {
        factBuffer.forEach(sink);
        sink.accept(Fact.terminator(Long.valueOf(timestamp)));
        timestamp = null;
        relationName = null;
        factBuffer.clear();
    }

    private void beginTuple() {
        fields.clear();
    }

    private void finishTuple() {
        factBuffer.add(new Fact(relationName, Long.valueOf(timestamp), new ArrayList<>(fields)));
        fields.clear();
    }

    private void beginCommand() {
        relationName = null;
        fields.clear();
    }

    private void finishCommand(Consumer<Fact> sink) {
        sink.accept(new Fact(relationName, null, new ArrayList<>(fields)));
        fields.clear();
        relationName = null;
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
                        beginDatabase();
                        parserState = ParserState.TIMESTAMP;
                    } else if (tokenType == TokenType.RIGHT_ANGLE) {
                        beginCommand();
                        parserState = ParserState.COMMAND_ARG_1;
                    } else if (tokenType != TokenType.END) {
                        error();
                    }
                    break;
                case TIMESTAMP:
                    if (tokenType == TokenType.STRING) {
                        timestamp = lexer.takeTokenValue();
                        parserState = ParserState.TABLE;
                    } else {
                        error();
                    }
                    break;
                case TABLE:
                    if (tokenType == TokenType.STRING) {
                        relationName = lexer.takeTokenValue();
                        parserState = ParserState.TUPLE;
                    } else if (tokenType == TokenType.AT) {
                        finishDatabase(sink);
                        parserState = ParserState.TIMESTAMP;
                    } else if (tokenType == TokenType.SEMICOLON) {
                        finishDatabase(sink);
                        parserState = ParserState.INITIAL;
                    } else if (tokenType == TokenType.RIGHT_ANGLE) {
                        finishDatabase(sink);
                        parserState = ParserState.COMMAND_ARG_1;
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
                        relationName = lexer.takeTokenValue();
                    } else if (tokenType == TokenType.AT) {
                        finishDatabase(sink);
                        parserState = ParserState.TIMESTAMP;
                    } else if (tokenType == TokenType.SEMICOLON) {
                        finishDatabase(sink);
                        parserState = ParserState.INITIAL;
                    } else if (tokenType == TokenType.RIGHT_ANGLE) {
                        finishDatabase(sink);
                        parserState = ParserState.COMMAND_ARG_1;
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
                        fields.add(lexer.takeTokenValue());
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
                        fields.add(lexer.takeTokenValue());
                        parserState = ParserState.AFTER_FIELD;
                    } else {
                        error();
                    }
                    break;
                case COMMAND_ARG_1:
                    if (tokenType == TokenType.STRING) {
                        relationName = lexer.takeTokenValue();
                        parserState = ParserState.COMMAND_ARG_N;
                    } else {
                        error();
                    }
                    break;
                case COMMAND_ARG_N:
                    if (tokenType == TokenType.STRING) {
                        fields.add(lexer.takeTokenValue());
                    } else if (tokenType == TokenType.LEFT_ANGLE) {
                        finishCommand(sink);
                        parserState = ParserState.INITIAL;
                    } else {
                        error();
                    }
                    break;
            }
        } while (true);
    }

    @Override
    public void endOfInput(Consumer<Fact> sink) throws ParseException {
        lexer.atEnd();
        runParser(sink);
    }

    @Override
    public void dontSendTerminators(boolean set) {
        throw new RuntimeException("not implemented");
    }

    public void parse(Consumer<Fact> sink, String input) throws ParseException {
        lexer.setPartialInput(input);
        runParser(sink);
    }

    @Override
    public void parseLine(Consumer<Fact> sink, String line) throws ParseException {
        parse(sink, line);
    }

    @Override
    public boolean inInitialState() {
        return lexer.inInitialState() && parserState == ParserState.INITIAL;
    }
}
