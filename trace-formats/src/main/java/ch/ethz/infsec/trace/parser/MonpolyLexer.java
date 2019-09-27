package ch.ethz.infsec.trace.parser;

import java.io.Serializable;

class MonpolyLexer implements Serializable {
    private static final long serialVersionUID = 3735158732427625889L;

    private enum LexerState {
        INITIAL,
        SIMPLE_STRING,
        QUOTED_STRING,
        LINE_COMMENT
    }

    enum TokenType {
        INCOMPLETE,
        AT,
        LEFT_PAREN,
        RIGHT_PAREN,
        LEFT_ANGLE,
        RIGHT_ANGLE,
        COMMA,
        STRING,
        SEMICOLON,
        END
    }

    private transient String input;
    private transient int position;
    private transient boolean isEnd;
    private LexerState lexerState;
    private final StringBuilder tokenValue;

    MonpolyLexer() {
        this.lexerState = LexerState.INITIAL;
        this.tokenValue = new StringBuilder();
    }

    void setPartialInput(String input) {
        this.input = input;
        this.position = 0;
        this.isEnd = false;
    }

    void setCompleteInput(String input) {
        this.input = input;
        this.position = 0;
        this.isEnd = true;
    }

    void atEnd() {
        this.input = "";
        this.position = 0;
        this.isEnd = true;
    }

    String currentInput() {
        return input;
    }

    void reset() {
        lexerState = LexerState.INITIAL;
        tokenValue.setLength(0);
    }

    private boolean isSimpleStringChar(char c) {
        return (c >= '0' && c <= '9') ||
                (c >= 'A' && c <= 'Z') ||
                (c >= 'a' && c <= 'z') ||
                c == '_' || c == '[' || c == ']' || c == '/' ||
                c == ':' || c == '-' || c == '.' || c == '!';
    }

    TokenType nextToken() throws ParseException {
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
                        case '<':
                            ++position;
                            return TokenType.LEFT_ANGLE;
                        case '>':
                            ++position;
                            return TokenType.RIGHT_ANGLE;
                        case ',':
                            ++position;
                            return TokenType.COMMA;
                        case ';':
                            ++position;
                            return TokenType.SEMICOLON;
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
                                throw new ParseException(input);
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
                    throw new ParseException(input);
            }
        }
        return TokenType.INCOMPLETE;
    }

    String takeTokenValue() {
        final String value = tokenValue.toString();
        tokenValue.setLength(0);
        return value;
    }

    boolean inInitialState() {
        return lexerState == LexerState.INITIAL;
    }
}
