package ch.ethz.infsec.trace.parser;

public class ParseException extends Exception {
    protected final String input;
    protected final int position;

    public ParseException(String input, int position) {
        super();
        this.input = input;
        this.position = position;
    }
}
