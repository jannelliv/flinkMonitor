package ch.ethz.infsec.trace.parser;

public class ParseException extends Exception {
    private static final long serialVersionUID = 4057773165895674158L;

    private final String context;

    public ParseException(String context) {
        super("Invalid input.");
        this.context = context;
    }

    public String getContext() {
        return context;
    }
}
