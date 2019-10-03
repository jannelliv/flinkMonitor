package ch.ethz.infsec.trace.formatter;

import java.io.Serializable;

abstract class AbstractMonpolyFormatter implements Serializable {
    private static final long serialVersionUID = 341268080256224906L;

    StringBuilder builder = new StringBuilder();

    private boolean isSimpleStringChar(char c) {
        return (c >= '0' && c <= '9') ||
                (c >= 'A' && c <= 'Z') ||
                (c >= 'a' && c <= 'z') ||
                c == '_' || c == '[' || c == ']' || c == '/' ||
                c == ':' || c == '-' || c == '.' || c == '!';
    }

    private boolean isSimpleString(String value) {
        for (int i = 0; i < value.length(); ++i) {
            if (!isSimpleStringChar(value.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    void printString(String value, StringBuilder builder) {
        if (isSimpleString(value)) {
            builder.append(value);
        } else {
            builder.append('"');
            builder.append(value);
            builder.append('"');
        }
    }
}
