package ch.ethz.infsec.monitor;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class Fact implements Serializable {
    // NOTE: In order to be efficiently serializable by Flink, this class must be recognised as a POJO/Java Bean.
    // Therefore, there must be a public no-argument constructor, and all fields must have getters and setters.

    private static final long serialVersionUID = 7532889759971015127L;

    private String name;
    private String timestamp;
    private List<String> arguments;

    public Fact() {
        this.name = "";
        this.timestamp = "";
        this.arguments = Collections.emptyList();
    }

    public Fact(String name, String timestamp, List<String> arguments) {
        this.name = Objects.requireNonNull(name, "name");
        this.timestamp = Objects.requireNonNull(timestamp, "timestamp");
        this.arguments = Objects.requireNonNull(arguments, "arguments");
    }

    public Fact(String name, String timestamp, String... arguments) {
        this.name = Objects.requireNonNull(name, "name");
        this.timestamp = Objects.requireNonNull(timestamp, "timestamp");
        this.arguments = Arrays.asList(arguments);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public List<String> getArguments() {
        return arguments;
    }

    public void setArguments(List<String> arguments) {
        this.arguments = arguments;
    }

    public int getArity() {
        return arguments.size();
    }

    public String getArgument(int index) {
        return arguments.get(index);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Fact fact = (Fact) o;
        return name.equals(fact.name) &&
                timestamp.equals(fact.timestamp) &&
                arguments.equals(fact.arguments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, timestamp, arguments);
    }

    @Override
    public String toString() {
        return name + "@" + timestamp + "(" + String.join(", ", arguments) + ")";
    }
}
