package ch.ethz.infsec.monitor;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A fact represents a single tuple in a temporal structure. It stores the name of the relation that the tuple is part
 * of, the arguments of the tuple, and the time-stamp. The position within the temporal structure is implicit: It is
 * encoded by the position of the fact in a stream. Moreover, each structure (i.e., the set of relations associated with
 * a single point in time) is closed by a special <em>terminator fact</em>, which does not have a relation name (i.e., it
 * is null).
 * <p>
 * The argument list can be heterogeneous. The only admissible types are {@link String}, {@link Long}, and {@link
 * Double}, though this is currently not enforced.
 * <p>
 * Verdict-facts are not part of the temporal structure, they are the outputs of a monitor.
 * Verdict-facts have empty strings (i.e., "") as their names. Time-points, time-stamps and arguments have
 * valid values.
 * <p>
 * Meta-facts carry metadata that is not part of the temporal structure. Meta-facts do not have a time-stamp.
 * <p>
 * This class can be serialized efficiently with Kryo using {@link FactSerializer}.
 */
public class Fact implements Serializable {
    private static final long serialVersionUID = -7663140134871742122L;

    // Invariant: At most one of name and (timestamp and timepoint) is null.
    private String name;
    private long timestamp;
    private long timepoint = -1;
    private List<Object> arguments;

    public Fact(){

    }

    private Fact(String name, long timestamp, List<Object> arguments) {
        this.name = name;
        this.timestamp = timestamp;
        this.arguments = arguments;
    }


    private Fact(String name, long timestamp, Object... arguments) {
        this.name = name;
        this.timestamp = timestamp;
        this.arguments = Arrays.asList(arguments);
    }

    public static Fact make(String name, long timestamp, List<Object> arguments) {
        return new Fact(name, timestamp, arguments);
    }

    public static Fact make(String name, long timestamp, Object... arguments) {
        return new Fact(name, timestamp, arguments);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setTimepoint(long timepoint) {
        this.timepoint = timepoint;
    }

    public long getTimepoint() { return timepoint; }

    public long getTimestamp() { return timestamp; }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public List<Object> getArguments() {
        return arguments;
    }

    public void setArguments(List<Object> arguments) {
        this.arguments = Objects.requireNonNull(arguments, "arguments");
    }

    public int getArity() {
        return arguments.size();
    }

    public Object getArgument(int index) {
        return arguments.get(index);
    }

    public boolean isTerminator() {
        return name == null;
    }

    public static Fact terminator(long timestamp) {
        assert timestamp != -1;
        return new Fact(null, timestamp, Collections.emptyList());
    }

    public boolean isMeta() {
        return timestamp == -1;
    }

    public static Fact meta(String name, List<Object> arguments) {
        return new Fact(name, -1, arguments);
    }

    public static Fact meta(String name, Object... arguments) {
        return new Fact(name, -1, arguments);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Fact fact = (Fact) o;
        return Objects.equals(name, fact.name) &&
                Objects.equals(timestamp, fact.timestamp) &&
                arguments.equals(fact.arguments);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, timepoint, timestamp, arguments);
    }

    @Override
    public String toString() {
        if (isTerminator()) {
            return "@" + timestamp;
        } else {
            final String args = arguments.stream().map(Object::toString).collect(Collectors.joining(", "));
            if (isMeta()) {
                return name + " (" + args + ")";
            } else {
                return "@" + timestamp + " " + name + " (" + args + ")";
            }
        }
    }
}
