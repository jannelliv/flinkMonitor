package ch.ethz.infsec.monitor;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A fact represents a single tuple in a temporal structure. It stores the name of the relation that the tuple is part
 * of the arguments of the tuple, and the time-stamp. The position within the temporal structure is implicit: It is
 * encoded by the position of the fact in a stream. Moreover, each structure (i.e., the set of relations associated with
 * a single point in time) is closed by a special <em>terminator fact</em>, which does not have a relation name.
 * <p>
 * The argument list can be heterogeneous. The only admissible types are {@link String}, {@link Long}, and {@link
 * Double}, though this is currently not enforced.
 * <p>
 * Meta-facts carry metadata that is not part of the temporal structure. Meta-facts do not have a time-stamp.
 * <p>
 * This class can be serialized efficiently with Kryo using {@link FactSerializer}.
 */
public class Fact implements Serializable {
    private static final long serialVersionUID = -7663140134871742122L;

    // Invariant: At most one of name and (timestamp and timepoint) is null.
    private String name;
    private Long timestamp;
    private Long timepoint;
    private List<Object> arguments;

    public Fact(String name, Long timestamp, List<Object> arguments) {
        this.name = name;
        this.timestamp = timestamp;
        this.arguments = Objects.requireNonNull(arguments, "arguments");
    }


    public static Fact make(String name, Long timestamp, Object... arguments) {
        return new Fact(name, timestamp, Arrays.asList(arguments));
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setTimepoint(Long timepoint) {
        this.timepoint = timepoint;
    }

    public Long getTimepoint() { return timepoint; }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
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

    public static Fact terminator(Long timestamp) {
        return new Fact(null, timestamp, Collections.emptyList());
    }

    public boolean isMeta() {
        return timestamp == null;
    }

    public static Fact meta(String name, Object... arguments) {
        return new Fact(name, null, Arrays.asList(arguments));
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
        return Objects.hash(name, timestamp, arguments);
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
