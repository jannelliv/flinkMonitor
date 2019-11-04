package ch.ethz.infsec.trace.formatter;

import ch.ethz.infsec.monitor.Fact;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.junit.Assert.assertEquals;

public class MonpolyVerdictFormatterTest {
    private StringBuilder sink;
    private MonpolyVerdictFormatter formatter;

    @Before
    public void setUp() {
        sink = new StringBuilder();
        formatter = new MonpolyVerdictFormatter();
    }

    @Test
    public void testPrintFact() throws Exception {
        formatter.printFact(sink::append, Fact.make("", 123L, "1001"));
        formatter.printFact(sink::append, Fact.terminator(123L));
        assertEquals("@123. (time point 1001): true\n", sink.toString());

        sink.setLength(0);
        formatter.printFact(sink::append, Fact.terminator(123L));
        assertEquals("", sink.toString());

        sink.setLength(0);
        formatter.printFact(sink::append, Fact.make("", 456L, "1002", "abc"));
        formatter.printFact(sink::append, Fact.make("", 456L, "1002", "def"));
        formatter.printFact(sink::append, Fact.terminator(456L));
        formatter.printFact(sink::append, Fact.make("", 456L, "1003", "foo bar", "42"));
        formatter.printFact(sink::append, Fact.terminator(456L));
        assertEquals("@456. (time point 1002): (abc)(def)\n@456. (time point 1003): (\"foo bar\",42)\n", sink.toString());
    }

    @Test
    public void testSerialization() throws Exception {
        formatter.printFact(sink::append, Fact.make("", 456L, "1002", "abc"));
        formatter.printFact(sink::append, Fact.make("", 456L, "1002", "def"));
        formatter.printFact(sink::append, Fact.terminator(456L));
        formatter.printFact(sink::append, Fact.make("", 456L, "1003", "foo bar", "42"));
        sink.setLength(0);

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final ObjectOutputStream objectOut = new ObjectOutputStream(out);
        objectOut.writeObject(formatter);

        final ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        final ObjectInputStream objectIn = new ObjectInputStream(in);
        formatter = (MonpolyVerdictFormatter) objectIn.readObject();

        formatter.printFact(sink::append, Fact.make("", 456L, "1003", "uvw", "xyz"));
        formatter.printFact(sink::append, Fact.terminator(456L));
        assertEquals("@456. (time point 1003): (\"foo bar\",42)(uvw,xyz)\n", sink.toString());
    }
}
