package ch.ethz.infsec.trace.formatter;

import ch.ethz.infsec.monitor.Fact;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.junit.Assert.assertEquals;

public class MonpolyTraceFormatterTest {
    private StringBuilder sink;
    private MonpolyTraceFormatter formatter;

    @Before
    public void setUp() {
        sink = new StringBuilder();
        formatter = new MonpolyTraceFormatter();
    }

    @Test
    public void testPrintFact() throws Exception {
        formatter.printFact(sink::append, Fact.make("abc", 123L));
        formatter.printFact(sink::append, Fact.terminator(123L));
        assertEquals("@123 abc()\n", sink.toString());

        sink.setLength(0);
        formatter.printFact(sink::append, Fact.terminator(123L));
        assertEquals("@123\n", sink.toString());

        sink.setLength(0);
        formatter.printFact(sink::append, Fact.make("abc", 456L));
        formatter.printFact(sink::append, Fact.make("def", 456L, "uvw"));
        formatter.printFact(sink::append, Fact.make("abc", 456L));
        formatter.printFact(sink::append, Fact.make("def", 456L, "xyz"));
        formatter.printFact(sink::append, Fact.terminator(456L));
        formatter.printFact(sink::append, Fact.make("def", 456L, "()", "foo bar"));
        formatter.printFact(sink::append, Fact.terminator(456L));
        assertEquals("@456 abc()() def(uvw)(xyz)\n@456 def(\"()\",\"foo bar\")\n", sink.toString());
    }

    @Test
    public void testPrintFactCommand() throws Exception {
        formatter.printFact(sink::append, Fact.meta("foo"));
        assertEquals(">foo<\n", sink.toString());

        sink.setLength(0);
        formatter.printFact(sink::append, Fact.meta("foo", "bar", "hello <", "123"));
        assertEquals(">foo bar \"hello <\" 123<\n", sink.toString());
    }

    @Test
    public void testPrintFactWithEndMarkers() throws Exception {
        formatter.setMarkDatabaseEnd(true);

        formatter.printFact(sink::append, Fact.make("abc", 123L));
        formatter.printFact(sink::append, Fact.terminator(123L));
        assertEquals("@123 abc();\n", sink.toString());

        sink.setLength(0);
        formatter.printFact(sink::append, Fact.terminator(123L));
        assertEquals("@123;\n", sink.toString());

        sink.setLength(0);
        formatter.printFact(sink::append, Fact.make("abc", 456L));
        formatter.printFact(sink::append, Fact.make("def", 456L, "uvw"));
        formatter.printFact(sink::append, Fact.make("abc", 456L));
        formatter.printFact(sink::append, Fact.make("def", 456L, "xyz"));
        formatter.printFact(sink::append, Fact.terminator(456L));
        formatter.printFact(sink::append, Fact.make("def", 456L, "()", "foo bar"));
        formatter.printFact(sink::append, Fact.terminator(456L));
        assertEquals("@456 abc()() def(uvw)(xyz);\n@456 def(\"()\",\"foo bar\");\n", sink.toString());
    }

    @Test
    public void testSerialization() throws Exception {
        formatter.printFact(sink::append, Fact.make("abc", 123L));
        formatter.printFact(sink::append, Fact.terminator(123L));
        formatter.printFact(sink::append, Fact.make("abc", 456L));
        formatter.printFact(sink::append, Fact.make("def", 456L, "foo", "bar"));
        sink.setLength(0);

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final ObjectOutputStream objectOut = new ObjectOutputStream(out);
        objectOut.writeObject(formatter);

        final ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        final ObjectInputStream objectIn = new ObjectInputStream(in);
        formatter = (MonpolyTraceFormatter) objectIn.readObject();

        formatter.printFact(sink::append, Fact.make("def", 456L, "uvw", "xyz"));
        formatter.printFact(sink::append, Fact.terminator(456L));
        assertEquals("@456 abc() def(foo,bar)(uvw,xyz)\n", sink.toString());
    }
}
