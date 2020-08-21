package ch.ethz.infsec.trace.formatter;

import ch.ethz.infsec.monitor.Fact;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.junit.Assert.assertEquals;

public class Crv2014CsvFormatterTest {
    private StringBuilder sink;
    private Crv2014CsvFormatter formatter;

    @Before
    public void setUp() {
        sink = new StringBuilder();
        formatter = new Crv2014CsvFormatter();
    }

    @Test
    public void testPrintFact() throws Exception {
        formatter.printFact(sink::append, Fact.make("abc", 123L));
        assertEquals("abc, tp=0, ts=123\n", sink.toString());

        sink.setLength(0);
        formatter.printFact(sink::append, Fact.terminator(123L));
        assertEquals("", sink.toString());

        sink.setLength(0);
        formatter.printFact(sink::append, Fact.terminator(123L));
        assertEquals("", sink.toString());

        sink.setLength(0);
        formatter.printFact(sink::append, Fact.make("abc", 456L));
        assertEquals("abc, tp=2, ts=456\n", sink.toString());

        sink.setLength(0);
        formatter.printFact(sink::append, Fact.make("def", 456L, "uvw", "xyz"));
        assertEquals("def, tp=2, ts=456, x0=uvw, x1=xyz\n", sink.toString());

        sink.setLength(0);
        formatter.printFact(sink::append, Fact.terminator(456L));
        assertEquals("", sink.toString());
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
        assertEquals("abc, tp=0, ts=123\n", sink.toString());

        sink.setLength(0);
        formatter.printFact(sink::append, Fact.terminator(123L));
        assertEquals(";;\n", sink.toString());

        sink.setLength(0);
        formatter.printFact(sink::append, Fact.terminator(123L));
        assertEquals(";;\n", sink.toString());

        sink.setLength(0);
        formatter.printFact(sink::append, Fact.make("abc", 456L));
        assertEquals("abc, tp=2, ts=456\n", sink.toString());

        sink.setLength(0);
        formatter.printFact(sink::append, Fact.make("def", 456L, "uvw", "xyz"));
        assertEquals("def, tp=2, ts=456, x0=uvw, x1=xyz\n", sink.toString());

        sink.setLength(0);
        formatter.printFact(sink::append, Fact.terminator(456L));
        assertEquals(";;\n", sink.toString());
    }

    @Test
    public void testSerialization() throws Exception {
        formatter.printFact(sink::append, Fact.make("abc", 123L));
        formatter.printFact(sink::append, Fact.terminator(123L));
        formatter.printFact(sink::append, Fact.terminator(123L));
        sink.setLength(0);

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final ObjectOutputStream objectOut = new ObjectOutputStream(out);
        objectOut.writeObject(formatter);

        final ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        final ObjectInputStream objectIn = new ObjectInputStream(in);
        formatter = (Crv2014CsvFormatter) objectIn.readObject();

        formatter.printFact(sink::append, Fact.make("def", 456L, "uvw", "xyz"));
        formatter.printFact(sink::append, Fact.terminator(456L));
        assertEquals("def, tp=2, ts=456, x0=uvw, x1=xyz\n", sink.toString());
    }
}
