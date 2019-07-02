package ch.ethz.infsec.trace.formatter;

import ch.ethz.infsec.monitor.Fact;
import ch.ethz.infsec.trace.Trace;
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
    public void testPrintFact() {
        formatter.printFact(sink, new Fact("abc", "123"));
        assertEquals("abc, tp=0, ts=123\n", sink.toString());

        sink.setLength(0);
        formatter.printFact(sink, new Fact(Trace.EVENT_FACT, "123"));
        assertEquals("", sink.toString());

        sink.setLength(0);
        formatter.printFact(sink, new Fact(Trace.EVENT_FACT, "123"));
        assertEquals("", sink.toString());

        sink.setLength(0);
        formatter.printFact(sink, new Fact("abc", "456"));
        assertEquals("abc, tp=2, ts=456\n", sink.toString());

        sink.setLength(0);
        formatter.printFact(sink, new Fact("def", "456", "uvw", "xyz"));
        assertEquals("def, tp=2, ts=456, x0=uvw, x1=xyz\n", sink.toString());

        sink.setLength(0);
        formatter.printFact(sink, new Fact(Trace.EVENT_FACT, "456"));
        assertEquals("", sink.toString());
    }

    @Test
    public void testPrintFactWithEndMarkers() {
        formatter.setMarkDatabaseEnd(true);

        formatter.printFact(sink, new Fact("abc", "123"));
        assertEquals("abc, tp=0, ts=123\n", sink.toString());

        sink.setLength(0);
        formatter.printFact(sink, new Fact(Trace.EVENT_FACT, "123"));
        assertEquals(";;\n", sink.toString());

        sink.setLength(0);
        formatter.printFact(sink, new Fact(Trace.EVENT_FACT, "123"));
        assertEquals(";;\n", sink.toString());

        sink.setLength(0);
        formatter.printFact(sink, new Fact("abc", "456"));
        assertEquals("abc, tp=2, ts=456\n", sink.toString());

        sink.setLength(0);
        formatter.printFact(sink, new Fact("def", "456", "uvw", "xyz"));
        assertEquals("def, tp=2, ts=456, x0=uvw, x1=xyz\n", sink.toString());

        sink.setLength(0);
        formatter.printFact(sink, new Fact(Trace.EVENT_FACT, "456"));
        assertEquals(";;\n", sink.toString());
    }

    @Test
    public void testSerialization() throws Exception {
        formatter.printFact(sink, new Fact("abc", "123"));
        formatter.printFact(sink, new Fact(Trace.EVENT_FACT, "123"));
        formatter.printFact(sink, new Fact(Trace.EVENT_FACT, "123"));
        sink.setLength(0);

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final ObjectOutputStream objectOut = new ObjectOutputStream(out);
        objectOut.writeObject(formatter);

        final ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        final ObjectInputStream objectIn = new ObjectInputStream(in);
        formatter = (Crv2014CsvFormatter) objectIn.readObject();

        formatter.printFact(sink, new Fact("def", "456", "uvw", "xyz"));
        formatter.printFact(sink, new Fact(Trace.EVENT_FACT, "456"));
        assertEquals("def, tp=2, ts=456, x0=uvw, x1=xyz\n", sink.toString());
    }
}
