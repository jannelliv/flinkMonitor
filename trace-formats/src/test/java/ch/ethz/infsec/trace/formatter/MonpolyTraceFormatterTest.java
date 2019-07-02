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

public class MonpolyTraceFormatterTest {
    private StringBuilder sink;
    private MonpolyTraceFormatter formatter;

    @Before
    public void setUp() {
        sink = new StringBuilder();
        formatter = new MonpolyTraceFormatter();
    }

    @Test
    public void testPrintFact() {
        formatter.printFact(sink, new Fact("abc", "123"));
        formatter.printFact(sink, new Fact(Trace.EVENT_FACT, "123"));
        assertEquals("@123 abc()\n", sink.toString());

        sink.setLength(0);
        formatter.printFact(sink, new Fact(Trace.EVENT_FACT, "123"));
        assertEquals("@123\n", sink.toString());

        sink.setLength(0);
        formatter.printFact(sink, new Fact("abc", "456"));
        formatter.printFact(sink, new Fact("def", "456", "uvw"));
        formatter.printFact(sink, new Fact("abc", "456"));
        formatter.printFact(sink, new Fact("def", "456", "xyz"));
        formatter.printFact(sink, new Fact(Trace.EVENT_FACT, "456"));
        formatter.printFact(sink, new Fact("def", "456", "()", "foo bar"));
        formatter.printFact(sink, new Fact(Trace.EVENT_FACT, "456"));
        assertEquals("@456 abc()() def(uvw)(xyz)\n@456 def(\"()\",\"foo bar\")\n", sink.toString());
    }

    @Test
    public void testPrintFactWithEndMarkers() {
        formatter.setMarkDatabaseEnd(true);

        formatter.printFact(sink, new Fact("abc", "123"));
        formatter.printFact(sink, new Fact(Trace.EVENT_FACT, "123"));
        assertEquals("@123 abc();\n", sink.toString());

        sink.setLength(0);
        formatter.printFact(sink, new Fact(Trace.EVENT_FACT, "123"));
        assertEquals("@123;\n", sink.toString());

        sink.setLength(0);
        formatter.printFact(sink, new Fact("abc", "456"));
        formatter.printFact(sink, new Fact("def", "456", "uvw"));
        formatter.printFact(sink, new Fact("abc", "456"));
        formatter.printFact(sink, new Fact("def", "456", "xyz"));
        formatter.printFact(sink, new Fact(Trace.EVENT_FACT, "456"));
        formatter.printFact(sink, new Fact("def", "456", "()", "foo bar"));
        formatter.printFact(sink, new Fact(Trace.EVENT_FACT, "456"));
        assertEquals("@456 abc()() def(uvw)(xyz);\n@456 def(\"()\",\"foo bar\");\n", sink.toString());
    }

    @Test
    public void testSerialization() throws Exception {
        formatter.printFact(sink, new Fact("abc", "123"));
        formatter.printFact(sink, new Fact(Trace.EVENT_FACT, "123"));
        formatter.printFact(sink, new Fact("abc", "456"));
        formatter.printFact(sink, new Fact("def", "456", "foo", "bar"));
        sink.setLength(0);

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final ObjectOutputStream objectOut = new ObjectOutputStream(out);
        objectOut.writeObject(formatter);

        final ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        final ObjectInputStream objectIn = new ObjectInputStream(in);
        formatter = (MonpolyTraceFormatter) objectIn.readObject();

        formatter.printFact(sink, new Fact("def", "456", "uvw", "xyz"));
        formatter.printFact(sink, new Fact(Trace.EVENT_FACT, "456"));
        assertEquals("@456 abc() def(foo,bar)(uvw,xyz)\n", sink.toString());
    }
}
