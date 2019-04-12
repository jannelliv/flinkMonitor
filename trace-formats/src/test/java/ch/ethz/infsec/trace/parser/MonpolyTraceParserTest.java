package ch.ethz.infsec.trace.parser;

import ch.ethz.infsec.monitor.Fact;
import ch.ethz.infsec.trace.Trace;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.*;

public class MonpolyTraceParserTest {
    private ArrayList<Fact> sink;
    private MonpolyTraceParser parser;

    @Before
    public void setUp() {
        sink = new ArrayList<>();
        parser = new MonpolyTraceParser();
    }

    @Test
    public void testSuccessfulParse() throws Exception {
        parser.parse(sink::add, "@123 @ 456 a b () \n");
        parser.parse(sink::add, "@ 456 abc()() def1(123");
        parser.parse(sink::add, ")\n\n(foo) def2([foo],\"(bar)\") ( a1 , \" 2b \")\r\n @789");
        parser.endOfInput(sink::add);

        assertEquals(Arrays.asList(
                new Fact(Trace.EVENT_FACT, "123"),
                new Fact("b", "456"),
                new Fact(Trace.EVENT_FACT, "456"),
                new Fact("abc", "456"),
                new Fact("abc", "456"),
                new Fact("def1", "456", "123"),
                new Fact("def1", "456", "foo"),
                new Fact("def2", "456", "[foo]", "(bar)"),
                new Fact("def2", "456", "a1", " 2b "),
                new Fact(Trace.EVENT_FACT, "456"),
                new Fact(Trace.EVENT_FACT, "789")
        ), sink);

        sink.clear();
        parser.parse(sink::add, "@123 a (b,c)(d,e) @456");
        assertEquals(Arrays.asList(
                new Fact("a", "123", "b", "c"),
                new Fact("a", "123", "d", "e"),
                new Fact(Trace.EVENT_FACT, "123")
        ), sink);
    }

    private void assertParseFailure(String input) {
        try {
            parser.parse(sink::add, input);
            parser.endOfInput(sink::add);
            fail("expected a ParseException");
        } catch (ParseException ignored) {
        }
        assertTrue(sink.isEmpty());
    }

    @Test
    public void testParseFailure() throws Exception {
        assertParseFailure("foo");
        assertParseFailure("@ @");
        assertParseFailure("@123 foo(,)");
        assertParseFailure("@123 foo(bar)(");

        parser.parse(sink::add, "@123 a (b,c)(d,e) @456");
        assertEquals(Arrays.asList(
                new Fact("a", "123", "b", "c"),
                new Fact("a", "123", "d", "e"),
                new Fact(Trace.EVENT_FACT, "123")
        ), sink);
    }

    @Test
    public void testSerialization() throws Exception {
        parser.parse(sink::add, "@123 a (b,c)(d,e");
        sink.clear();

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final ObjectOutputStream objectOut = new ObjectOutputStream(out);
        objectOut.writeObject(parser);

        final ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        final ObjectInputStream objectIn = new ObjectInputStream(in);
        parser = (MonpolyTraceParser) objectIn.readObject();

        parser.parse(sink::add, "f) @456");
        parser.endOfInput(sink::add);
        assertEquals(Arrays.asList(
                new Fact("a", "123", "b", "c"),
                new Fact("a", "123", "d", "ef"),
                new Fact(Trace.EVENT_FACT, "123"),
                new Fact(Trace.EVENT_FACT, "456")
        ), sink);
    }
}