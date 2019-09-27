package ch.ethz.infsec.trace.parser;

import ch.ethz.infsec.monitor.Fact;
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
        parser.parse(sink::add, "@123 @ 456 a b () \n;");
        parser.parse(sink::add, "@ 456 abc()() def1(123");
        parser.parse(sink::add, ")\n\n(foo) def2([foo],\"(bar)\") ( a1 , \" 2b \")\r\n @789");
        parser.endOfInput(sink::add);

        assertEquals(Arrays.asList(
                Fact.terminator("123"),
                Fact.make("b", "456"),
                Fact.terminator("456"),
                Fact.make("abc", "456"),
                Fact.make("abc", "456"),
                Fact.make("def1", "456", "123"),
                Fact.make("def1", "456", "foo"),
                Fact.make("def2", "456", "[foo]", "(bar)"),
                Fact.make("def2", "456", "a1", " 2b "),
                Fact.terminator("456"),
                Fact.terminator("789")
        ), sink);

        sink.clear();
        parser.parse(sink::add, "@123 a (b,c)(d,e) @456");
        assertEquals(Arrays.asList(
                Fact.make("a", "123", "b", "c"),
                Fact.make("a", "123", "d", "e"),
                Fact.terminator("123")
        ), sink);
    }

    @Test
    public void testCommand() throws Exception {
        parser.parse(sink::add, ">foo<\n@1 p()");
        parser.endOfInput(sink::add);
        assertEquals(Arrays.asList(
                Fact.meta("foo"),
                Fact.make("p", "1"),
                Fact.terminator("1")
        ), sink);

        sink.clear();
        parser.parse(sink::add, "@1 p(q) >foo \"bar <\" 123<");
        assertEquals(Arrays.asList(
                Fact.make("p", "1", "q"),
                Fact.terminator("1"),
                Fact.meta("foo", "bar <", "123")
        ), sink);
    }

    @Test
    public void testDatabaseTerminator() throws Exception {
        parser.parse(sink::add, "@123 a();");
        assertEquals(Arrays.asList(
                Fact.make("a", "123"),
                Fact.terminator("123")
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
                Fact.make("a", "123", "b", "c"),
                Fact.make("a", "123", "d", "e"),
                Fact.terminator("123")
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
                Fact.make("a", "123", "b", "c"),
                Fact.make("a", "123", "d", "ef"),
                Fact.terminator("123"),
                Fact.terminator("456")
        ), sink);
    }
}
