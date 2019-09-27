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
import java.util.Collections;

import static org.junit.Assert.*;

public class Crv2014CsvParserTest {
    private ArrayList<Fact> sink;
    private Crv2014CsvParser parser;

    @Before
    public void setUp() {
        sink = new ArrayList<>();
        parser = new Crv2014CsvParser();
    }

    @Test
    public void testSuccessfulParse() throws Exception {
        parser.parseLine(sink::add, "");
        assertTrue(sink.isEmpty());

        parser.parseLine(sink::add, "\n");
        assertTrue(sink.isEmpty());

        parser.parseLine(sink::add, "  \t \r\n");
        assertTrue(sink.isEmpty());

        parser.parseLine(sink::add, "a,tp=1,ts=11");
        assertEquals(Collections.singletonList(Fact.make("a", "11")), sink);

        sink.clear();
        parser.parseLine(sink::add, "ab, tp = 1, ts = 11, x = y");
        parser.parseLine(sink::add, ";;\n");
        assertEquals(Arrays.asList(
                Fact.make("ab", "11", "y"),
                Fact.terminator("11")
        ), sink);

        sink.clear();
        parser.parseLine(sink::add, " cde , tp = 2 , ts = 11 , x =  y\r\n");
        assertEquals(Collections.singletonList(Fact.make("cde", "11", "y")), sink);

        sink.clear();
        parser.parseLine(sink::add, "a, tp=2, ts=12, x=y, 123 = 4Foo56   ");
        assertEquals(Arrays.asList(
                Fact.terminator("11"),
                Fact.make("a", "12", "y", "4Foo56")
        ), sink);

        sink.clear();
        parser.endOfInput(sink::add);
        assertEquals(Collections.singletonList(Fact.terminator("12")), sink);

        sink.clear();
        parser.endOfInput(sink::add);
        assertTrue(sink.isEmpty());

        parser.parseLine(sink::add, "xyz, tp = 1, ts = 2");
        assertEquals(Collections.singletonList(Fact.make("xyz", "2")), sink);
    }

    @Test
    public void testCommand() throws Exception {
        parser.parseLine(sink::add, "> foo<");
        parser.parseLine(sink::add, "a,tp=1,ts=1,x=y");
        parser.endOfInput(sink::add);
        assertEquals(Arrays.asList(
                Fact.meta("foo"),
                Fact.make("a", "1", "y"),
                Fact.terminator("1")
        ), sink);

        sink.clear();
        parser.parseLine(sink::add, "a,tp=1,ts=1,x=y");
        parser.parseLine(sink::add, ">foo \"hello <\" 123<");
        assertEquals(Arrays.asList(
                Fact.make("a", "1", "y"),
                Fact.terminator("1"),
                Fact.meta("foo", "hello <", "123")
        ), sink);
    }

    private void assertParseFailure(String line) {
        try {
            parser.parseLine(sink::add, line);
            fail("expected a ParseException");
        } catch (ParseException ignored) {
        }
    }

    @Test
    public void testParseFailure() throws Exception {
        assertParseFailure("abc");
        assertParseFailure("abc, foo=bar\n");
        assertParseFailure("abc, tp=1, ts=1, foo, bar");

        parser.parseLine(sink::add, "abc, tp=1, ts=1, x=y");
        assertEquals(Collections.singletonList(Fact.make("abc", "1", "y")), sink);
    }

    @Test
    public void testSerialization() throws Exception {
        parser.parseLine(sink::add, "abc, tp=1, ts=1, uvw=xyz");
        sink.clear();

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final ObjectOutputStream objectOut = new ObjectOutputStream(out);
        objectOut.writeObject(parser);

        final ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        final ObjectInputStream objectIn = new ObjectInputStream(in);
        parser = (Crv2014CsvParser) objectIn.readObject();

        parser.parseLine(sink::add, "def, tp=2, ts=2, foo=bar");
        assertEquals(Arrays.asList(
                Fact.terminator("1"),
                Fact.make("def", "2", "bar")
        ), sink);
    }
}
