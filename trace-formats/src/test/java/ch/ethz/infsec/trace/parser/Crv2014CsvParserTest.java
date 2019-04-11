package ch.ethz.infsec.trace.parser;

import ch.ethz.infsec.monitor.Fact;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class Crv2014CsvParserTest {
    Crv2014CsvParser parser;

    @Before
    public void setUp() {
        parser = new Crv2014CsvParser("event");
    }

    private void assertParseException() {
        try {
            parser.nextFact();
            fail("expected a ParseException");
        } catch (ParseException ignored) { }
    }

    @Test
    public void testSuccessfulParse() throws Exception {
        parser.setInput("");
        assertNull(parser.nextFact());

        parser.setInput(" ");
        assertNull(parser.nextFact());

        parser.setInput("  \t \n");
        assertNull(parser.nextFact());

        parser.setInput("\r\n");
        assertNull(parser.nextFact());

        parser.setInput("a,tp=1,ts=11");
        assertEquals(new Fact("a", "11"), parser.nextFact());
        assertNull(parser.nextFact());

        parser.setInput("ab, tp = 1, ts = 11, x = y\n");
        assertEquals(new Fact("ab", "11", "y"), parser.nextFact());
        assertNull(parser.nextFact());

        parser.setInput(" cde , tp = 2 , ts = 11 , x = y\n");
        assertEquals(new Fact("event", "11"), parser.nextFact());
        assertEquals(new Fact("cde", "11", "y"), parser.nextFact());
        assertNull(parser.nextFact());

        parser.setInput("a, tp=2, ts=12, x=y, 123 = 4FOO56   ");
        assertEquals(new Fact("event", "11"), parser.nextFact());
        assertEquals(new Fact("a", "12", "y", "4FOO56"), parser.nextFact());
        assertNull(parser.nextFact());

        parser.terminateEvent();
        assertEquals(new Fact("event", "12"), parser.nextFact());
        assertNull(parser.nextFact());
    }

    @Test
    public void testParseFailure() throws Exception {
        parser.setInput("abc");
        assertParseException();

        parser.setInput("abc, foo=bar");
        assertParseException();

        parser.setInput("abc, ts=1, tp=1");
        assertParseException();

        parser.setInput("abc, tp=1, ts=1, foo, bar");
        assertParseException();

        parser.setInput("abc, tp=1, ts=1, x=y=z");
        assertParseException();

        parser.setInput("abc, tp=1, ts=1, x=y");
        assertEquals(new Fact("abc", "1", "y"), parser.nextFact());
        assertNull(parser.nextFact());
    }
}