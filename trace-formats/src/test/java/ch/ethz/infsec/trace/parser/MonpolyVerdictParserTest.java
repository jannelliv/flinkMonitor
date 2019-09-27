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

public class MonpolyVerdictParserTest {
    private ArrayList<Fact> sink;
    private MonpolyVerdictParser parser;

    @Before
    public void setUp() {
        sink = new ArrayList<>();
        parser = new MonpolyVerdictParser();
    }

    @Test
    public void testSuccessfulParse() throws Exception {
        parser.parseLine(sink::add, "@123. (time point 1001): true\n");
        parser.parseLine(sink::add, "@456. (time point 1002): (xyz)");
        parser.parseLine(sink::add, "@789. (time point 1003): (foo,\"bar\") (a,1b)");
        parser.endOfInput(sink::add);

        assertEquals(Arrays.asList(
                Fact.make("", "123", "1001"),
                Fact.terminator("123"),
                Fact.make("", "456", "1002", "xyz"),
                Fact.terminator("456"),
                Fact.make("", "789", "1003", "foo", "bar"),
                Fact.make("", "789", "1003", "a", "1b"),
                Fact.terminator("789")
        ), sink);

        sink.clear();
        parser.parseLine(sink::add, "@123. (time point 1004): (9)");
        assertEquals(Arrays.asList(
                Fact.make("", "123", "1004", "9"),
                Fact.terminator("123")
        ), sink);
    }

    private void assertParseFailure(String input) {
        try {
            parser.parseLine(sink::add, input);
            parser.endOfInput(sink::add);
            fail("expected a ParseException");
        } catch (ParseException ignored) {
        }
        assertTrue(sink.isEmpty());
    }

    @Test
    public void testParseFailure() throws Exception {
        assertParseFailure("foo");
        assertParseFailure("@123 foo (bar)");
        assertParseFailure("@123. (time point 1001): xyz");

        parser.parseLine(sink::add, "@123. (time point 1001): (xyz)\n");
        assertEquals(Arrays.asList(
                Fact.make("", "123", "1001", "xyz"),
                Fact.terminator("123")
        ), sink);
    }

    @Test
    public void testSerialization() throws Exception {
        parser.parseLine(sink::add, "@123. (time point 1001): (xyz)\n");
        sink.clear();

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final ObjectOutputStream objectOut = new ObjectOutputStream(out);
        objectOut.writeObject(parser);

        final ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        final ObjectInputStream objectIn = new ObjectInputStream(in);
        parser = (MonpolyVerdictParser) objectIn.readObject();

        parser.parseLine(sink::add, "@123. (time point 1002): (a)\n");
        parser.endOfInput(sink::add);
        assertEquals(Arrays.asList(
                Fact.make("", "123", "1002", "a"),
                Fact.terminator("123")
        ), sink);
    }
}
