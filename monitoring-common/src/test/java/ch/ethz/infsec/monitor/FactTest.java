package ch.ethz.infsec.monitor;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.*;

public class FactTest {
    @Test
    public void testName() {
        {
            final Fact fact = new Fact();
            assertEquals("", fact.getName());
        }
        {
            final Fact fact = new Fact("foo", "123");
            assertEquals("foo", fact.getName());
        }
        {
            final Fact fact = new Fact("foo", "123", Arrays.asList("abc"));
            assertEquals("foo", fact.getName());
            fact.setName("bar");
            assertEquals("bar", fact.getName());
        }
    }

    @Test
    public void testTimestamp() {
        {
            final Fact fact = new Fact();
            assertEquals("", fact.getTimestamp());
        }
        {
            final Fact fact = new Fact("foo", "123");
            assertEquals("123", fact.getTimestamp());
        }
        {
            final Fact fact = new Fact("foo", "123", Arrays.asList("abc"));
            assertEquals("123", fact.getTimestamp());
            fact.setTimestamp("456");
            assertEquals("456", fact.getTimestamp());
        }
    }

    @Test
    public void testArguments() {
        {
            final Fact fact = new Fact();
            assertEquals(Collections.emptyList(), fact.getArguments());
            assertEquals(0, fact.getArity());
        }
        {
            final Fact fact = new Fact("foo", "123");
            assertEquals(Collections.emptyList(), fact.getArguments());
            assertEquals(0, fact.getArity());
        }
        {
            final Fact fact = new Fact("foo", "123", "abc");
            assertEquals(Arrays.asList("abc"), fact.getArguments());
            assertEquals("abc", fact.getArgument(0));
            assertEquals(1, fact.getArity());
        }
        {
            final Fact fact = new Fact("foo", "123", Arrays.asList("abc", "d"));
            assertEquals(Arrays.asList("abc", "d"), fact.getArguments());
            assertEquals("abc", fact.getArgument(0));
            assertEquals("d", fact.getArgument(1));
            assertEquals(2, fact.getArity());

            fact.setArguments(Arrays.asList("xyz"));
            assertEquals(Arrays.asList("xyz"), fact.getArguments());
            assertEquals("xyz", fact.getArgument(0));
            assertEquals(1, fact.getArity());
        }
    }

    @Test
    public void testEquals() {
        final Fact fact0 = new Fact("f", "123");
        final Fact fact1 = new Fact("f1", "123");
        final Fact fact2 = new Fact("f", "456");
        final Fact fact3 = new Fact("f", "123", Arrays.asList("abc"));

        assertEquals(fact0, fact0);
        assertNotEquals(fact0, fact1);
        assertNotEquals(fact0, fact2);
        assertNotEquals(fact0, fact3);

        fact1.setName("f");
        assertEquals(fact0, fact1);
    }

    @Test
    public void testHashCode() {
        final Fact fact0 = new Fact("f", "123");
        final Fact fact1 = new Fact("f", "123", Arrays.asList("abc"));

        fact0.setArguments(Arrays.asList("abc"));
        assertNotSame(fact0, fact1);
        assertEquals(fact0.hashCode(), fact1.hashCode());
    }
}