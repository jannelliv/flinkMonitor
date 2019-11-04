package ch.ethz.infsec.monitor;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.*;

public class FactTest {
    @Test
    public void testName() {
        {
            final Fact fact = Fact.terminator(123L);
            assertNull(fact.getName());
        }
        {
            final Fact fact = Fact.make("foo", 123L);
            assertEquals("foo", fact.getName());
        }
        {
            final Fact fact = new Fact("foo", 123L, Collections.singletonList("abc"));
            assertEquals("foo", fact.getName());
            fact.setName("bar");
            assertEquals("bar", fact.getName());
        }
        {
            final Fact fact = Fact.meta("bar", "xyz");
            assertEquals("bar", fact.getName());
        }
    }

    @Test
    public void testTimestamp() {
        {
            final Fact fact = Fact.terminator(123L);
            assertEquals(Long.valueOf(123), fact.getTimestamp());
        }
        {
            final Fact fact = Fact.make("foo", 123L);
            assertEquals(Long.valueOf(123), fact.getTimestamp());
        }
        {
            final Fact fact = new Fact("foo", 123L, Collections.singletonList("abc"));
            assertEquals(Long.valueOf(123), fact.getTimestamp());
            fact.setTimestamp(456L);
            assertEquals(Long.valueOf(456), fact.getTimestamp());
        }
        {
            final Fact fact = Fact.meta("bar", "xyz");
            assertNull(fact.getTimestamp());
        }
    }

    @Test
    public void testArguments() {
        {
            final Fact fact = Fact.terminator(123L);
            assertEquals(Collections.emptyList(), fact.getArguments());
            assertEquals(0, fact.getArity());
        }
        {
            final Fact fact = Fact.make("foo", 123L);
            assertEquals(Collections.emptyList(), fact.getArguments());
            assertEquals(0, fact.getArity());
        }
        {
            final Fact fact = Fact.make("foo", 123L, "abc");
            assertEquals(Collections.singletonList("abc"), fact.getArguments());
            assertEquals("abc", fact.getArgument(0));
            assertEquals(1, fact.getArity());
        }
        {
            final Fact fact = Fact.meta("foo", "abc");
            assertEquals(Collections.singletonList("abc"), fact.getArguments());
            assertEquals("abc", fact.getArgument(0));
            assertEquals(1, fact.getArity());
        }
        {
            final Fact fact = new Fact("foo", 123L, Arrays.asList("abc", "d"));
            assertEquals(Arrays.asList("abc", "d"), fact.getArguments());
            assertEquals("abc", fact.getArgument(0));
            assertEquals("d", fact.getArgument(1));
            assertEquals(2, fact.getArity());

            fact.setArguments(Collections.singletonList("xyz"));
            assertEquals(Collections.singletonList("xyz"), fact.getArguments());
            assertEquals("xyz", fact.getArgument(0));
            assertEquals(1, fact.getArity());
        }
    }

    @Test
    public void testIsTerminator() {
        {
            final Fact fact = Fact.make("foo", 123L);
            assertFalse(fact.isTerminator());
        }
        {
            final Fact fact = Fact.terminator(123L);
            assertTrue(fact.isTerminator());
        }
    }

    @Test
    public void testIsMeta() {
        {
            final Fact fact = Fact.terminator(123L);
            assertFalse(fact.isMeta());
        }
        {
            final Fact fact = Fact.meta("foo", "xyz");
            assertTrue(fact.isMeta());
        }

    }

    @Test
    public void testEquals() {
        final Fact fact0 = Fact.make("f", 123L);
        final Fact fact1 = Fact.make("f1", 123L);
        final Fact fact2 = Fact.make("f", 456L);
        final Fact fact3 = new Fact("f", 123L, Collections.singletonList("abc"));
        final Fact fact4 = Fact.terminator(123L);
        final Fact fact5 = Fact.meta("f", "123");

        assertEquals(fact0, fact0);
        assertEquals(fact5, fact5);
        assertNotEquals(fact0, fact1);
        assertNotEquals(fact0, fact2);
        assertNotEquals(fact0, fact3);
        assertNotEquals(fact0, fact4);
        assertNotEquals(fact5, fact0);

        fact1.setName("f");
        assertEquals(fact0, fact1);
    }

    @Test
    public void testHashCode() {
        final Fact fact0 = Fact.make("f", 123L);
        final Fact fact1 = new Fact("f", 123L, Collections.singletonList("abc"));

        fact0.setArguments(Collections.singletonList("abc"));
        assertNotSame(fact0, fact1);
        assertEquals(fact0.hashCode(), fact1.hashCode());
    }
}
