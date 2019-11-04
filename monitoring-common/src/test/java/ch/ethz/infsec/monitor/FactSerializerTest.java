package ch.ethz.infsec.monitor;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

public class FactSerializerTest {
    private Kryo kryo;

    @Before
    public void setUp() {
        kryo = new Kryo();
        kryo.register(Fact.class, new FactSerializer());
    }

    @Test
    public void testRoundTrip() {
        final Fact writtenFact = Fact.make("hello world", 12345678L, "String", 42L, 1.234);

        final Output output = new Output(1024);
        kryo.writeObject(output, writtenFact);

        final Input input = new Input(output.getBuffer());
        final Fact readFact = kryo.readObject(input, Fact.class);

        assertNotSame(writtenFact, readFact);
        assertEquals(writtenFact, readFact);
    }
}
