package ch.ethz.infsec;

import ch.ethz.infsec.monitor.Fact;
import ch.ethz.infsec.src.ParsingFunction;
import ch.ethz.infsec.trace.parser.MonpolyTraceParser;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.types.Record;
import org.junit.Before;
import org.junit.Test;




import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;


public class ParsingTest {

    private OneInputStreamOperatorTestHarness<String, Fact> testHarness;
    private FlatMapFunction<String, Fact> statefulFlatMapFunction;

    @Before
    public void setUp() throws Exception{
        statefulFlatMapFunction = new ParsingFunction(new MonpolyTraceParser());
        testHarness = new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(statefulFlatMapFunction));
        testHarness.open();

    }

    @Test
    public void testParsingWithFlink() throws Exception{

        testHarness.processElement("@0 p() q() @1 r(1,3) s(\"foo\") @2", 1L);

        //test correct facts
        List<Fact> facts = testHarness.getOutput().stream().map(x -> (Fact)((StreamRecord) x).getValue()).collect(Collectors.toList());
        assertEquals(Arrays.asList(
                Fact.make("p", 0L),
                Fact.make("q", 0L),
                Fact.terminator(0L),
                Fact.make("r", 1L, "1","3"),
                Fact.make("s", 1L, "foo"),
                Fact.terminator(1L)
        ), facts);

        //test timepoints
        facts.forEach(f -> {assertEquals(f.getTimepoint(),f.getTimestamp());});

    }

}
