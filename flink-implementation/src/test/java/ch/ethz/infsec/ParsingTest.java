package ch.ethz.infsec;

import ch.ethz.infsec.formula.visitor.Init0;
import ch.ethz.infsec.monitor.Fact;
import ch.ethz.infsec.monitor.MPred;
import ch.ethz.infsec.policy.GenFormula;
import ch.ethz.infsec.policy.Policy;
import ch.ethz.infsec.policy.Pred;
import ch.ethz.infsec.policy.VariableID;
import ch.ethz.infsec.util.ParsingFunction;
import ch.ethz.infsec.trace.parser.MonpolyTraceParser;
import ch.ethz.infsec.util.PipelineEvent;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;
import org.junit.Before;
import org.junit.Test;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.util.Either;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static ch.ethz.infsec.formula.JavaGenFormula.convert;
import static org.junit.Assert.assertEquals;


public class ParsingTest {

    private OneInputStreamOperatorTestHarness<String, Fact> testHarness;
    private FlatMapFunction<String, Fact> statefulFlatMapFunction;

    private OneInputStreamOperatorTestHarness<Fact, PipelineEvent> testHarnessPred;
    private FlatMapFunction<Fact, PipelineEvent> statefulFlatMapFunctionPred;

    @Before
    public void setUp() throws Exception{

        statefulFlatMapFunction = new ParsingFunction(new MonpolyTraceParser());
        testHarness = new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(statefulFlatMapFunction));
        testHarness.open();

        Either<String, GenFormula<VariableID>> a = Policy.read("publish(r)");
        GenFormula<VariableID> formula = a.right().get();

        statefulFlatMapFunctionPred = (MPred) (convert(formula)).accept(new Init0(formula.freeVariablesInOrder()));
        testHarnessPred = new OneInputStreamOperatorTestHarness<>(new StreamFlatMap<>(statefulFlatMapFunctionPred));
        testHarnessPred.open();

    }

    @Test
    public void testPred() throws Exception{

        testHarnessPred.processElement(Fact.make("publish", 1L, "160"), 1L);
        //does it make sense that the events above all have the same timestamp
        List<PipelineEvent> pe = testHarnessPred.getOutput().stream().map(x -> (PipelineEvent)((StreamRecord) x).getValue()).collect(Collectors.toList());
        assert(pe.size()==1);
        assert(pe.get(0).get().size() == 1);
        assert(pe.get(0).get().get(0).equals("160"));
        assert(pe.get(0).getTimestamp() == 1L);


    }

    private Collector<PipelineEvent> mock(Class<Collector> collectorClass) {
        return new Collector<PipelineEvent>() {
            @Override
            public void collect(PipelineEvent record) {
                this.collect(record);
            }

            @Override
            public void close() {
                this.close();
            }
        };
    }

    @Test
    public void testParsingWithFlink() throws Exception{
        testHarness.processElement("@0 p() q() @1 r(1,3) s(\"foo\") @2", 1L);
        //the test harness operator simulates an operator
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
