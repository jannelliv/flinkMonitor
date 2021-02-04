package ch.ethz.infsec;

import ch.ethz.infsec.tools.FileEndPoint;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import ch.ethz.infsec.formula.JavaGenFormula;
import ch.ethz.infsec.formula.visitor.Init0;
import ch.ethz.infsec.monitor.*;
import ch.ethz.infsec.monitor.visitor.MformulaVisitorFlink;
import ch.ethz.infsec.policy.*;
import ch.ethz.infsec.tools.SocketEndpoint;
import ch.ethz.infsec.trace.parser.Crv2014CsvParser;
import ch.ethz.infsec.util.Assignment;
import ch.ethz.infsec.util.ParsingFunction;
import ch.ethz.infsec.trace.parser.MonpolyTraceParser;
import ch.ethz.infsec.util.PipelineEvent;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.operators.StreamFlatMap;
import org.apache.flink.streaming.api.operators.co.CoStreamFlatMap;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;
import static org.junit.Assert.assertArrayEquals;

import org.apache.flink.util.OutputTag;
import org.junit.Before;
import org.junit.Test;
import scala.collection.Iterator;
import scala.collection.Set;
import scala.io.Codec;
import scala.io.Source;
import scala.util.Either;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static ch.ethz.infsec.formula.JavaGenFormula.convert;
import static org.junit.Assert.assertEquals;

import ch.ethz.infsec.monitor.Fact;
import ch.ethz.infsec.policy.GenFormula;
import ch.ethz.infsec.policy.Policy;





public class TestHierarchy {

    public static HashMap<String, OutputTag<Fact>> hashmap = new HashMap<>();
    BufferedWriter writer;

    //testParsingWithFlink()
    private OneInputStreamOperatorTestHarness<String, Fact> testHarness;

    //testPred(), testPred2(),
    private OneInputStreamOperatorTestHarness<Fact, PipelineEvent> testHarnessPred;
    private OneInputStreamOperatorTestHarness<Fact, PipelineEvent> testHarnessPredConst;

    //testRelTrueFalse()
    private OneInputStreamOperatorTestHarness<Fact, PipelineEvent> testHarnessRel;

    //testEventually()
    private OneInputStreamOperatorTestHarness<Fact, PipelineEvent> testHarnessPredEv;
    private OneInputStreamOperatorTestHarness<PipelineEvent, PipelineEvent> testHarnessEv;

    //testOnce()
    private OneInputStreamOperatorTestHarness<Fact, PipelineEvent> testHarnessPredOnce;
    private OneInputStreamOperatorTestHarness<PipelineEvent, PipelineEvent> testHarnessOnce;
    //testAnd()
    private OneInputStreamOperatorTestHarness<Fact, PipelineEvent> testHarnessPred1;
    private OneInputStreamOperatorTestHarness<Fact, PipelineEvent> testHarnessPred2;
    private OneInputStreamOperatorTestHarness<Fact, PipelineEvent> testHarnessPred1fv;
    private OneInputStreamOperatorTestHarness<Fact, PipelineEvent> testHarnessPred2fv;
    private TwoInputStreamOperatorTestHarness<PipelineEvent, PipelineEvent, PipelineEvent> testHarnessAnd;
    private TwoInputStreamOperatorTestHarness<PipelineEvent, PipelineEvent, PipelineEvent> testHarnessAndFV;


    //testOr()
    private OneInputStreamOperatorTestHarness<Fact, PipelineEvent> testHarnessPred1Or;
    private OneInputStreamOperatorTestHarness<Fact, PipelineEvent> testHarnessPred2Or;
    private TwoInputStreamOperatorTestHarness<PipelineEvent, PipelineEvent, PipelineEvent> testHarnessOr;

    @Before
    public void setUp() throws Exception{

    }

    @Test
    public void testMain2() throws Exception{
        String logFile = System.getProperty("user.dir")+ "\\" + "test2.log"; //Attention: test2.log is empty
        ////((ONCE[0,10) A(a,b)) AND B(a,c)) AND EVENTUALLY[0,10) C(a,d)
        Either<String, GenFormula<VariableID>> a = Policy.read("B(a,c)");
        if(a.isLeft()){
            throw new ExceptionInInitializerError();
        }else{
            GenFormula<VariableID> formula = a.right().get();
            formula.atoms();
            formula.freeVariables();
            StreamExecutionEnvironment e = StreamExecutionEnvironment.getExecutionEnvironment();
            e.setMaxParallelism(1);
            e.setParallelism(1);
            DataStream<String> text = e.readTextFile(logFile);

            DataStream<Fact> facts = text.flatMap(new ParsingFunction(new MonpolyTraceParser()))
                    .name("dummy")
                    .setParallelism(1)
                    .setMaxParallelism(1);
            writer = new BufferedWriter(new FileWriter(System.getProperty("user.dir")+ "\\" + "output.txt", true));
            Set<Pred<VariableID>> atomSet = formula.atoms();
            Iterator<Pred<VariableID>> iter = atomSet.iterator();

            while(iter.hasNext()) {
                Pred<VariableID> n = iter.next();
                hashmap.put(n.relation(), new OutputTag<Fact>(n.relation()){});

            }

            hashmap.put("0Terminator", new OutputTag<Fact>("0Terminator"){});

            SingleOutputStreamOperator<Fact> mainDataStream = facts
                    .process(new ProcessFunction<Fact, Fact>() {

                        @Override
                        public void processElement(
                                Fact fact,
                                Context ctx,
                                Collector<Fact> out) throws Exception {

                            if(fact.isTerminator()){
                                for (String str: hashmap.keySet()){
                                    ctx.output(hashmap.get(str), fact);
                                }
                            }else{
                                if(hashmap.containsKey(fact.getName())) {
                                    ctx.output(hashmap.get(fact.getName()), fact);
                                }
                            }
                        }
                    });
            Mformula mformula = (convert(formula)).accept(new Init0(formula.freeVariablesInOrder()));
            DataStream<PipelineEvent> sink = mformula.accept(new MformulaVisitorFlink(hashmap, mainDataStream));

            DataStream<String> strOutput = sink.map(PipelineEvent::toString);
            final StreamingFileSink<String> sinkk = StreamingFileSink.forRowFormat(new Path(System.getProperty("user.dir")+ "\\" + "test2.log"),
                    new SimpleStringEncoder<String>("UTF-8")).build();
            strOutput.addSink(sinkk);


            strOutput.addSink(new SinkFunction<String>() {
                @Override
                public void invoke(String value) throws Exception {
                    try {
                        BufferedWriter out = new BufferedWriter(
                        new FileWriter(System.getProperty("user.dir")+ "\\" + "output.txt", true));
                        out.write(value);
                        out.close();

                    } catch (IOException e) {
                        System.out.println("An error occurred.");
                        e.printStackTrace();
                    }
                }
            });


            writer.write("done."+ "\n");
            //e.execute("dummy");
            writer.close();
        }

    }




}

