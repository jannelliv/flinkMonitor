package ch.ethz.infsec;

import ch.ethz.infsec.formula.visitor.Init0;
import ch.ethz.infsec.monitor.*;
import ch.ethz.infsec.monitor.visitor.MformulaVisitorFlink;
import ch.ethz.infsec.tools.EndPoint;
import ch.ethz.infsec.tools.FileEndPoint;
import ch.ethz.infsec.tools.ParallelSocketTextStreamFunction;
import ch.ethz.infsec.tools.SocketEndpoint;
import ch.ethz.infsec.trace.parser.Crv2014CsvParser;
import ch.ethz.infsec.util.*;

import ch.ethz.infsec.monitor.Fact;
import ch.ethz.infsec.policy.GenFormula;
import ch.ethz.infsec.policy.Policy;
import ch.ethz.infsec.policy.*;
import ch.ethz.infsec.trace.parser.MonpolyTraceParser;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import scala.Option;
import scala.collection.Iterator;
import scala.collection.Seq;
import scala.collection.Set;
import scala.io.Codec;
import scala.io.Source;
import scala.util.Either;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import static ch.ethz.infsec.formula.JavaGenFormula.convert;


public class Main {

    public static HashMap<String, OutputTag<Fact>> hashmap = new HashMap<>();

    private static final String TERMINATOR_TAG = "0Terminator";

    public static Integer checkpointInterval = 500;
    public static String checkpointUri = "file:///home/valeriaj/checkpoints";
    public static Integer restarts = 0;

    public static int numberProcessors = 1;

    public static void main(String[] args) throws Exception{

        ParameterTool p = ParameterTool.fromArgs(args);

        Option<EndPoint> inputSource = StreamMonitoring.parseEndpointArg(p.get("in"));
        Option<EndPoint> outputFile = StreamMonitoring.parseEndpointArg(p.get("out"));
        //for the above two, I had to add a maven dependency to flink-monitor
        // TODO: avoid maven dependency and implement this separately
        String formulaFile = p.get("formula");

        numberProcessors = p.getInt("processors");
        String jobName = p.get("job");

        if(inputSource.isDefined()){
            if(!(inputSource.get() instanceof SocketEndpoint)){
                throw new RuntimeException("NOT SUPPORTED!");
            }
        }else{
            throw new RuntimeException("Cannot parse the input argument (white-box main method)");
        }

        Either<String, GenFormula<VariableID>> a = Policy.read(Source.fromFile(formulaFile, Codec.fallbackSystemCodec()).mkString());
        if(a.isLeft()){
            throw new ExceptionInInitializerError();
        }else{
            GenFormula<VariableID> formula = a.right().get();
            formula.atoms();
            formula.freeVariables();
            StreamExecutionEnvironment e = StreamExecutionEnvironment.getExecutionEnvironment();
            //e.setMaxParallelism(1);
            //e.setParallelism(1);

            //e.setStateBackend(new RocksDBStateBackend(checkpointUri));
            //e.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE);
            //RestartStrategies.RestartStrategyConfiguration restartStrategy = RestartStrategies.noRestart();
            //e.setRestartStrategy(restartStrategy);


            DataStream<String> text = e.addSource(new ParallelSocketTextStreamFunction(((SocketEndpoint) inputSource.get()).socket_addr(), ((SocketEndpoint) inputSource.get()).port()))
                    .setParallelism(1)
                    .setMaxParallelism(1)
                    .name("Socket source")
                    .uid("socket-source");
            //DataStream<String> text = e.socketTextStream(((SocketEndpoint) inputSource.get()).socket_addr(), ((SocketEndpoint) inputSource.get()).port());

            DataStream<Fact> facts = text.flatMap(new ParsingFunction(new MonpolyTraceParser()))
                                         .setParallelism(1)
                                         .setMaxParallelism(1)
                                        .name("parser")
                                        .uid("parser");
            //BufferedWriter writer = new BufferedWriter(new FileWriter(((FileEndPoint)outputFile.get()).file_path(), true));
            Set<Pred<VariableID>> atomSet = formula.atoms();
            Iterator<Pred<VariableID>> iter = atomSet.iterator();

            while(iter.hasNext()) {
                Pred<VariableID> n = iter.next();
                hashmap.put(n.relation(), new OutputTag<Fact>(n.relation()){});

            }

            hashmap.put(TERMINATOR_TAG, new OutputTag<Fact>(TERMINATOR_TAG){});

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
            strOutput.addSink(StreamingFileSink.forRowFormat(new Path(((FileEndPoint)outputFile.get()).file_path()),new SimpleStringEncoder<String>("UTF-8")).build());


            e.execute(jobName);
            //writer.write("done."+ "\n");
            //writer.close();
            //Currently, PipelineEvent is printed as "@ <timestamp> : <timepoint>" when it is a terminator and as
            // "@ <timestamp> : <timepoint> (<val>, <val>, ..., <val>)" when it's not.

            //strOutput.writeAsText(((FileEndPoint)outputFile.get()).file_path(), org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE);

            
        }

    }


}
