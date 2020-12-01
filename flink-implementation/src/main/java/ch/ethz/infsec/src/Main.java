package ch.ethz.infsec.src;

import ch.ethz.infsec.src.formula.*;
import ch.ethz.infsec.src.formula.visitor.Init0;
import ch.ethz.infsec.src.monitor.*;
import ch.ethz.infsec.src.monitor.visitor.MformulaVisitorFlink;
import ch.ethz.infsec.src.util.*;

import ch.ethz.infsec.monitor.Fact;
import ch.ethz.infsec.policy.GenFormula;
import ch.ethz.infsec.policy.Policy;
import ch.ethz.infsec.policy.*;
import ch.ethz.infsec.trace.parser.MonpolyTraceParser;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import scala.collection.Iterator;
import scala.collection.Set;
import scala.io.Codec;
import scala.io.Source;
import scala.util.Either;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.*;
import static ch.ethz.infsec.src.formula.JavaGenFormula.convert;


public class Main {

    private static final String TERMINATOR_TAG = "0Terminator";

    public static void main(String[] args) throws Exception{


        //TODO: Validate the input arguments
        String formulaFile = System.getProperty("user.dir")+ "/" + args[0];
        String logFile = System.getProperty("user.dir")+ "/" + args[1];
        String outputFile = System.getProperty("user.dir")+ "/" + args[2];

        Either<String, GenFormula<VariableID>> a = Policy.read(Source.fromFile(formulaFile, Codec.fallbackSystemCodec()).mkString());


        if(a.isLeft()){
            throw new ExceptionInInitializerError();
        }else{
            //the following is the formula that we have to verify!
            GenFormula<VariableID> formula = a.right().get();
            formula.atoms();
            formula.freeVariables();

            StreamExecutionEnvironment e = StreamExecutionEnvironment.getExecutionEnvironment();
            e.setMaxParallelism(1);
            e.setParallelism(1);

            //TODO: Choose type of input (e.g., file, socket,...)
            //DataStream<String> text = e.socketTextStream("127.0.0.1", 5000);
            DataStreamSource<String> text = e.readTextFile(logFile);

            DataStream<Fact> facts = text.flatMap(new ParsingFunction(new MonpolyTraceParser()))
                                         .name("Stream Parser")
                                         .setParallelism(1)
                                         .setMaxParallelism(1);
            //could also be a MonPoly parser, depending on the input --> ?
            //The above is the stream from which we have to find the satisfactions!
            //atomic facts should go to operators that handle atoms:
            //ATOMIC OPERATORS
            BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile + "_debug", true));
            HashMap<String, OutputTag<Fact>> hashmap = new HashMap<>();
            Set<Pred<VariableID>> atomSet = formula.atoms();
            writer.write(formula.toString() + "\n");
            Iterator<Pred<VariableID>> iter = atomSet.iterator();

            while(iter.hasNext()) {
                Pred<VariableID> n = iter.next();
                hashmap.put(n.relation(), new OutputTag<Fact>(n.relation()){});

                writer.write(n.relation() + "\n");
            }

            hashmap.put(TERMINATOR_TAG, new OutputTag<Fact>(TERMINATOR_TAG){}); //I don't think someone can parse a predicate with an empty string.

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
                                System.out.println(fact.toString());
                                if(hashmap.containsKey(fact.getName())) {
                                    ctx.output(hashmap.get(fact.getName()), fact);
                                }
                            }
                        }
                    });
            Mformula mformula = (convert(formula)).accept(new Init0(formula.freeVariablesInOrder()));
            //is it normal that I have to cast here?
            DataStream<PipelineEvent> sink = mformula.accept(new MformulaVisitorFlink(hashmap, mainDataStream));
            //is the above the correct way to create a sink?


            //TODO: Validate the input arguments
            //TODO: Choose type of output (e.g., file, socket, standard output...)
            //TODO: Re-implement with non-deprecated sink (see the example code below)
            sink.writeAsText("file://" + outputFile);

//            DataStream<String> strOutput = output.map(PipelineEvent::toString);
//            strOutput.addSink(StreamingFileSink.forRowFormat(new Path("file:///tmp/flink/output"), new SimpleStringEncoder<String>("UTF-8")).build());;
            e.execute();
            //Currently, PipelineEvent is printed as "@ <timestamp> : <timepoint>" when it is a terminator and as
            // "@ <timestamp> : <timepoint> (<val>, <val>, ..., <val>)" when it's not.
            writer.close();
        }

    }


}
