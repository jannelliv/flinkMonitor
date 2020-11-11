package ch.ethz.infsec.src;

import ch.ethz.infsec.monitor.Fact;
import ch.ethz.infsec.policy.GenFormula;
import ch.ethz.infsec.policy.Policy;
import ch.ethz.infsec.policy.*;
import ch.ethz.infsec.trace.ParsingFunction;
import ch.ethz.infsec.trace.parser.Crv2014CsvParser;
//import ch.ethz.infsec.trace.parser.MonpolyTraceParser;
//import org.apache.flink.api.common.functions.FilterFunction;
//import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.functions.ProcessFunction;
//import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import scala.collection.Iterator;

import scala.collection.Set;

import scala.util.Either;

import java.util.*;
import static ch.ethz.infsec.src.JavaGenFormula.convert;


public class Test {
    public static void main(String[] args) throws Exception{
        Either<String, GenFormula<VariableID>> a = Policy.read("(x > 2)");
        // See what the input should actually look like! ???

        if(a.isLeft()){
            throw new ExceptionInInitializerError();
        }else{
            //the following is the formula that we have to verify!
            GenFormula<VariableID> formula = a.right().get();
            formula.atoms();
            formula.freeVariables();

            StreamExecutionEnvironment e = StreamExecutionEnvironment.getExecutionEnvironment();
            DataStream<String> text = e.socketTextStream("127.0.0.1", 5000);
            //not sure if this hostname and portname make sense!
            DataStream<Fact> facts = text.flatMap(new ParsingFunction(new Crv2014CsvParser()));
            //could also be a MonPoly parser, depending on the input --> ?
            //The above is the stream from which we have to find the satisfactions!
            //atomic facts should go to operators that handle atoms:
            //ATOMIC OPERATORS
            HashMap<String, OutputTag<Fact>> hashmap = new HashMap<>();
            Set<Pred<VariableID>> atomSet = formula.atoms();
            Iterator<Pred<VariableID>> iter = atomSet.iterator();
            OutputTag<Fact> outputTag;
            while(iter.hasNext()) {
                Pred<VariableID> n = iter.next();
                outputTag = new OutputTag<>(n.toString());
                hashmap.put(n.relation(), outputTag);
            }
            hashmap.put("", new OutputTag<>("")); //I don't think someone can parse a predicate with an empty string.

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
                                ctx.output(hashmap.get(fact.getName()), fact);
                            }
                        }
                    });
            Mformula mformula = (convert(formula)).accept(new Init0(formula.freeVariablesInOrder()));
            //is it normal that I have to cast here?
            DataStream<Optional<List<Optional<Object>>>> sink = mformula.accept(new MformulaVisitorFlink(mformula, mformula, hashmap, mainDataStream));
            //is the above the correct way to create a sink?
            e.execute();
        }

    }


}
