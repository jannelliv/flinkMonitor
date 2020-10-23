package ch.ethz.infsec.src;

import ch.ethz.infsec.monitor.Fact;
import ch.ethz.infsec.policy.GenFormula;
import ch.ethz.infsec.policy.Policy;
import ch.ethz.infsec.policy.*;
import ch.ethz.infsec.trace.ParsingFunction;
import ch.ethz.infsec.trace.parser.Crv2014CsvParser;
import ch.ethz.infsec.trace.parser.MonpolyTraceParser;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import scala.Option;
import scala.collection.Iterator;
import scala.collection.Set;
import scala.reflect.ClassTag;
import scala.util.Either;
import scala.collection.JavaConverters.*;

import java.util.*;
import java.util.function.Function;



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


            HashMap<String, OutputTag<Fact>> hashmap = new HashMap<String, OutputTag<Fact>>();


            Set<Pred<VariableID>> atomSet = formula.atoms();
            Iterator iter = atomSet.iterator();
            OutputTag<Fact> outputTag;
            while(iter.hasNext()) {
                Pred<VariableID> n = (Pred<VariableID>) iter.next();
                outputTag = new OutputTag<Fact>(n.toString()) {};
                hashmap.put(n.relation(), outputTag);
            }
            SingleOutputStreamOperator<Fact> mainDataStream = facts
                    .process(new ProcessFunction<Fact, Fact>() {

                        @Override
                        public void processElement(
                                Fact fact,
                                Context ctx,
                                Collector<Fact> out) throws Exception {
                            // emit data to regular output
                            //out.collect(fact);

                            // emit data to side output
                            ctx.output(hashmap.get(fact.getName()), fact);
                        }
                    });
            DataStream<List<Optional<Object>>> sink = writeTo((JavaGenFormula) formula, (JavaGenFormula) formula, hashmap, mainDataStream);
            //is the above the correct way to create a sink?
            e.execute();
        }

    }

    public static DataStream<List<Optional<Object>>> writeTo(JavaGenFormula formula, JavaGenFormula subformula, HashMap<String, OutputTag<Fact>> hashmap,
                                                             SingleOutputStreamOperator<Fact> mainDataStream){

        return (DataStream<List<Optional<Object>>>) subformula.accept(new StructFormulaVisitor() {
            @Override
            public DataStream<List<Optional<Object>>> visit(JavaPred f) {
                OutputTag<Fact> factStream = hashmap.get(((JavaPred) f).toString());
                return mainDataStream.getSideOutput(factStream).flatMap(new FlatMapFunction<Fact, List<Optional<Object>>>() {
                    @Override
                    public void flatMap(Fact value, Collector<List<Optional<Object>>> out) throws Exception {
                        if(value.getName().equals(f.relation()) && value.getArity() == f.args().size()){
                        //TODO: remove safe check and put it in the match method
                            Object[] tsArray = f.args().toArray(ClassTag.Object());
                            List<Object> ts = new ArrayList<>(Arrays.asList(tsArray));
                            List<Object> ys = value.getArguments();
                            Optional<Function<String, Optional<Object>>> result = match(ts, ys);
                            if(result.isPresent()){
                                List<Optional<Object>> list = new LinkedList<Optional<Object>>();
                                //building of satisfaction, from the fact:
                                for(freeVar : formula.freeVariablesInOrder()){
                                    if(!f.freeVariables().contains(freeVar)) {
                                        Optional<Object> none = Optional.empty();
                                        list.add(none);
                                    }else{
                                        List<Object> factArgs = value.getArguments();
                                        for(int i = 0; i < factArgs.size(); i++){
                                            //this is not really efficient, but is it correct?
                                            if(factArgs.get(i).toString().equals(freeVar.toString())){
                                                Optional<Object> satValue = Optional.of(factArgs.get(i));
                                                list.add(satValue);
                                            }
                                        }
                                    }
                                }
                                out.collect(list);
                            }

                        }
                        //if there are no satisfactions, we simply don't put anything in the collector.

                    }
                });

            }

            @Override
            public DataStream<List<Optional<Object>>> visit(JavaNot f) {
                DataStream<List<Optional<Object>>> input = writeTo((JavaGenFormula) formula, (JavaGenFormula)  f.arg(), hashmap, mainDataStream);
                //is it ok to do the cast as I did above?

                return input.flatMap(new FlatMapFunction<List<Optional<Object>>, List<Optional<Object>>>() {
                    @Override
                    public void flatMap(List<Optional<Object>> value, Collector<List<Optional<Object>>> out) throws Exception {
                        //do I have to implement the flink/isabelle operator logic here?
                    }
                });
            }

            @Override
            public DataStream<List<Optional<Object>>> visit(JavaAnd f) {
                DataStream<List<Optional<Object>>> input1 = writeTo((JavaGenFormula) formula, (JavaGenFormula) f.arg1(), hashmap, mainDataStream);
                DataStream<List<Optional<Object>>> input2 = writeTo((JavaGenFormula) formula, (JavaGenFormula) f.arg2(), hashmap, mainDataStream);
                ConnectedStreams<List<Optional<Object>>, List<Optional<Object>>> connectedStreams = input1.connect(input2);
                return connectedStreams.flatMap(new CoFlatMapFunction<List<Optional<Object>>, List<Optional<Object>>,
                        List<Optional<Object>>>() {

                    @Override
                    public void flatMap1(List<Optional<Object>> optionals, Collector<List<Optional<Object>>> collector) throws Exception {

                    }

                    @Override
                    public void flatMap2(List<Optional<Object>> optionals, Collector<List<Optional<Object>>> collector) throws Exception {

                    }
                });

            }

            @Override
            public DataStream<List<Optional<Object>>> visit(JavaAll f) {
                DataStream<List<Optional<Object>>> input = writeTo((JavaGenFormula) formula, (JavaGenFormula)  f.arg(), hashmap, mainDataStream);


                return input.flatMap(new FlatMapFunction<List<Optional<Object>>, List<Optional<Object>>>() {
                    @Override
                    public void flatMap(List<Optional<Object>> value, Collector<List<Optional<Object>>> out) throws Exception {

                    }
                });

            }

            @Override
            public DataStream<List<Optional<Object>>> visit(JavaEx f) {
                DataStream<List<Optional<Object>>> input = writeTo((JavaGenFormula) formula, (JavaGenFormula)  f.arg(), hashmap, mainDataStream);


                return input.flatMap(new FlatMapFunction<List<Optional<Object>>, List<Optional<Object>>>() {
                    @Override
                    public void flatMap(List<Optional<Object>> value, Collector<List<Optional<Object>>> out) throws Exception {

                    }
                });
            }

            @Override
            public DataStream<List<Optional<Object>>> visit(JavaFalse f) {
                return null; //don't know how to handle this!
            }

            @Override
            public DataStream<List<Optional<Object>>> visit(JavaTrue f) {
                return null;
            }

            @Override
            public DataStream<List<Optional<Object>>> visit(JavaNext f) {
                DataStream<List<Optional<Object>>> input = writeTo((JavaGenFormula) formula, (JavaGenFormula)  f.arg(), hashmap, mainDataStream);


                return input.flatMap(new FlatMapFunction<List<Optional<Object>>, List<Optional<Object>>>() {
                    @Override
                    public void flatMap(List<Optional<Object>> value, Collector<List<Optional<Object>>> out) throws Exception {

                    }
                });
            }

            @Override
            public DataStream<List<Optional<Object>>> visit(JavaOr f) {
                DataStream<List<Optional<Object>>> input1 = writeTo((JavaGenFormula) formula, (JavaGenFormula) f.arg1(), hashmap, mainDataStream);
                DataStream<List<Optional<Object>>> input2 = writeTo((JavaGenFormula) formula, (JavaGenFormula) f.arg2(), hashmap, mainDataStream);
                ConnectedStreams<List<Optional<Object>>, List<Optional<Object>>> connectedStreams = input1.connect(input2);
                return connectedStreams.flatMap(new CoFlatMapFunction<List<Optional<Object>>, List<Optional<Object>>,
                        List<Optional<Object>>>() {

                    @Override
                    public void flatMap1(List<Optional<Object>> optionals, Collector<List<Optional<Object>>> collector) throws Exception {

                    }

                    @Override
                    public void flatMap2(List<Optional<Object>> optionals, Collector<List<Optional<Object>>> collector) throws Exception {

                    }
                });
            }

            @Override
            public DataStream<List<Optional<Object>>> visit(JavaPrev f) {
                DataStream<List<Optional<Object>>> input = writeTo((JavaGenFormula) formula, (JavaGenFormula)  f.arg(), hashmap, mainDataStream);


                return input.flatMap(new FlatMapFunction<List<Optional<Object>>, List<Optional<Object>>>() {
                    @Override
                    public void flatMap(List<Optional<Object>> value, Collector<List<Optional<Object>>> out) throws Exception {

                    }
                });
            }

            @Override
            public DataStream<List<Optional<Object>>> visit(JavaSince f) {
                DataStream<List<Optional<Object>>> input1 = writeTo((JavaGenFormula) formula, (JavaGenFormula) f.arg1(), hashmap, mainDataStream);
                DataStream<List<Optional<Object>>> input2 = writeTo((JavaGenFormula) formula, (JavaGenFormula) f.arg2(), hashmap, mainDataStream);
                ConnectedStreams<List<Optional<Object>>, List<Optional<Object>>> connectedStreams = input1.connect(input2);
                return connectedStreams.flatMap(new CoFlatMapFunction<List<Optional<Object>>, List<Optional<Object>>,
                        List<Optional<Object>>>() {

                    @Override
                    public void flatMap1(List<Optional<Object>> optionals, Collector<List<Optional<Object>>> collector) throws Exception {

                    }

                    @Override
                    public void flatMap2(List<Optional<Object>> optionals, Collector<List<Optional<Object>>> collector) throws Exception {

                    }
                });
            }

            @Override
            public DataStream<List<Optional<Object>>> visit(JavaUntil f) {
                DataStream<List<Optional<Object>>> input1 = writeTo((JavaGenFormula) formula, (JavaGenFormula) f.arg1(), hashmap, mainDataStream);
                DataStream<List<Optional<Object>>> input2 = writeTo((JavaGenFormula) formula, (JavaGenFormula) f.arg2(), hashmap, mainDataStream);
                ConnectedStreams<List<Optional<Object>>, List<Optional<Object>>> connectedStreams = input1.connect(input2);
                return connectedStreams.flatMap(new CoFlatMapFunction<List<Optional<Object>>, List<Optional<Object>>,
                        List<Optional<Object>>>() {

                    @Override
                    public void flatMap1(List<Optional<Object>> optionals, Collector<List<Optional<Object>>> collector) throws Exception {

                    }

                    @Override
                    public void flatMap2(List<Optional<Object>> optionals, Collector<List<Optional<Object>>> collector) throws Exception {

                    }
                });
            }
        });
    }

    public static Optional<Function<String, Optional<Object>>> match(List<Object> ts, List<Object> ys){
        if(ts.size() == 0 && ys.size() == 0) {
            Function<String, Optional<Object>> emptyMap = new Function<String, Optional<Object>>() {
                @Override
                public Optional<Object> apply(String s) {
                    return Optional.empty();
                }
            };
            Optional<Function<String, Optional<Object>>> result = Optional.of(emptyMap);
            return result;
        }else {
            if(ts.size() > 0 && ys.size() > 0 && (ts.get(0) instanceof JavaConst)) {
                if(ts.get(0).equals(ys.get(0))) {

                    ts.remove(0);
                    ys.remove(0);
                    return match(ts, ys);
                }else {
                    return Optional.empty();
                }
            }else if(ts.size() > 0 && ys.size() > 0 && (ts.get(0) instanceof JavaVar)) {
                JavaVar x = (JavaVar) ts.remove(0);

                Object y = ys.remove(0);
                Optional<Function<String, Optional<Object>>> recFunction = match(ts, ys);
                if(!recFunction.isPresent()){
                    return Optional.empty();
                }else{
                    Function<String, Optional<Object>> f = recFunction.get();
                    if(!(f.apply(x.toString())).isPresent()){
                        Function<Optional<Object>, Optional<Object>> after = new Function<Optional<Object>, Optional<Object>>() {
                            @Override
                            public Optional<Object> apply(Optional<Object> x) {
                                if(ts.indexOf(x) == 0){
                                    Optional<Object> yOpt = Optional.of(ys.get(0));
                                    return yOpt;
                                }else{
                                    return  f.apply(x.toString());
                                }

                            }
                        };
                        Function<String, Optional<Object>> mappingFunction = f.andThen(after);

                    }else{
                        Object z = f.apply(x.toString()).get();
                        Object u = ys.get(0);
                        if (u.equals(z)){
                            return Optional.of(f);
                        }else{
                            return Optional.empty();
                        }

                    }
                }
            }else{
                return Optional.empty();
            }
        }
        return Optional.empty();

    }
}
