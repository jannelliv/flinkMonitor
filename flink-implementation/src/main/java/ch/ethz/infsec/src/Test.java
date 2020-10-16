package ch.ethz.infsec.src;

import ch.ethz.infsec.monitor.Fact;
import ch.ethz.infsec.policy.GenFormula;
import ch.ethz.infsec.policy.Policy;
import ch.ethz.infsec.policy.*;
import ch.ethz.infsec.trace.ParsingFunction;
import ch.ethz.infsec.trace.parser.Crv2014CsvParser;
import ch.ethz.infsec.trace.parser.MonpolyTraceParser;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.*;
import scala.reflect.ClassTag;
import scala.util.Either;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public class Test {
    public static void main(String[] args){
        Either<String, GenFormula<VariableID>> a = Policy.read("");
        // See what the input should actually look like! ???
        //the input is the string which represents the formula, and it can also be given via the terminal
        //The return type is Either a String or a Policy formula
        //check
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
            //DataStream<Fact> facts2 = text.flatMap(new ParsingFunction(new MonpolyTraceParser()));
            //Which parser of the above two should I be working with?
            //The above is the stream from which we have to find the satisfactions!

            //atomic facts should go to operators that handle atoms:
            //ATOMIC OPERATORS

            DataStream<Fact> atomicSatisfactions = facts.filter(new FilterFunction<Fact>() {
                @Override
                public boolean filter(Fact event) throws Exception {
                    //filter out the events that satisfy the leaves of the parsing
                    //tree, aka the predicates.
                    //Get the predicates
                    //create a filter for each predicate and a new datastream for each pred!
                    //This whole thing should actually be implemented with a different method
                    return true;
                }
            });
        }
    }

    public static DataStream<Fact> meval(DataStream<Fact> inputDS, JavaGenFormula f){
        return (DataStream<Fact>) f.accept(new MevalFormulaVisitor() {
            @Override
            public DataStream<Fact> visit(JavaPred f) {

                return inputDS.filter(new FilterFunction<Fact>() {
                    @Override
                    public boolean filter(Fact fact) throws Exception {
                        if(fact.getName().equals(f.relation()) && fact.getArity() == f.args().size()){
                            Object[] ts = f.args().toArray(ClassTag.Object());
                            //not sure if the above makes sense
                            Object[] ys = fact.getArguments().toArray();
                            for(int i = 0; i < ts.length; i++){
                                if(ts[i] instanceof JavaConst){
                                    if(ts[i].equals(ys[i])){
                                        continue;
                                    }else{
                                        return false;
                                    }
                                }else if(ts[i] instanceof JavaVar){

                                }else{
                                    return false;
                                    //this should actually return an error
                                }
                            }

                        }
                        return false;

                        //instead of terminator, this has to be isPred, somehow
                    }
                });
            }

            @Override
            public DataStream<Fact> visit(JavaNot f) {
                return null;
            }

            @Override
            public DataStream<Fact> visit(JavaAnd f) {
                return null;
            }

            @Override
            public DataStream<Fact> visit(JavaAll f) {
                return null;
            }

            @Override
            public DataStream<Fact> visit(JavaEx f) {
                return null;
            }

            @Override
            public DataStream<Fact> visit(JavaFalse f) {
                return null;
            }

            @Override
            public DataStream<Fact> visit(JavaTrue f) {
                return null;
            }

            @Override
            public DataStream<Fact> visit(JavaNext f) {
                return null;
            }

            @Override
            public DataStream<Fact> visit(JavaOr f) {
                return null;
            }

            @Override
            public DataStream<Fact> visit(JavaPrev f) {
                return null;
            }

            @Override
            public DataStream<Fact> visit(JavaSince f) {
                return null;
            }

            @Override
            public DataStream<Fact> visit(JavaUntil f) {
                return null;
            }
        });

    }

    //ask if my implementation of match is as expected and similar to the isabelle code
    public static Optional<Function<Integer, Object>> match(List<Object> ts, List<Object> ys){
        //are the elements of ts the indices of the terms in the predicate arguments? I don't think so
        //Is it wrong to have an Object here as a function output?
        if(ts.size() == 0 && ys.size() == 0) {
            Function<Integer, Object> emptyMap = new Function<Integer, Object>() {
                //are the indices of the predicate arguments used as the domain of maps, e.g. emptyMap?
                @Override
                public Optional<Object> apply(Integer integer) {
                    return null;
                }
            };
            Optional<Function<Integer, Object>> result = emptyMap;
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
                Optional<Function<Integer, Optional<Object>>> recFunction = match(ts, ys);
                if(!recFunction.isPresent()){
                    return Optional.empty();
                }else{
                    Function<Integer, Object> f = recFunction.get();
                    if(!(f.apply(0)).isPresent()){
                        Function<Integer, Object> after = new Function<Integer, Object>() {
                            @Override
                            public Optional<Object> apply(Integer index) {
                                if(index == 0){
                                    //Optional<Object> yOpt = Optional.of(ys.get(0));
                                    return ys.get(0); //yOpt;
                                }else{
                                    return recFunction.apply(index);
                                }
                                return null;
                            }
                        };
                        Function<Integer, Optional<Object>> mappingFunction = (Function<Integer, Optional<Object>>) f.andThen(after);

                    }
                }
            }else{
                return Optional.empty();
            }
        }
        return null;

    }
}
