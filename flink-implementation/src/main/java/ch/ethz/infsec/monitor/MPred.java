package ch.ethz.infsec.monitor;
import ch.ethz.infsec.policy.Term;
import ch.ethz.infsec.policy.VariableID;
import ch.ethz.infsec.term.JavaConst;
import ch.ethz.infsec.term.JavaTerm;
import ch.ethz.infsec.term.JavaVar;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import scala.collection.Seq;
import scala.collection.*;
import java.util.*;
import static ch.ethz.infsec.term.JavaTerm.convert;
import ch.ethz.infsec.util.*;
import ch.ethz.infsec.monitor.visitor.*;

public final class MPred implements Mformula, FlatMapFunction<Fact, PipelineEvent> {
    String predName;
    ArrayList<JavaTerm<VariableID>> args;
    List<VariableID> freeVariablesInOrder; //I DON'T ACTUALLY USE THIS! :/


    //I AM REALLY NOT SURE ABOUT USING TOSTRING IN THE MATCH METHOD!!


    public MPred(String predName, Seq<Term<VariableID>> args, List<VariableID> fvio){
        List<Term<VariableID>> argsScala = new ArrayList(JavaConverters.seqAsJavaList(args));
        ArrayList<JavaTerm<VariableID>> argsJava = new ArrayList<>();
        for (Term<VariableID> variableIDTerm : argsScala) {
            argsJava.add(convert(variableIDTerm));
        }
        this.predName = predName;
        this.freeVariablesInOrder = fvio;
        this.args = argsJava;
    }

    public String getPredName(){
      return predName;
    }


    public void flatMap(Fact fact, Collector<PipelineEvent> out) throws Exception {
        if(fact.isTerminator()){
            Assignment none = Assignment.nones(0); //why 0?
            out.collect(new PipelineEvent(fact.getTimestamp(),fact.getTimepoint(),  true, none));
        }else{
            assert(fact.getName().equals(this.predName) );

            List<Object> ys = fact.getArguments();
            ArrayList<JavaTerm<VariableID>> argsFormula = new ArrayList<>(this.args);

            ArrayList<Object> argsEvent = new ArrayList<>(ys);
            Optional<HashMap<String, Optional<Object>>> result = match(argsFormula, argsEvent);
            //above: changed Function to HashMap as the return type for match().
            if(result.isPresent()){
                Assignment list = new Assignment();
                //building of satisfaction, but not from the free Variables of MPred (this) --> ?
                for (JavaTerm<VariableID> argument : this.args) {
                    //remember to iterate over the arguments, not the free variables
                    if(result.get().get(argument.toString()).isPresent()){
                        list.add(0, result.get().get(argument.toString()));
                    }
                }
                out.collect(new PipelineEvent(fact.getTimestamp(),fact.getTimepoint(),  false, list));
            }
            //if there are no satisfactions, we simply don't put anything in the collector.
        }
    }

    @Override
    public <T> DataStream<PipelineEvent> accept(MformulaVisitor<T> v) {
        return (DataStream<PipelineEvent>) v.visit(this);
        //Is it ok that I did the cast here above?
    }

    public static Optional<HashMap<String, Optional<Object>>> match(List<JavaTerm<VariableID>> ts, ArrayList<Object> ys){
        //ts: arguments of the formula
        //ys: arguments of the incoming fact
        //Now I am using a HashMap instead of a Function!

        if(ts.size() != ys.size() || ts.size() == 0 && ys.size() == 0) {
            HashMap<String, Optional<Object>> emptyMap = new HashMap<>(); //s -> Optional.empty();
            //emptyMap.put("all", Optional.empty()); //not necessary???
            return Optional.of(emptyMap);
        }else {
            if(ts.size() > 0 && ys.size() > 0 && (ts.get(0) instanceof JavaConst)) {

                if(ts.get(0).toString().equals(ys.get(0).toString())) { //is it ok to do things with toString????

                    JavaTerm<VariableID> t = ts.remove(0); //from formula
                    Object y = ys.remove(0); //from fact

                    Optional<HashMap<String, Optional<Object>>> partialResult =  match(ts, ys);
                    partialResult.get().put(t.toString(), Optional.of(y));
                    return partialResult;
                }else {
                    return Optional.empty();
                }
            }else if(ts.size() > 0 && ys.size() > 0 && (ts.get(0) instanceof JavaVar)) {

                JavaVar<VariableID> x =  (JavaVar<VariableID>) ts.remove(0);
                Object y = ys.remove(0);
                //the above line gave me an error with testPred():
                //UnsupportedOperationException at java.util.AbstractList.remove(AbstractList.java: 167)
                //Changed second argument of match() to ArrayList
                //Not sure why the error was not present with the previous "non-test" input
                Optional<HashMap<String, Optional<Object>>> recFunction = match(ts, ys);
                if(!recFunction.isPresent()){
                    return Optional.empty();
                }else{
                    HashMap<String, Optional<Object>> f = recFunction.get();
                    if(!f.containsKey(x.toString()) || !(f.get(x.toString())).isPresent()){
                        f.put(x.toString(), Optional.of(y));
                        return Optional.of(f);
                        /*Function<Optional<Object>, Optional<Object>> after = x1 -> {
                            if(x1.equals(f.apply(x.toString()))){//still not sure if this is the best way to do this
                                return Optional.of(y);
                            }else{
                                return f.apply(x1.toString());
                            }
                        };
                        Function<String, Optional<Object>> mappingFunction = f.andThen(after);
                        return Optional.of(mappingFunction);*/
                    }else{
                        Object z = f.get(x.toString()).get();
                        //Object u = ys.get(0); --> NOT TESTED YET!!!
                        if (y.equals(z)){
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

    }


}
