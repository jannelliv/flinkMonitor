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
    List<VariableID> freeVariablesInOrder;

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
            out.collect(PipelineEvent.terminator(fact.getTimestamp(),fact.getTimepoint()));
        }else{
            assert(fact.getName().equals(this.predName) );


            ArrayList<JavaTerm<VariableID>> argsFormula = new ArrayList<>(this.args);

            List<Object> ys = fact.getArguments();
            ArrayList<Object> argsEvent = new ArrayList<>(ys);
            //Events are parametrized by data values. Two events are said to match if the
            //corresponding data values are equal.
            Optional<HashMap<VariableID, Optional<Object>>> result = matchFV(argsFormula, argsEvent);
            if(result.isPresent()){

                /*Assignment list = new Assignment();
                for (JavaTerm<VariableID> argument : this.args) {
                    //remember to iterate over the arguments, not the free variables
                    if(result.get().get(argument.toString()).isPresent()){
                        list.add(0, result.get().get(argument.toString()));
                    }
                }*/


                Assignment list = new Assignment();
                  for (VariableID formulaVariable : this.freeVariablesInOrder) {
                    if(!result.get().containsKey(formulaVariable) || !result.get().get(formulaVariable).isPresent()){
                        list.addLast(Optional.empty());
                    }else{
                        list.addLast(result.get().get(formulaVariable));
                    }
                }


                out.collect(PipelineEvent.event(fact.getTimestamp(),fact.getTimepoint(), list));
            }
            //if there are no satisfactions, we simply don't put anything in the collector.
        }
    }

    @Override
    public <T> DataStream<PipelineEvent> accept(MformulaVisitor<T> v) {
        return (DataStream<PipelineEvent>) v.visit(this);
    }

    public static Optional<HashMap<VariableID, Optional<Object>>> matchFV(List<JavaTerm<VariableID>> ts, ArrayList<Object> ys){
        //ts: arguments of the formula
        //ys: arguments of the incoming fact
        //I am using a HashMap instead of a Function, which is what is used in Verimon
        //I added the below if condition, which is not present in the Verimon implementation
        if(ts.size() != ys.size()){
            return Optional.empty();
        }else if(ts.size() == 0 && ys.size() == 0) {
            HashMap<VariableID, Optional<Object>> emptyMap = new HashMap<>();
            return Optional.of(emptyMap);
        }else {
            if(ts.size() > 0 && ys.size() > 0 && (ts.get(0) instanceof JavaConst)) {

                if(ts.get(0).toString().equals(ys.get(0).toString())) {

                    JavaTerm<VariableID> t = ts.remove(0); //from formula
                    Object y = ys.remove(0); //from fact
                    Optional<HashMap<VariableID, Optional<Object>>> partialResult =  matchFV(ts, ys);
                    //partialResult.get().put(t, Optional.of(y)); --> doesn't have to be added if it's a constant!
                    return partialResult;
                }else {
                    return Optional.empty();
                }
            }else if(ts.size() > 0 && ys.size() > 0 && (ts.get(0) instanceof JavaVar)) {

                JavaVar<VariableID> x =  (JavaVar<VariableID>) ts.remove(0);
                Object y = ys.remove(0);

                Optional<HashMap<VariableID, Optional<Object>>> recFunction = matchFV(ts, ys);
                if(!recFunction.isPresent()){
                    return Optional.empty();
                }else{
                    HashMap<VariableID, Optional<Object>> f = recFunction.get();
                    if(!f.containsKey(x) || !(f.get(x)).isPresent()){
                        f.put(x.variable(), Optional.of(y));
                        return Optional.of(f);
                    }else{
                        Object z = f.get(x).get();
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

    public static Optional<HashMap<String, Optional<Object>>> match(List<JavaTerm<VariableID>> ts, ArrayList<Object> ys){
        //ts: arguments of the formula
        //ys: arguments of the incoming fact
        //Now I am using a HashMap instead of a Function!

        if(ts.size() != ys.size() || ts.size() == 0 && ys.size() == 0) {
            HashMap<String, Optional<Object>> emptyMap = new HashMap<>();
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
