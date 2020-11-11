package ch.ethz.infsec.src;
import ch.ethz.infsec.monitor.Fact;
import ch.ethz.infsec.policy.Term;
import ch.ethz.infsec.policy.VariableID;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import scala.collection.Seq;
import scala.collection.*;
import java.util.*;
import java.util.function.Function;
import static ch.ethz.infsec.src.JavaTerm.convert;

public class MPred implements Mformula, FlatMapFunction<Fact, Optional<List<Optional<Object>>>> {
    String predName;
    ArrayList<JavaTerm<VariableID>> args; // when you assign this now, you will need to convert it
    List<VariableID> freeVariablesInOrder;


    public MPred(String predName, Seq<Term<VariableID>> args, List<VariableID> fvio){

        List<Term<VariableID>> argsScala = new ArrayList<>(JavaConverters.seqAsJavaList(args));
        ArrayList<JavaTerm<VariableID>> argsJava = new ArrayList<>();
        for (Term<VariableID> variableIDTerm : argsScala) {
            //is this for loop very bad in terms of efficiency?
            argsJava.add(convert(variableIDTerm));
        }
        this.predName = predName;
        this.freeVariablesInOrder = fvio;
        this.args = argsJava;

    }

    public String getPredName(){
      return predName;
    }


    public void flatMap(Fact fact, Collector<Optional<List<Optional<Object>>>> out) throws Exception {
        if(fact.isTerminator()){
            Optional<List<Optional<Object>>> none = Optional.empty();
            out.collect(none);
        }
        assert(fact.getName().equals(this.predName) );

        List<Object> ys = fact.getArguments();
        Optional<Function<String, Optional<Object>>> result = match(this.args, ys);
        if(result.isPresent()){
            List<Optional<Object>> list = new LinkedList<>();
            //building of satisfaction, from the free Variables of MPred (this)
            for (VariableID freeVarPred : this.freeVariablesInOrder) {
                list.add(result.get().apply(freeVarPred.toString()));
            }
            Optional<List<Optional<Object>>> assignment = Optional.of(list);
            out.collect(assignment);
        }
        //if there are no satisfactions, we simply don't put anything in the collector.

    }

    @Override
    public <T> DataStream<Optional<List<Optional<Object>>>> accept(MformulaVisitor<T> v) {
        return (DataStream<Optional<List<Optional<Object>>>>) v.visit(this);
        //Is it ok that I did the cast here above?
    }

    public static Optional<Function<String, Optional<Object>>> match(List<JavaTerm<VariableID>> ts, List<Object> ys){
        if(ts.size() != ys.size() || ts.size() == 0 && ys.size() == 0) {
            Function<String, Optional<Object>> emptyMap = s -> Optional.empty();
            return Optional.of(emptyMap);
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
                JavaVar<VariableID> x =  (JavaVar<VariableID>) ts.remove(0);

                Object y = ys.remove(0);
                Optional<Function<String, Optional<Object>>> recFunction = match(ts, ys);
                if(!recFunction.isPresent()){
                    return Optional.empty();
                }else{
                    Function<String, Optional<Object>> f = recFunction.get();
                    if(!(f.apply(x.toString())).isPresent()){
                        Function<Optional<Object>, Optional<Object>> after = x1 -> {
                            if(ts.indexOf(x1) == 0){
                                return Optional.of(ys.get(0));
                            }else{
                                return  f.apply(x1.toString());
                            }

                        };
                        Function<String, Optional<Object>> mappingFunction = f.andThen(after);
                        return Optional.of(mappingFunction);

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

    }


}
