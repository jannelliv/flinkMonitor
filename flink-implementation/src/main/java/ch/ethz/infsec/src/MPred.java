package ch.ethz.infsec.src;
import ch.ethz.infsec.monitor.Fact;
import ch.ethz.infsec.policy.Term;
import ch.ethz.infsec.policy.VariableID;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import scala.collection.Seq;
import scala.collection.*;
import java.util.*;
import java.util.function.Function;

public class MPred implements Mformula<Fact>{
    String predName;
    List<Term<VariableID>> args;
    List<VariableID> freeVariablesInOrder;

    public MPred(String predName, Seq<Term<VariableID>> args, List<VariableID> fvio){

        this.args = new ArrayList<>(JavaConverters.seqAsJavaList(args));
        //Above, should I have used mfotlTerm instead of Term?
        this.predName = predName;
        this.freeVariablesInOrder = fvio;

    }
    public void flatMap(Fact fact, Collector<List<Optional<Object>>> out) throws Exception {
        if(fact.getName().equals(this.predName) && fact.getArity() == this.args.size()){
            //TODO: remove safe check and put it in the match method
            Object[] tsArray = this.args.toArray();
            List<Object> ts = new ArrayList<>(Arrays.asList(tsArray));
            List<Object> ys = fact.getArguments();
            Optional<Function<String, Optional<Object>>> result = match(ts, ys);
            if(result.isPresent()){ //is it necessary to have this if condition?
                List<Optional<Object>> list = new LinkedList<>();
                //building of satisfaction, from the free Variables of MPred (this)
                for(int i = 0; i < this.freeVariablesInOrder.size(); i++){
                    VariableID freeVarPred = this.freeVariablesInOrder.get(i);
                    list.add(result.get().apply(freeVarPred.toString()));
                }
                out.collect(list);
            }

        }
        //if there are no satisfactions, we simply don't put anything in the collector.

    }

    @Override
    public void flatMap1(List<Optional<Object>> optionals, Collector<List<Optional<Object>>> collector) throws Exception {

    }

    @Override
    public void flatMap2(List<Optional<Object>> optionals, Collector<List<Optional<Object>>> collector) throws Exception {

    }

    @Override
    public <T> DataStream<List<Optional<Object>>> accept(MformulaVisitor<T> v) {
        return (DataStream<List<Optional<Object>>>) v.visit(this);
        //Is it ok that I did the cast here above?
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
