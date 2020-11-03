package ch.ethz.infsec.src;
import ch.ethz.infsec.monitor.Fact;
import ch.ethz.infsec.policy.Term;
import ch.ethz.infsec.policy.VariableID;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import scala.collection.Seq;
import scala.collection.*;
import java.util.*;
import java.util.Set;
import java.util.function.Function;

public class MPred implements Mformula<Fact> {
    String Mfotlname;
    List<Term> args;
    Set<MPred> atoms = null;
    List<VariableID> freeVariablesInOrder;

    public MPred(String Mfotlname, Seq args, List<VariableID> fvio){

        this.args = new ArrayList<Term>((Collection<? extends Term>) JavaConverters.seqAsJavaListConverter(args).asJava());
        //Above, should I have used mfotlTerm instead of Term?
        this.Mfotlname = Mfotlname;
        this.freeVariablesInOrder = fvio;

    }
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

    @Override
    public void flatMap1(List<Optional<Object>> optionals, Collector<List<Optional<Object>>> collector) throws Exception {

    }

    @Override
    public void flatMap2(List<Optional<Object>> optionals, Collector<List<Optional<Object>>> collector) throws Exception {

    }

    @Override
    public <T> DataStream<List<Optional<Object>>> accept(MformulaVisitor<T> v) {
        return null;
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
