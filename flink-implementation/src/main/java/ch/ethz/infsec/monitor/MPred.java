package ch.ethz.infsec.monitor;
import ch.ethz.infsec.monitor.Fact;
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
import java.util.function.Function;
import static ch.ethz.infsec.term.JavaTerm.convert;
import ch.ethz.infsec.util.*;
import ch.ethz.infsec.monitor.visitor.*;

public class MPred implements Mformula, FlatMapFunction<Fact, PipelineEvent> {
    String predName;
    ArrayList<JavaTerm<VariableID>> args; // when you assign this now, you will need to convert it
    List<VariableID> freeVariablesInOrder;
    //if you had a formula p(x,y) AND q(z), then at both the p(x,y) predicate and the q(z) predicate you need
    //to have the same list of free variables in some order, like x,y,z. So to create a pipeline event
    //you need to iterate over freeVariablesInOrder. Args on the other hand, are the terms of the predicate,
    //so if you have a predicate p(x,y) then the args would be [x,y]. But if you have a formula where p(x,y) occurs,
    //like p(x,y) AND q(z), then freeVariablesInOrder will be [x,y,z] and args will be [x,y]. SO somehow at the
    //predicate level, you need to establish an order of variables that occur outside of the predicate, this is
    //why in Init0 we compute the freeVariablesInOrder. But the match has to be done with the args, not the free
    //variables, because the predicate and the event have corresponding numbers of parameters.


    public MPred(String predName, Seq<Term<VariableID>> args, List<VariableID> fvio){
        System.out.println("Inside the MPred() constructor method");
        List<Term<VariableID>> argsScala = new ArrayList(JavaConverters.seqAsJavaList(args));
        ArrayList<JavaTerm<VariableID>> argsJava = new ArrayList<>();
        for (Term<VariableID> variableIDTerm : argsScala) {
            //is this for loop very bad in terms of efficiency?
            argsJava.add(convert(variableIDTerm));
        }
        this.predName = predName;
        this.freeVariablesInOrder = fvio;
        this.args = argsJava;
        //System.out.println("Predname" + this.predName);
        //System.out.println("freeVars" + this.freeVariablesInOrder);
        //System.out.println("arguments" + this.args + ";  arg size: " + this.args.size());

    }

    public String getPredName(){
      return predName;
    }


    public void flatMap(Fact fact, Collector<PipelineEvent> out) throws Exception {
        //System.out.println("Inside the flatMap() method");
        //System.out.println("fact:  " + fact.toString());
        if(fact.isTerminator()){
            Assignment none = Assignment.nones(0); //why 0?
            out.collect(new PipelineEvent(fact.getTimestamp(),fact.getTimepoint(),  true, none));
        }else{
            assert(fact.getName().equals(this.predName) );

            List<Object> ys = fact.getArguments();

            //System.out.println("fact Arguments: " + ys.toString());

            ArrayList<JavaTerm<VariableID>> argsCopy = new ArrayList<>(this.args);

            ArrayList<Object> c_ys = new ArrayList<>(ys);
            Optional<Function<String, Optional<Object>>> result = match(argsCopy, c_ys);
            if(result.isPresent()){
                Assignment list = new Assignment();
                //building of satisfaction, but not from the free Variables of MPred (this) --> ?
                for (VariableID argument : this.freeVariablesInOrder) {
                    //remember to iterate over the arguments, not the free variables
                    if(result.get().apply(argument.toString()).isPresent()){
                        list.add(0, result.get().apply(argument.toString()));
                    }

                    //System.out.println("ass. element " + (result.get().apply(argument.toString())).toString());
                }
                out.collect(new PipelineEvent(fact.getTimestamp(),fact.getTimepoint(),  false, list));
            }else{
                //System.out.println("result not present :(");
            }
            //if there are no satisfactions, we simply don't put anything in the collector.
        }


    }

    @Override
    public <T> DataStream<PipelineEvent> accept(MformulaVisitor<T> v) {
        return (DataStream<PipelineEvent>) v.visit(this);
        //Is it ok that I did the cast here above?
    }

    public static Optional<Function<String, Optional<Object>>> match(List<JavaTerm<VariableID>> ts, ArrayList<Object> ys){
        //System.out.println("Inside the match() method");
        if(ts.size() != ys.size() || ts.size() == 0 && ys.size() == 0) {
            Function<String, Optional<Object>> emptyMap = s -> Optional.empty();
            return Optional.of(emptyMap);
        }else {
            if(ts.size() > 0 && ys.size() > 0 && (ts.get(0) instanceof JavaConst)) {
                //System.out.println("reach1");
                if(ts.get(0).equals(ys.get(0))) {

                    ts.remove(0);
                    ys.remove(0);
                    return match(ts, ys);
                }else {
                    return Optional.empty();
                }
            }else if(ts.size() > 0 && ys.size() > 0 && (ts.get(0) instanceof JavaVar)) {
                //System.out.println("reach2");
                JavaVar<VariableID> x =  (JavaVar<VariableID>) ts.remove(0);
                Object y = ys.remove(0);
                //the above line gave me an error with testPred():
                //UnsupportedOperationException at java.util.AbstractList.remove(AbstractList.java: 167)
                //Changed second argument of match() to ArrayList
                //Not sure why the error was not present with the previous "non-test" input

                Optional<Function<String, Optional<Object>>> recFunction = match(ts, ys);
                if(!recFunction.isPresent()){
                    //System.out.println("reach4");
                    return Optional.empty();
                }else{
                    Function<String, Optional<Object>> f = recFunction.get();
                    if(!(f.apply(x.toString())).isPresent()){
                        //System.out.println("reach5");
                        //arrives here
                        Function<Optional<Object>, Optional<Object>> after = x1 -> {
                            if(x1.equals(f.apply(x.toString()))){//still not sure if this is the best way to do this
                                //System.out.println("reachThis");
                                return Optional.of(y);
                            }else{
                                return f.apply(x1.toString());
                            }
                        };
                        Function<String, Optional<Object>> mappingFunction = f.andThen(after);
                        return Optional.of(mappingFunction);
                    }else{
                        //System.out.println("reach6");
                        Object z = f.apply(x.toString()).get();
                        Object u = ys.get(0);
                        if (u.equals(z)){
                            return Optional.of(f);
                        }else{
                            //System.out.println("reach7");
                            return Optional.empty();
                        }

                    }
                }
            }else{
                //System.out.println("reach3");
                return Optional.empty();
            }
        }

    }


}
