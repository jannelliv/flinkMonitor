package ch.ethz.infsec.src;


import ch.ethz.infsec.policy.VariableID;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Optional;

public class MExists implements Mformula, FlatMapFunction<Optional<List<Optional<Object>>>, Optional<List<Optional<Object>>>> {

    Mformula subFormula;
    VariableID var; //is the way I constructed this from Init0 correct?


    public MExists(Mformula subformula, VariableID var){
        this.subFormula = subformula;
        this.var = var;

    }

    @Override
    public <T> DataStream<Optional<List<Optional<Object>>>> accept(MformulaVisitor<T> v) {
        return (DataStream<Optional<List<Optional<Object>>>>) v.visit(this);
        //Is it ok that I did the cast here above?
    }


    @Override
    public void flatMap(Optional<List<Optional<Object>>> value, Collector<Optional<List<Optional<Object>>>> out) throws Exception {
        //satisfaction list lengths need to be the same for implementations of joins, but they change for quantifier
        //operators. For the existential quantifier operator, the input free-variables array has length n+1 and the
        //output will have length n
        if(!value.isPresent()){
            out.collect(value);
        }else{
            List<Optional<Object>> satList = value.get();
            satList.remove(0);
            Optional<List<Optional<Object>>> output = Optional.of(satList);
            out.collect(output);
            //check if this implementation of flatMap is correct --> by running something
        }
    }

}