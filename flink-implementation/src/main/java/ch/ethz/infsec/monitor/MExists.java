package ch.ethz.infsec.monitor;
import ch.ethz.infsec.policy.VariableID;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import java.util.Optional;
import ch.ethz.infsec.util.*;
import ch.ethz.infsec.monitor.visitor.*;

public class MExists implements Mformula, FlatMapFunction<PipelineEvent, PipelineEvent> {

    public Mformula subFormula;
    VariableID var;


    public MExists(Mformula subformula, VariableID var){
        //actually the second argument is not necessary
        //You use the first argument in the second visitor
        this.subFormula = subformula;
        this.var = var;

    }

    @Override
    public <T> DataStream<PipelineEvent> accept(MformulaVisitor<T> v) {
        return (DataStream<PipelineEvent>) v.visit(this);
        //Is it ok that I did the cast here above?
    }


    @Override
    public void flatMap(PipelineEvent value, Collector<PipelineEvent> out) throws Exception {
        //satisfaction list lengths need to be the same for implementations of joins, but they change for quantifier
        //operators. For the existential quantifier operator, the input free-variables array has length n+1 and the
        //output will have length n
        if(!value.isPresent()){
            out.collect(value);
        }else{
            Assignment satList = value.get();
            satList.remove(0); //here we are taking the front of the list
            Optional<Assignment> output = Optional.of(satList);
            if(output.isPresent()){
                //previously, I had omitted this if loop
                PipelineEvent result = PipelineEvent.event(value.getTimestamp(),value.getTimepoint(), output.get());
                out.collect(result);
            }
        }
    }

}