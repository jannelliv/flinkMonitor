package ch.ethz.infsec.monitor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import ch.ethz.infsec.util.*;
import ch.ethz.infsec.monitor.visitor.*;


public class MRel implements Mformula, FlatMapFunction<Fact, PipelineEvent> {
    Table table;

    public MRel(Table table){
        this.table = table;
    }

    @Override
    public <T> DataStream<PipelineEvent> accept(MformulaVisitor<T> v) {
        return (DataStream<PipelineEvent>) v.visit(this);
    }


    @Override
    public void flatMap(Fact value, Collector<PipelineEvent> out) throws Exception {
        //The stream of Terminators coming from Test.java should contain only Terminators
        assert(value.isTerminator());
        //no matter what i put above the size of none is always zero because freeVariablesInOrder has size zero for T&F
        for(Assignment as : table){
            out.collect(PipelineEvent.event(value.getTimestamp(), value.getTimepoint(), as));
        }
        out.collect(PipelineEvent.terminator(value.getTimestamp(), value.getTimepoint()));
    }

}