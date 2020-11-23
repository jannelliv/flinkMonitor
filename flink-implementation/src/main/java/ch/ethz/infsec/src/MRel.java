package ch.ethz.infsec.src;

import ch.ethz.infsec.monitor.Fact;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;


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
        Assignment none = Assignment.nones(2);
        //is it important what value I put as the argument of nones above?
        out.collect(new PipelineEvent(value.getTimestamp(), value.getTimepoint(), true, none));

    }

}