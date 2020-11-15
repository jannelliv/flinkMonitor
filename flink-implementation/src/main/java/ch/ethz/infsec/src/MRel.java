package ch.ethz.infsec.src;

import ch.ethz.infsec.monitor.Fact;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import java.util.*;

public class MRel implements Mformula, FlatMapFunction<Fact, Optional<Assignment>> {
    Table table;

    public MRel(Table table){

        this.table = table;
    }

    @Override
    public <T> DataStream<Optional<Assignment>> accept(MformulaVisitor<T> v) {
        return (DataStream<Optional<Assignment>>) v.visit(this);

    }


    @Override
    public void flatMap(Fact value, Collector<Optional<Assignment>> out) throws Exception {
        //The stream of Terminators coming from Test.java should contain only Terminators
        assert(value.isTerminator());
        Optional<Assignment> none = Optional.empty();
        out.collect(none);

    }

}