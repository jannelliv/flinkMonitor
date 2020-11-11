package ch.ethz.infsec.src;

import ch.ethz.infsec.monitor.Fact;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import java.util.*;

public class MRel implements Mformula, FlatMapFunction<Fact, Optional<List<Optional<Object>>>> {
    HashSet<Optional<LinkedList<Optional<Object>>>> table;

    public MRel(HashSet<Optional<LinkedList<Optional<Object>>>> table){

        this.table = table;
    }

    @Override
    public <T> DataStream<Optional<List<Optional<Object>>>> accept(MformulaVisitor<T> v) {
        return (DataStream<Optional<List<Optional<Object>>>>) v.visit(this);

    }


    @Override
    public void flatMap(Fact value, Collector<Optional<List<Optional<Object>>>> out) throws Exception {
        //The stream of Terminators coming from Test.java should contain only Terminators
        assert(value.isTerminator());
        Optional<List<Optional<Object>>> none = Optional.empty();
        out.collect(none);

    }

}