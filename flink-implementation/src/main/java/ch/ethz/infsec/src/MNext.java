package ch.ethz.infsec.src;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Optional;

public class MNext implements Mformula, FlatMapFunction<Optional<List<Optional<Object>>>, Optional<List<Optional<Object>>>> {
    ch.ethz.infsec.policy.Interval interval;
    Mformula formula;
    boolean bool;
    List<Integer> tsList;

    public MNext(ch.ethz.infsec.policy.Interval interval, Mformula mform, boolean bool, List<Integer> tsList) {
        this.interval = interval;
        this.formula = mform;
        this.bool = bool;
        this.tsList = tsList;
    }

    @Override
    public <T> DataStream<Optional<List<Optional<Object>>>> accept(MformulaVisitor<T> v) {
        return (DataStream<Optional<List<Optional<Object>>>>) v.visit(this);
        //Is it ok that I did the cast here above?
    }


    @Override
    public void flatMap(Optional<List<Optional<Object>>> value, Collector<Optional<List<Optional<Object>>>> out) throws Exception {

    }

}
