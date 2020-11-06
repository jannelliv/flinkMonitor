package ch.ethz.infsec.src;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Optional;

public class MNext implements Mformula<List<Optional<Object>>> {
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
    public <T> DataStream<List<Optional<Object>>> accept(MformulaVisitor<T> v) {
        return (DataStream<List<Optional<Object>>>) v.visit(this);
        //Is it ok that I did the cast here above?
    }


    @Override
    public void flatMap(List<Optional<Object>> value, Collector<List<Optional<Object>>> out) throws Exception {

    }

    @Override
    public void flatMap1(List<Optional<Object>> optionals, Collector<List<Optional<Object>>> collector) throws Exception {
        //probably won't need this
    }

    @Override
    public void flatMap2(List<Optional<Object>> optionals, Collector<List<Optional<Object>>> collector) throws Exception {
        //probably won't need this
    }
}
