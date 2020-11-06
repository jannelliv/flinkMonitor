package ch.ethz.infsec.src;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;


public class MPrev implements Mformula<List<Optional<Object>>> {
    ch.ethz.infsec.policy.Interval interval;
    Mformula formula;
    boolean bool;
    LinkedList<LinkedList<LinkedList<Optional<Object>>>> tableList;
    LinkedList<Integer> tsList;

    public MPrev(ch.ethz.infsec.policy.Interval interval, Mformula mform, boolean bool, LinkedList<Integer> tsList) {
        this.interval = interval;
        this.formula = mform;
        this.bool = bool;
        this.tsList = tsList;

        Optional<Object> el = Optional.empty();
        LinkedList<Optional<Object>> listEl = new LinkedList<>();
        listEl.add(el);
        LinkedList<LinkedList<Optional<Object>>> listEl2 = new LinkedList<>();
        listEl2.add(listEl);
        LinkedList<LinkedList<LinkedList<Optional<Object>>>> listEl3 = new LinkedList<>();
        listEl3.add(listEl2);
        this.tableList = listEl3;

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

    }

    @Override
    public void flatMap2(List<Optional<Object>> optionals, Collector<List<Optional<Object>>> collector) throws Exception {

    }
}