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

    public MPrev(ch.ethz.infsec.policy.Interval interval, Mformula mform, boolean bool, LinkedList<Integer> tsList,
                 LinkedList<LinkedList<LinkedList<Optional<Object>>>> tableList) {
        this.interval = interval;
        this.formula = mform;
        this.bool = bool;
        this.tableList = tableList;
        this.tsList = tsList;
    }

    @Override
    public <T> DataStream<List<Optional<Object>>> accept(MformulaVisitor<T> v) {
        return null;
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