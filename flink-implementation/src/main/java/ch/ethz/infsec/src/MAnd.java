package ch.ethz.infsec.src;
import java.util.*;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;


public class MAnd implements Mformula<List<Optional<Object>>> {
    boolean bool;
    Mformula op1;
    Mformula op2;
    Tuple<List<Set<List<Optional<Object>>>>, List<Set<List<Optional<Object>>>>> mbuf2;


    public MAnd(Mformula arg1, boolean bool, Mformula arg2, Tuple<List<Set<List<Optional<Object>>>>, List<Set<List<Optional<Object>>>>> tuple) {
        this.bool = bool;
        this.op1 = arg1;
        this.op2 = arg2;
        this.mbuf2 = tuple;
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
