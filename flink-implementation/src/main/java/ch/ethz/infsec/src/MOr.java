package ch.ethz.infsec.src;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public class MOr implements Mformula<List<Optional<Object>>> {

    Mformula op1;
    Mformula op2;
    Tuple<List<Set<List<Optional<Object>>>>, List<Set<List<Optional<Object>>>>> mbuf2;


    public MOr(Mformula accept, Mformula accept1, Tuple tuple) {
        this.op1 = accept;
        this.op2 = accept1;
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

