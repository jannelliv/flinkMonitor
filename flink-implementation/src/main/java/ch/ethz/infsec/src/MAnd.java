package ch.ethz.infsec.src;
import java.util.*;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;


public class MAnd implements Mformula<List<Optional<Object>>>{
    boolean bool;
    Mformula op1;
    Mformula op2;
    Tuple<LinkedList<HashSet<LinkedList<Optional<Object>>>>, LinkedList<HashSet<LinkedList<Optional<Object>>>>> mbuf2;


    public MAnd(Mformula arg1, boolean bool, Mformula arg2) {
        this.bool = bool;
        this.op1 = arg1;
        this.op2 = arg2;
        Optional<Object> el = Optional.empty();
        LinkedList<Optional<Object>> listEl = new LinkedList<>();
        listEl.add(el);
        HashSet<LinkedList<Optional<Object>>> setEl = new HashSet<>();
        setEl.add(listEl);
        LinkedList<HashSet<LinkedList<Optional<Object>>>> fst = new LinkedList<HashSet<LinkedList<Optional<Object>>>>();
        LinkedList<HashSet<LinkedList<Optional<Object>>>> snd = new LinkedList<HashSet<LinkedList<Optional<Object>>>>();
        fst.add(setEl);
        snd.add(setEl);
        this.mbuf2 = new Tuple<>(fst, snd);

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
