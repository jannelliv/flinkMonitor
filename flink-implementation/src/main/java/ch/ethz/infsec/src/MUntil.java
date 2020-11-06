package ch.ethz.infsec.src;

import ch.ethz.infsec.policy.Interval;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import java.util.*;


public class MUntil implements Mformula<List<Optional<Object>>> {

    boolean bool;
    Mformula formula1;
    ch.ethz.infsec.policy.Interval interval;
    Mformula formula2;
    Tuple<List<Set<List<Optional<Object>>>>, List<Set<List<Optional<Object>>>>> mbuf2;
    List<Integer> tsList;
    LinkedList<Triple<Integer, HashSet<LinkedList<Optional<Object>>>, HashSet<LinkedList<Optional<Object>>>>> muaux;



    public MUntil(boolean b, Mformula accept, ch.ethz.infsec.policy.Interval interval, Mformula accept1, LinkedList<Integer> integers, LinkedList<Triple<Integer, HashSet<LinkedList<Optional<Object>>>, HashSet<LinkedList<Optional<Object>>>>> triples) {
        this.bool = b;
        this.formula1 = accept;
        this.formula2 = accept1;
        this.interval = interval;
        this.tsList = integers;
        this.muaux = triples;

        Optional<Object> el = Optional.empty();
        LinkedList<Optional<Object>> listEl = new LinkedList<>();
        listEl.add(el);
        HashSet<LinkedList<Optional<Object>>> setEl = new HashSet<>();
        setEl.add(listEl);
        LinkedList<HashSet<LinkedList<Optional<Object>>>> fst = new LinkedList<>();
        LinkedList<HashSet<LinkedList<Optional<Object>>>> snd = new LinkedList<>();
        fst.add(setEl);
        snd.add(setEl);
        this.mbuf2 = new Tuple(fst, snd);

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
