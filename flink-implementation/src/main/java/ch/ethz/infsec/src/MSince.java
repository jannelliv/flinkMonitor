package ch.ethz.infsec.src;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.*;

public class MSince implements Mformula, CoFlatMapFunction<Optional<List<Optional<Object>>>, Optional<List<Optional<Object>>>, Optional<List<Optional<Object>>>> {

    boolean bool;
    Mformula formula1;
    ch.ethz.infsec.policy.Interval interval;
    Mformula formula2;
    Tuple<LinkedList<HashSet<Optional<LinkedList<Optional<Object>>>>>, LinkedList<HashSet<Optional<LinkedList<Optional<Object>>>>>> mbuf2;
    List<Integer> tsList;
    LinkedList<Triple<Integer, HashSet<Optional<LinkedList<Optional<Object>>>>, HashSet<Optional<LinkedList<Optional<Object>>>>>> muaux;


    public MSince(boolean b, Mformula accept, ch.ethz.infsec.policy.Interval interval, Mformula accept1,
                  LinkedList<Integer> integers, LinkedList<Triple<Integer,
            HashSet<Optional<LinkedList<Optional<Object>>>>, HashSet<Optional<LinkedList<Optional<Object>>>>>> triples) {
        this.bool = b;
        this.formula1 = accept;
        this.formula2 = accept1;
        this.interval = interval;

        this.tsList = integers;
        this.muaux = triples;

        Optional<Object> el = Optional.empty();
        LinkedList<Optional<Object>> listEl = new LinkedList<>();
        listEl.add(el);
        Optional<LinkedList<Optional<Object>>> el1 = Optional.of(listEl);
        HashSet<Optional<LinkedList<Optional<Object>>>> setEl = new HashSet<>();
        setEl.add(el1);
        LinkedList<HashSet<Optional<LinkedList<Optional<Object>>>>> fst = new LinkedList<>();
        LinkedList<HashSet<Optional<LinkedList<Optional<Object>>>>> snd = new LinkedList<>();
        fst.add(setEl);
        snd.add(setEl);
        this.mbuf2 = new Tuple<>(fst, snd);
    }


    @Override
    public <T> DataStream<Optional<List<Optional<Object>>>> accept(MformulaVisitor<T> v) {
        return (DataStream<Optional<List<Optional<Object>>>>) v.visit(this);
        //Is it ok that I did the cast here above?
    }


    @Override
    public void flatMap1(Optional<List<Optional<Object>>> optionals, Collector<Optional<List<Optional<Object>>>> collector) throws Exception {

    }

    @Override
    public void flatMap2(Optional<List<Optional<Object>>> optionals, Collector<Optional<List<Optional<Object>>>> collector) throws Exception {

    }
}
