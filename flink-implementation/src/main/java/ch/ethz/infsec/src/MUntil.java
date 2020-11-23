package ch.ethz.infsec.src;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.*;


public class MUntil implements Mformula, CoFlatMapFunction<PipelineEvent, PipelineEvent, PipelineEvent> {

    boolean bool;
    Mformula formula1;
    ch.ethz.infsec.policy.Interval interval;
    Mformula formula2;
    Tuple<List<Set<Optional<Assignment>>>, List<Set<Optional<Assignment>>>> mbuf2;
    List<Integer> tsList;
    LinkedList<Triple<Integer, HashSet<Optional<Assignment>>, HashSet<Optional<Assignment>>>> muaux;



    public MUntil(boolean b, Mformula accept, ch.ethz.infsec.policy.Interval interval,
                  Mformula accept1) {
        this.bool = b;
        this.formula1 = accept;
        this.formula2 = accept1;
        this.interval = interval;
        this.tsList = new LinkedList<Integer>();
        this.muaux = new LinkedList<Triple<Integer, HashSet<Optional<Assignment>>, HashSet<Optional<Assignment>>>>();

        Optional<Object> el = Optional.empty();
        Assignment listEl = new Assignment();
        listEl.add(el);

        Optional<Assignment> el1 = Optional.of(listEl);
        HashSet<Optional<Assignment>> setEl = new HashSet<>();
        setEl.add(el1);
        LinkedList<HashSet<Optional<Assignment>>> fst = new LinkedList<>();
        LinkedList<HashSet<Optional<Assignment>>> snd = new LinkedList<>();
        fst.add(setEl);
        snd.add(setEl);
        this.mbuf2 = new Tuple(fst, snd);

    }



    @Override
    public <T> DataStream<PipelineEvent> accept(MformulaVisitor<T> v) {
        return (DataStream<PipelineEvent>) v.visit(this);
        //Is it ok that I did the cast here above?
    }



    @Override
    public void flatMap1(PipelineEvent optionals, Collector<PipelineEvent> collector) throws Exception {

    }

    @Override
    public void flatMap2(PipelineEvent optionals, Collector<PipelineEvent> collector) throws Exception {

    }
}
