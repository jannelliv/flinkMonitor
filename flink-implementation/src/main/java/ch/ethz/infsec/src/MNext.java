package ch.ethz.infsec.src;

import ch.ethz.infsec.policy.Interval;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import java.util.*;

public class MNext implements Mformula, FlatMapFunction<PipelineEvent, PipelineEvent> {
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
    public <T> DataStream<PipelineEvent> accept(MformulaVisitor<T> v) {
        return (DataStream<PipelineEvent>) v.visit(this);
        //Is it ok that I did the cast here above?
    }


    @Override
    public void flatMap(PipelineEvent value, Collector<PipelineEvent> out) throws Exception {

    }

    public static Triple<List<Table>, List<Table>, List<Integer>> mprev_next(Interval i, List<Table> xs,
                                                                             List<Integer> ts){
        if(xs.size() == 0) {
            List<Table> fstResult = new ArrayList<>();
            List<Table> sndResult = new ArrayList<>();
            return new Triple(fstResult,sndResult, ts);
        }else if(ts.size() == 0) {
            List<Table> fstResult = new ArrayList<>();
            List<Integer> thrdResult = new ArrayList<>();
            return new Triple(fstResult,xs, thrdResult);
        }else if(ts.size() == 1) {
            List<Table> fstResult = new ArrayList<>();
            List<Integer> thrdResult = new ArrayList<>();
            thrdResult.add(ts.get(0));
            return new Triple(fstResult,xs, thrdResult);
        }else if(xs.size() >= 1 && ts.size() >= 2) {
            Integer t = ts.remove(0);
            Integer tp = ts.get(0);
            Table x = xs.remove(0);
            Triple<List<Table>, List<Table>, List<Integer>> yszs = mprev_next(i, xs, ts);
            //above, should return a triple, not a tuple --> problem with verimon
            List<Table> fst = yszs.fst;
            List<Table> snd = yszs.snd;
            List<Integer> thr = yszs.thrd;
            if(mem(tp - t, i)) {
                fst.add(0, x);
            }else{
                Table empty_table = new Table();
                fst.add(0, empty_table);
            }
            return new Triple<>(fst, snd, thr);
        }
        return null;

    }

    public static boolean mem(int n, Interval I){
        if(I.lower() <= n && (!I.upper().isDefined() || (I.upper().isDefined() && n <= ((int) I.upper().get())))){
            return true;
        }
        return false;
    }

}
