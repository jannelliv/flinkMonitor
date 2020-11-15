package ch.ethz.infsec.src;

import ch.ethz.infsec.policy.Interval;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import java.util.*;

public class MNext implements Mformula, FlatMapFunction<Optional<Assignment>, Optional<Assignment>> {
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
    public <T> DataStream<Optional<Assignment>> accept(MformulaVisitor<T> v) {
        return (DataStream<Optional<Assignment>>) v.visit(this);
        //Is it ok that I did the cast here above?
    }


    @Override
    public void flatMap(Optional<Assignment> value, Collector<Optional<Assignment>> out) throws Exception {

    }

    public static Triple<List<Set<Optional<Assignment>>>, List<Set<Optional<Assignment>>>, List<Integer>> mprev_next(Interval i,
                                                                                                                                             List<Set<Optional<Assignment>>> xs,
                                                                                                                                             List<Integer> ts){
        if(xs.size() == 0) {
            List<Set<Optional<Assignment>>> fstResult = new ArrayList<>();
            List<Set<Optional<Assignment>>> sndResult = new ArrayList<>();
            return new Triple(fstResult,sndResult, ts);
        }else if(ts.size() == 0) {
            List<Set<Optional<Assignment>>> fstResult = new ArrayList<>();
            List<Integer> thrdResult = new ArrayList<>();
            return new Triple(fstResult,xs, thrdResult);
        }else if(ts.size() == 1) {
            List<Set<Optional<Assignment>>> fstResult = new ArrayList<>();
            List<Integer> thrdResult = new ArrayList<>();
            thrdResult.add(ts.get(0));
            return new Triple(fstResult,xs, thrdResult);
        }else if(xs.size() >= 1 && ts.size() >= 2) {
            Integer t = ts.remove(0);
            Integer tp = ts.get(0);
            Set<Optional<Assignment>> x = xs.remove(0);
            Triple<List<Set<Optional<Assignment>>>,
                    List<Set<Optional<Assignment>>>, List<Integer>> yszs = mprev_next(i, xs, ts);
            //above, should return a triple, not a tuple --> problem with verimon
            List<Set<Optional<Assignment>>> fst = yszs.fst;
            List<Set<Optional<Assignment>>> snd = yszs.snd;
            List<Integer> thr = yszs.thrd;
            if(mem(tp - t, i)) {
                fst.add(0, x);
            }else{
                Set<Optional<Assignment>> empty_table = new HashSet<>();
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
