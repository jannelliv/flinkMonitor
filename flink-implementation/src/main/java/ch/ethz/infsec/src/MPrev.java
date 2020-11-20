package ch.ethz.infsec.src;
import ch.ethz.infsec.policy.Interval;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import java.util.*;


public class MPrev implements Mformula, FlatMapFunction<Optional<Assignment>, Optional<Assignment>> {
    ch.ethz.infsec.policy.Interval interval;
    Mformula formula;
    boolean bool;
    ArrayList<Table> tableList; //buf --> Verimon
    LinkedList<Integer> tsList; //nts --> name used in Verimon

    public MPrev(ch.ethz.infsec.policy.Interval interval, Mformula mform, boolean bool, LinkedList<Integer> tsList) {
        this.interval = interval;
        this.formula = mform;
        this.bool = bool;
        this.tsList = tsList;

        //Optional<Object> el = Optional.empty();
        //Assignment listEl = new Assignment();
        //listEl.add(el); Do I have to add an optional empty here?
        //Optional<Assignment> el1 = Optional.of(listEl);
        //LinkedList<Optional<Assignment>> listEl2 = new LinkedList<>();
        //listEl2.add(el1);
        ArrayList<Table> listEl3 = new ArrayList<>();
        //listEl3.add(listEl2);
        this.tableList = listEl3;

    }

    @Override
    public <T> DataStream<Optional<Assignment>> accept(MformulaVisitor<T> v) {
        return (DataStream<Optional<Assignment>>) v.visit(this);
    }


    @Override
    public void flatMap(Optional<Assignment> value, Collector<Optional<Assignment>> out) throws Exception {
        if(value.isPresent()){
            List<Table> bufXS = new ArrayList<>();
            //a table is a set of assignments
            Table xs = Table.one(value.get()); //NOT SURE IF THIS IS CORRECT
            bufXS.add(0,xs);
            List<Table> bufXsNew = new ArrayList();
            bufXsNew.addAll(this.tableList);
            bufXsNew.addAll(bufXS);
            /////////////////////////////////////
            List<Integer> mprev_nextSecondArgument = new ArrayList<>();
            //HOW DO I KNO WHAT THE TIMESTAMP OF value IS????

            //Triple<List<Table>, List<Table>, List<Integer>> tripleResult = mprev_next(this.interval, bufXsNew, );
        }


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