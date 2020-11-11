package ch.ethz.infsec.src;
import java.util.*;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;
import static ch.ethz.infsec.src.Table.join1;


public class MAnd implements Mformula, CoFlatMapFunction<Optional<List<Optional<Object>>>, Optional<List<Optional<Object>>>, Optional<List<Optional<Object>>>> {
    boolean bool;
    Mformula op1;
    Mformula op2;
    Tuple<LinkedList<HashSet<Optional<List<Optional<Object>>>>>, LinkedList<HashSet<Optional<List<Optional<Object>>>>>> mbuf2;
    boolean terminatorLHS;
    boolean terminatorRHS;
    Integer indexlhs, indexrhs;



    public MAnd(Mformula arg1, boolean bool, Mformula arg2) {
        this.bool = bool;
        this.op1 = arg1;
        this.op2 = arg2;
        Optional<Object> el = Optional.empty();
        List<Optional<Object>> listEl = new LinkedList<>();
        listEl.add(el);
        HashSet<Optional<List<Optional<Object>>>> setEl = new HashSet<>();
        Optional<List<Optional<Object>>> el1 = Optional.of(listEl);
        setEl.add(el1);
        LinkedList<HashSet<Optional<List<Optional<Object>>>>> fst = new LinkedList<>();
        LinkedList<HashSet<Optional<List<Optional<Object>>>>> snd = new LinkedList<>();
        fst.add(setEl);
        snd.add(setEl);
        this.mbuf2 = new Tuple<>(fst, snd);
        terminatorLHS = false;
        terminatorRHS = false;
        indexlhs = 0;
        indexrhs = 0;

    }
    @Override
    public <T> DataStream<Optional<List<Optional<Object>>>> accept(MformulaVisitor<T> v) {
        return (DataStream<Optional<List<Optional<Object>>>>) v.visit(this);
        //Is it ok that I did the cast here above?
    }

    @Override
    public void flatMap1(Optional<List<Optional<Object>>> fact, Collector<Optional<List<Optional<Object>>>> collector) throws Exception {
        if(!fact.isPresent()){
            terminatorLHS = true;
            indexlhs++;
            if(terminatorRHS){
                this.mbuf2.fst.remove(0);
                this.mbuf2.snd.remove(0);
                collector.collect(fact);
                terminatorRHS = false;
                terminatorLHS = false;
            }
        }else if(!terminatorLHS){
            if(this.mbuf2.fst.size() == 0){
                this.mbuf2.fst.add(new HashSet<>());

            }
            this.mbuf2.fst.get(0).add(fact);
            for(Optional<List<Optional<Object>>> rhs : this.mbuf2.snd.get(0)){
                Optional<List<Optional<Object>>> joinResult = join1(fact, rhs);
                collector.collect(joinResult);
            }
        }else{
            if(this.mbuf2.fst.size() < indexlhs + 1){
                this.mbuf2.fst.add(new HashSet<>());

            }
            this.mbuf2.fst.get(indexlhs).add(fact);
            if(this.mbuf2.snd.get(indexlhs) != null){
                for(Optional<List<Optional<Object>>> rhs : this.mbuf2.snd.get(indexlhs)){
                    Optional<List<Optional<Object>>> joinResult = join1(fact, rhs);
                    collector.collect(joinResult);
                }
            }
        }
    }

    @Override
    public void flatMap2(Optional<List<Optional<Object>>> fact, Collector<Optional<List<Optional<Object>>>> collector) throws Exception {
        //one terminator fact has to be sent out once it is received on both incoming streams!!
        if(!fact.isPresent()){
            terminatorRHS = true;
            indexrhs++;
            if(terminatorLHS){
                this.mbuf2.fst.remove(0);
                this.mbuf2.snd.remove(0);
                //does the way I structured the sets in the linkedlist make sense?
                collector.collect(fact);
                terminatorRHS = false;
                terminatorLHS = false;
            }
        }else if(!terminatorRHS){
            if(this.mbuf2.snd.size() == 0){
                this.mbuf2.snd.add(new HashSet<>());
            }
            this.mbuf2.snd.get(0).add(fact);
            for(Optional<List<Optional<Object>>> lhs : this.mbuf2.fst.get(0)){
                Optional<List<Optional<Object>>> joinResult = join1(lhs, fact);
                collector.collect(joinResult);
            }
        }else{
            if(this.mbuf2.snd.size() < indexrhs + 1){
                //to make sure that there is something to get() here
                this.mbuf2.snd.add(new HashSet<>());
                //YOU CAN ALSO GET MORE THAN 1 TERMINATOR ON ONE SIDE!!
            }
            this.mbuf2.snd.get(indexrhs).add(fact);
            if(this.mbuf2.fst.get(indexrhs) != null){
                for(Optional<List<Optional<Object>>> lhs : this.mbuf2.fst.get(indexrhs)){
                    Optional<List<Optional<Object>>> joinResult = join1(lhs, fact);
                    collector.collect(joinResult);
                }
            }
        }
    }
}
