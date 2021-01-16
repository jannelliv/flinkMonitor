package ch.ethz.infsec.monitor;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

import ch.ethz.infsec.util.*;
import ch.ethz.infsec.monitor.visitor.*;
import java.util.*;

public class MSince implements Mformula, CoFlatMapFunction<PipelineEvent, PipelineEvent, PipelineEvent> {

    boolean pos; //flag indicating whether left subformula is positive (non-negated)
    public Mformula formula1; //left subformula
    ch.ethz.infsec.policy.Interval interval;
    public Mformula formula2; //right subformula
    Tuple<HashMap<Long, Table>, HashMap<Long, Table>> mbuf2; //"buf" in Verimon

    //List<Long> tsList; //"nts" in Verimon
    HashMap<Long, Table> msaux;//"aux" in Verimon
    //for every timestamp, lists the satisfying assignments!

    Long largestInOrderTPsub1;
    Long largestInOrderTPsub2;
    HashSet<Long> terminLeft;
    HashSet<Long> terminRight;

    HashMap<Long, Long> timepointToTimestamp;

    Long startEvalTimepoint;


    public MSince(boolean b, Mformula accept, ch.ethz.infsec.policy.Interval interval, Mformula accept1) {
        this.pos = b;
        this.formula1 = accept;
        this.formula2 = accept1;
        this.interval = interval;

        this.msaux = new HashMap<>();
        this.timepointToTimestamp = new HashMap<>();
        this.largestInOrderTPsub1 = -1L;
        this.largestInOrderTPsub2 = -1L;
        this.startEvalTimepoint = 0L;
        HashMap<Long, Table> fst = new HashMap<>();
        HashMap<Long, Table> snd = new HashMap<>();
        this.mbuf2 = new Tuple<>(fst, snd);
        this.terminLeft = new HashSet<>();
        this.terminRight = new HashSet<>();
    }


    @Override
    public <T> DataStream<PipelineEvent> accept(MformulaVisitor<T> v) {
        return (DataStream<PipelineEvent>) v.visit(this);
    }

    @Override
    public void flatMap2(PipelineEvent event, Collector<PipelineEvent> collector) throws Exception {

        if(!timepointToTimestamp.containsKey(event.getTimepoint())){
            timepointToTimestamp.put(event.getTimepoint(), event.getTimestamp());
        }
        if(event.isPresent()){
            if(mbuf2.snd().containsKey(event.getTimepoint())){
                mbuf2.snd().get(event.getTimepoint()).add(event.get());
            }else{
                mbuf2.snd().put(event.getTimepoint(), Table.one(event.get()));
            }
            if(msaux.containsKey(event.getTimestamp())){
                msaux.get(event.getTimestamp()).add(event.get());
            }else{
                msaux.put(event.getTimestamp(), Table.one(event.get()));
            }
        }else{ //if we have a terminator
            if(!terminRight.contains(event.getTimepoint())){
                terminRight.add(event.getTimepoint());
            }else{
                throw new Exception("Not possible to receive two terminators for the same timepoint.");
            }
            while(terminRight.contains(largestInOrderTPsub2 + 1L)){
                largestInOrderTPsub2++;
            }
            if(largestInOrderTPsub2 >= startEvalTimepoint && largestInOrderTPsub1>= startEvalTimepoint &&
                    !(largestInOrderTPsub2 == -1 || largestInOrderTPsub1 == -1)){
                Mbuf2take_function func = (Table r1,
                                           Table r2,
                                           Long t, HashMap<Long, Table> zsAux) -> {Table us_result = update_since(r1, r2, t);
                    HashMap<Long, Table> intermRes = new HashMap<>(zsAux);intermRes.put(t, us_result);
                    return intermRes;};
                HashMap<Long, Table> msaux_zs = mbuf2t_take(func, new HashMap<>(), event.getTimepoint()); //WAS:event.getTimepoint()
                for(Long tp : msaux_zs.keySet()){
                    Table evalSet = msaux_zs.get(tp);
                    for(Assignment oa : evalSet){
                        if(oa != null && oa.size() != 0){
                            collector.collect(PipelineEvent.event(timepointToTimestamp.get(tp), tp, oa));
                        }
                    }
                    //at the end, we output the terminator! --> for each of the timepoints in zs. See line below:
                    collector.collect(PipelineEvent.terminator(timepointToTimestamp.get(tp), tp));
                    startEvalTimepoint += msaux_zs.size();
                }

            }
        }

    }

    @Override
    public void flatMap1(PipelineEvent event, Collector<PipelineEvent> collector) throws Exception {
        if(!timepointToTimestamp.containsKey(event.getTimepoint())){
            timepointToTimestamp.put(event.getTimepoint(), event.getTimestamp());
        }

        if(event.isPresent()){
            if(mbuf2.fst().containsKey(event.getTimepoint())){
                mbuf2.fst().get(event.getTimepoint()).add(event.get());
            }else{
                mbuf2.fst().put(event.getTimepoint(), Table.one(event.get()));
            }
            if(msaux.containsKey(event.getTimestamp())){
                msaux.get(event.getTimestamp()).add(event.get());
            }else{
                msaux.put(event.getTimestamp(), Table.one(event.get()));
            }
        }else{ //if we have a terminator
            if(!terminLeft.contains(event.getTimepoint())){
                terminLeft.add(event.getTimepoint());
            }else{
                throw new Exception("Not possible to receive two terminators for the same timepoint.");
            }
            while(terminLeft.contains(largestInOrderTPsub1 + 1L)){
                largestInOrderTPsub1++;
            }
            if(largestInOrderTPsub2 >= startEvalTimepoint && largestInOrderTPsub1>= startEvalTimepoint &&
                    !(largestInOrderTPsub2 == -1 || largestInOrderTPsub1 == -1)){
                Mbuf2take_function func = (Table r1,
                                           Table r2,
                                           Long t, HashMap<Long, Table> zsAux) -> {Table us_result = update_since(r1, r2, t);
                    HashMap<Long, Table> intermRes = new HashMap<>(zsAux); intermRes.put(t, us_result);
                    return intermRes;};

                HashMap<Long, Table> msaux_zs = mbuf2t_take(func,new HashMap<>(), event.getTimepoint()); //WAS: event.getTimepoint()
                for(Long tp : msaux_zs.keySet()){
                    Table evalSet = msaux_zs.get(tp);
                    for(Assignment oa : evalSet){
                        if(oa.size() != 0){
                            collector.collect(PipelineEvent.event(timepointToTimestamp.get(tp), tp, oa));
                        }
                    }
                    //at the end, we output the terminator! --> for each of the timepoints in zs. See line below:
                    collector.collect(PipelineEvent.terminator(timepointToTimestamp.get(tp), tp));
                    startEvalTimepoint += msaux_zs.size();
                }

            }
        }
    }

    public Table update_since(Table rel1, Table rel2, Long nt){
        //In the return type, I actually don't need a tuple because in the flink implementation the state is kept by the
        ///java class.
        //nt is a timestamp!!!
        HashMap<Long, Table> auxResult = new HashMap<>();
        HashMap<Long, Table> auxIntervalList = new HashMap<>();

        for(Long t : msaux.keySet()){
            Table rel = msaux.get(t);
            Long subtr = nt - t;
            if(!interval.upper().isDefined() || (interval.upper().isDefined() && (subtr.intValue() <= ((int) interval.upper().get())))){
                auxIntervalList.put(t, join(rel, pos, rel1));
            }
        }
        HashMap<Long, Table> auxIntervalList2 = new HashMap<>(auxIntervalList);
        if(auxIntervalList.size() == 0){
            //msaux.put(nt, rel2);
            auxResult.put(nt, rel2);
            auxIntervalList2.put(nt,rel2);
        }else{
            Table x = auxIntervalList.get(timepointToTimestamp.get(startEvalTimepoint));
            if(timepointToTimestamp.get(startEvalTimepoint).equals(nt)){
                Table unionSet = Table.fromTable(x);
                unionSet.addAll(rel2);
                auxIntervalList2.put(timepointToTimestamp.get(startEvalTimepoint), unionSet);
                //auxResult = new HashMap<>(auxIntervalList2);
            }else{
                auxIntervalList2.put(timepointToTimestamp.get(startEvalTimepoint),x);
                auxIntervalList2.put(nt, rel2);
                //auxResult = new HashMap<>(auxIntervalList2);
            }
            //update_since I pos rel1 rel2 nt aux =
            /*(let aux = (case [(t, join rel pos rel1). (t, rel) ← aux, nt - t ≤ right I] of
            [] ⇒ [(nt, rel2)]
            | x # aux'' ⇒ (if fst x = nt then (fst x, snd x ∪ rel2) # aux'' else (nt, rel2) # x # aux''))
                in (foldr (∪) [rel. (t, rel) ← aux, left I ≤ nt - t] {}, aux))*/

            /*meval n t db (MSince pos φ I ψ buf nts aux) =
            (let (xs, φ) = meval n t db φ; (ys, ψ) = meval n t db ψ;
            ((zs, aux), buf, nts) = mbuf2t_take (λr1 r2 t (zs, aux).
                            let (z, aux) = update_since I pos r1 r2 t aux
                    in (zs @ [z], aux)) ([], aux) (mbuf2_add xs ys buf) (nts @ [t])
            in (zs, MSince pos φ I ψ buf nts aux))*/
        }

        Table bigUnion = new Table();
        //It also computes the satisfactions by taking the union over all tables in the list that satisfy c element of I.
        for(Long t : auxIntervalList2.keySet()){
            Table rel = auxIntervalList2.get(t);
            if(nt - t >= interval.lower()){
                bigUnion.addAll(rel);
            }
        }
        msaux = new HashMap<>(auxIntervalList2);
        return bigUnion;
    }


    public HashMap<Long, Table> mbuf2t_take(Mbuf2take_function func,
                                            HashMap<Long, Table> z,
                                            Long currentTimepoint){
        if(mbuf2.fst().containsKey(currentTimepoint) && mbuf2.snd().containsKey(currentTimepoint) &&
                terminLeft.contains(currentTimepoint) && terminRight.contains(currentTimepoint)){
            Table x = mbuf2.fst().get(currentTimepoint);
            Table y = mbuf2.snd().get(currentTimepoint);
            //mbuf2.fst().remove(currentTimepoint, mbuf2.fst().get(currentTimepoint));
            //mbuf2.snd().remove(currentTimepoint, mbuf2.snd().get(currentTimepoint));
            HashMap<Long, Table> mbuf2t_output = func.run(x,y,timepointToTimestamp.get(currentTimepoint),z);
            currentTimepoint++; //double check!
            return mbuf2t_take(func, mbuf2t_output, currentTimepoint);
        }
        else{
            return z;
        }
    }


    public static Optional<Assignment> join1(Assignment aOriginal, Assignment bOriginal){
        Assignment a = Assignment.someAssignment(aOriginal);
        Assignment b = Assignment.someAssignment(bOriginal);
        if(a.size() == 0 && b.size() == 0) {
            Assignment emptyList = new Assignment();
            Optional<Assignment> result = Optional.of(emptyList);
            return result;
        }else if(a.size() == 0 || b.size() == 0){
            Optional<Assignment> result = Optional.empty();
            return result;
        }else {
            Optional<Object> x = a.remove(0);
            Optional<Object> y = b.remove(0);
            Optional<Assignment> subResult = join1(a, b);
            if(!x.isPresent() && !y.isPresent()) {
                if(!subResult.isPresent()) {
                    Optional<Assignment> result = Optional.empty();
                    return result;
                }else {
                    Assignment consList = new Assignment();
                    consList.add(Optional.empty());
                    consList.addAll(subResult.get());
                    //Problem: get() can only return a value if the wrapped object is not null;
                    //otherwise, it throws a no such element exception
                    Optional<Assignment> result = Optional.of(consList);
                    return result;
                }
            }else if(x.isPresent() && !y.isPresent()) {
                if(!subResult.isPresent()) {
                    Optional<Assignment> result = Optional.empty();
                    return result;
                }else {
                    Assignment consList = new Assignment();
                    consList.add(x);
                    consList.addAll(subResult.get());
                    Optional<Assignment> result = Optional.of(consList);
                    return result;
                }
            }else if(!x.isPresent() && y.isPresent()) {
                if(!subResult.isPresent()) {
                    Optional<Assignment> result = Optional.empty();
                    return result;
                }else {
                    Assignment consList = new Assignment();
                    consList.add(y);
                    consList.addAll(subResult.get());
                    Optional<Assignment> result = Optional.of(consList);
                    return result;
                }
            }else if(x.isPresent() && y.isPresent() || x.get().equals(y.get())) {
                if(!subResult.isPresent()) {
                    Optional<Assignment> result = Optional.empty();
                    return result;
                }else {
                    if(x.get().equals(y.get())) {
                        Assignment consList = new Assignment();
                        consList.add(x);
                        consList.addAll(subResult.get());
                        Optional<Assignment> result = Optional.of(consList);
                        return result;
                    }
                }
            }else {
                Optional<Assignment> result = Optional.empty();
                return result;
            }
        }

        Optional<Assignment> result = Optional.empty();
        return result;
    }



    public static Table join(java.util.HashSet<Assignment> table, boolean pos, java.util.HashSet<Assignment> table2){
        if(table.isEmpty() || table2.isEmpty()){
            return new Table();
        }

        java.util.HashSet<Assignment> result = new java.util.HashSet<>();

        for(Assignment op1 : table) {
            for (Assignment optional2 : table2) {
                Optional<Assignment> tupleRes = join1(op1, optional2);
                if (tupleRes.isPresent()) {
                    Assignment tuple = tupleRes.get();
                    result.add(tuple);
                }
            }
        }
        if(pos) {
            return Table.fromSet(result);
        }else {
            table.removeAll(result);
            return Table.fromSet(table);

        }

    }
    public void cleanUpDatastructures(){
        mbuf2.fst.keySet().removeIf(tp -> tp < largestInOrderTPsub1);
        mbuf2.snd.keySet().removeIf(tp -> tp < largestInOrderTPsub2);
        if(timepointToTimestamp.containsKey(startEvalTimepoint)){
            msaux.keySet().removeIf(ts -> ts < timepointToTimestamp.get(startEvalTimepoint));
        }
        timepointToTimestamp.keySet().removeIf(tp -> tp < startEvalTimepoint);
    }

}

interface Mbuf2take_function{
    HashMap<Long, Table> run(Table t1,
                             Table t2,
                             Long ts, HashMap<Long, Table> zs);
}


