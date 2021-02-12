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
            if(!mbuf2.snd().containsKey(event.getTimepoint())){
                mbuf2.snd().put(event.getTimepoint(), Table.empty());
            }
            if(!msaux.containsKey(event.getTimestamp())){
                //not 100% sure
                msaux.put(event.getTimestamp(), Table.empty());
            }
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
                                           Long t, HashMap<Long, Table> zsAux) -> {Table us_result = update_since(r1, r2, timepointToTimestamp.get(t));
                    HashMap<Long, Table> intermRes = new HashMap<>(zsAux);intermRes.put(t, us_result);
                    return intermRes;};
                HashMap<Long, Table> msaux_zs = mbuf2t_take(func, new HashMap<>(), startEvalTimepoint); //WAS:event.getTimepoint()
                Long outResultTP = startEvalTimepoint;
                while(msaux_zs.containsKey(outResultTP)){
                    Table evalSet = msaux_zs.get(outResultTP);
                    for(Assignment oa : evalSet){

                        collector.collect(PipelineEvent.event(timepointToTimestamp.get(outResultTP), outResultTP, oa));

                    }
                    //at the end, we output the terminator! --> for each of the timepoints in zs. See line below:
                    collector.collect(PipelineEvent.terminator(timepointToTimestamp.get(outResultTP), outResultTP));
                    outResultTP++; //not sure we should increment by 1 here
                }
                startEvalTimepoint += msaux_zs.size();

            }
        }
        cleanUpDatastructures();
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
            if(!mbuf2.fst().containsKey(event.getTimepoint())){
                mbuf2.fst().put(event.getTimepoint(), Table.empty());
            }
            if(!msaux.containsKey(event.getTimestamp())){
                //not 100% sure
                msaux.put(event.getTimestamp(), Table.empty());
            }
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
                                           Long t, HashMap<Long, Table> zsAux) -> {Table us_result = update_since(r1, r2, timepointToTimestamp.get(t));
                    HashMap<Long, Table> intermRes = new HashMap<>(zsAux); intermRes.put(t, us_result);
                    return intermRes;};

                HashMap<Long, Table> msaux_zs = mbuf2t_take(func,new HashMap<>(), startEvalTimepoint);

                Long outResultTP = startEvalTimepoint;
                while(msaux_zs.containsKey(outResultTP)){
                    Table evalSet = msaux_zs.get(outResultTP);
                    for(Assignment oa : evalSet){

                        collector.collect(PipelineEvent.event(timepointToTimestamp.get(outResultTP), outResultTP, oa));

                    }
                    //at the end, we output the terminator! --> for each of the timepoints in zs. See line below:
                    collector.collect(PipelineEvent.terminator(timepointToTimestamp.get(outResultTP), outResultTP));
                    outResultTP++;
                }
                startEvalTimepoint += msaux_zs.size();
            }
        }
        cleanUpDatastructures();
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
                terminLeft.contains(currentTimepoint) && terminRight.contains(currentTimepoint)){ //check timestamps!!!
            Table x = mbuf2.fst().get(currentTimepoint);
            Table y = mbuf2.snd().get(currentTimepoint);
            HashMap<Long, Table> mbuf2t_output = func.run(x,y,currentTimepoint,z);
            currentTimepoint++; //double check!
            return mbuf2t_take(func, mbuf2t_output, currentTimepoint);
        }
        else{
            return z;
        }
    }

    public static Optional<Assignment> join1(Assignment a, Assignment b, int i){
        //Assignment a = Assignment.someAssignment(aOriginal);
        //Assignment b = Assignment.someAssignment(bOriginal);
        if(a.size() == 0 && b.size() == 0) {
            Assignment emptyList = new Assignment();
            Optional<Assignment> result = Optional.of(emptyList);
            return result;
        }else if(a.size() == 0 || b.size() == 0){
            Optional<Assignment> result = Optional.empty();
            return result;
        }else {
            if( i < a.size() && i < b.size()){
                Optional<Object> x = a.get(i);
                Optional<Object> y = b.get(i);
                Optional<Assignment> subResult = join1(a, b, i+1);
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
                    //is it ok to do things with toString here above?
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
            }else{
                if(a.size() != b.size()){
                    Optional<Assignment> result = Optional.empty();
                    return result;
                }else{
                    return Optional.of(new Assignment()); //not 100% sure about this
                }
            }

        }

        Optional<Assignment> result = Optional.empty();
        return result;
    }




    public static Table join(java.util.HashSet<Assignment> table, boolean pos, java.util.HashSet<Assignment> table2){

        java.util.HashSet<Assignment> result = new java.util.HashSet<>();
        for(Assignment op1 : table) {
            for (Assignment optional2 : table2) {
                Optional<Assignment> tupleRes = join1(op1, optional2, 0);
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
        mbuf2.fst.keySet().removeIf(tp -> tp < startEvalTimepoint);
        mbuf2.snd.keySet().removeIf(tp -> tp < startEvalTimepoint);
        /*if(timepointToTimestamp.containsKey(startEvalTimepoint)){
            msaux.keySet().removeIf(ts -> ts < timepointToTimestamp.get(startEvalTimepoint));
        }*/
        timepointToTimestamp.keySet().removeIf(tp -> tp < startEvalTimepoint);
    }

}

interface Mbuf2take_function{
    HashMap<Long, Table> run(Table t1,
                             Table t2,
                             Long ts, HashMap<Long, Table> zs);
}


