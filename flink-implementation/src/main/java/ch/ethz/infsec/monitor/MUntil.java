package ch.ethz.infsec.monitor;
import ch.ethz.infsec.policy.Interval;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.*;

import ch.ethz.infsec.util.*;
import ch.ethz.infsec.monitor.visitor.*;
import scala.collection.mutable.HashSet;


public class MUntil implements Mformula, CoFlatMapFunction<PipelineEvent, PipelineEvent, PipelineEvent> {



    boolean pos;//indicates whether the first subformula is negated or not
    public Mformula formula1;
    ch.ethz.infsec.policy.Interval interval;
    public Mformula formula2;
    Tuple<HashMap<Long, Table>, HashMap<Long, Table>> mbuf2;
    HashMap<Long, Tuple<Table, Table>> muaux;
    //for muaux, contrary to the Verimon impl, we have a HashMap with a Tuple in the Range, instead of a set of triples

    Long largestInOrderTPsub1;
    Long largestInOrderTPsub2;

    Long startEvalTimepoint; //this gives us the timepoint from which to retrieve the timestamp and corresponding entries
    //in mbuf2 and muaux, in order to process these datastructures from the last entry which was not evaluated, and hence
    //was also not "taken" from the buffer.
    //At the end of the flatMap functions, remember to remove the necessary entries from timepointToTimestamp. Otw there
    //may be a memory leak/stack overflow.

    Long smallestFullTimestamp;
    HashMap<Long, Long> timepointToTimestamp;
    HashSet<Long> terminSub1;
    HashSet<Long> terminSub2;

    public MUntil(boolean b, Mformula accept, ch.ethz.infsec.policy.Interval interval,
                  Mformula accept1) {
        this.pos = b;
        this.formula1 = accept;
        this.formula2 = accept1;
        this.interval = interval;
        this.muaux = new HashMap<>();

        this.timepointToTimestamp = new HashMap<>();
        this.largestInOrderTPsub1 = -1L;
        this.largestInOrderTPsub2 = -1L;
        this.smallestFullTimestamp = -1L;
        this.startEvalTimepoint = 0L; //not sure if this should be 0 or -1
        HashMap<Long, Table> fst = new HashMap<>();
        HashMap<Long, Table> snd = new HashMap<>();
        this.mbuf2 = new Tuple<>(fst, snd);
        this.terminSub1 = new HashSet<>();
        this.terminSub2 = new HashSet<>();

    }

    @Override
    public <T> DataStream<PipelineEvent> accept(MformulaVisitor<T> v) {
        return (DataStream<PipelineEvent>) v.visit(this);
        //Is it ok that I did the cast here above?
    }

    @Override
    public void flatMap1(PipelineEvent event, Collector<PipelineEvent> collector) throws Exception {
        //for implementing MUntil there are many options, we chose to go with the simplest one which is to implement
        //it the way it's implemented in Verimon, i.e. buffer everything.
        if(!timepointToTimestamp.containsKey(event.getTimepoint())){
            timepointToTimestamp.put(event.getTimepoint(), event.getTimestamp());
        }
        //NB: I think I don't actually need tsList,
        //because we store the timestamps in msaux, the timepoints in mbuf2, and we have
        //a specific datastructure which maps timepoints to timestamps
        //we still have to sort filling mbuf2 appropriately:
        if(mbuf2.fst().containsKey(event.getTimepoint())){
            mbuf2.fst().get(event.getTimepoint()).add(event.get());
        }else{
            mbuf2.fst().put(event.getTimepoint(), Table.one(event.get()));
        }
        //filling muaux appropriately:
        if(muaux.containsKey(event.getTimestamp())){
            muaux.get(event.getTimestamp()).fst().add(event.get());
        }else{
            muaux.put(event.getTimestamp(), new Tuple<>(Table.one(event.get()), Table.empty()));
        }
        if(!event.isPresent()){ //if we have a terminator
            if(!terminSub1.contains(event.getTimepoint())){
                terminSub1.add(event.getTimepoint());
            }else{
                throw new Exception("Not possible to receive two terminators for the same timepoint.");
            }
            while(terminSub1.contains(largestInOrderTPsub1 + 1L)){
                largestInOrderTPsub1++;
            }

            if(largestInOrderTPsub2 >= largestInOrderTPsub1){
                if(largestInOrderTPsub1 > -1){
                    smallestFullTimestamp = timepointToTimestamp.get(startEvalTimepoint);
                }else{
                    smallestFullTimestamp = -1L;
                }
            }else{
                if(largestInOrderTPsub2 > -1){
                    smallestFullTimestamp = timepointToTimestamp.get(startEvalTimepoint);
                }else{
                    smallestFullTimestamp = -1L;
                }
            }
            if(largestInOrderTPsub2 >= startEvalTimepoint && largestInOrderTPsub1>= startEvalTimepoint){
                //initiate meval procedure:
                Mbuf2take_function_Until func = this::update_until;
                mbuf2t_take(func);
                //update_until accepts a timepoint, but then within the method, we retrieve the timestamp
                //and work with that! On the other hand, eval_until accepts a timestamp because it only works
                //with timestamps and never with timepoints.

                HashMap<Long, Table> evalUntilResult;

                if(timepointToTimestamp.containsKey(largestInOrderTPsub1 + 1L)){
                    //to write about this in the thesis, see the schemes made during the meeting
                    evalUntilResult = eval_until(smallestFullTimestamp + 1L);
                }else if(timepointToTimestamp.containsKey(largestInOrderTPsub1)){
                    evalUntilResult = eval_until(smallestFullTimestamp);
                }else{
                    throw new Exception("The mapping from tp to ts should contain at least one of these entries.");
                }
                HashMap<Long, Table> muaux_zs = evalUntilResult;
                for(Long timestamp : muaux_zs.keySet()){ //I'm not starting from "startEvalTimepoint"
                    Table evalSet = muaux_zs.get(timestamp);
                    for(Assignment oa : evalSet){
                        if(event.getTimepoint() != -1 && oa.size() !=0){
                            collector.collect(new PipelineEvent(timestamp, event.getTimepoint(), false, oa));
                        }else{
                            throw new Exception("problem retrieving timepoint");
                        }
                    }
                    //at the end, we output the terminator! --> for each of the timepoints in zs. See line below:
                    if(event.getTimepoint() != -1) {
                        collector.collect(new PipelineEvent(timestamp, event.getTimepoint(), true, Assignment.nones(event.get().size())));
                        //not sure about the nones(), and the number of free variables
                    }else{
                        throw new Exception("problem retrieving timepoint");
                    }
                }
            }
            if(largestInOrderTPsub1 <= largestInOrderTPsub2){
                startEvalTimepoint = largestInOrderTPsub1 +1;
            }else{
                startEvalTimepoint = largestInOrderTPsub2 +1; //not sure about the +1
            }
            //TODO: remove parts from the data-structures in the fields in order to avoid a memory leak.
        }
    }

    @Override
    public void flatMap2(PipelineEvent event, Collector<PipelineEvent> collector) throws Exception {
        if(!timepointToTimestamp.containsKey(event.getTimepoint())){
            timepointToTimestamp.put(event.getTimepoint(), event.getTimestamp());
        }
        //filling mbuf2 appropriately:
        if(mbuf2.snd().containsKey(event.getTimepoint())){
            mbuf2.snd().get(event.getTimepoint()).add(event.get());
        }else{
            mbuf2.snd().put(event.getTimepoint(), Table.one(event.get()));
        }
        //filling muaux appropriately:
        if(muaux.containsKey(event.getTimestamp())){
            muaux.get(event.getTimestamp()).snd().add(event.get());
        }else{
            muaux.put(event.getTimestamp(), new Tuple<>(Table.empty(), Table.one(event.get())));
        }

        if(!event.isPresent()){ //if we have a terminator
            if(!terminSub2.contains(event.getTimepoint())){
                terminSub2.add(event.getTimepoint());
            }else{
                throw new Exception("Not possible to receive two terminators for the same timepoint.");
            }
            while(terminSub2.contains(largestInOrderTPsub2 + 1L)){
                largestInOrderTPsub2++;
            }
            if(largestInOrderTPsub2 >= largestInOrderTPsub1){
                if(largestInOrderTPsub1 > -1){
                    smallestFullTimestamp = timepointToTimestamp.get(startEvalTimepoint);
                }else{
                    smallestFullTimestamp = -1L;
                }
            }else{
                if(largestInOrderTPsub2 > -1){
                    smallestFullTimestamp = timepointToTimestamp.get(startEvalTimepoint);
                }else{
                    smallestFullTimestamp = -1L;
                }
            }
            if(largestInOrderTPsub2 >= startEvalTimepoint && largestInOrderTPsub1>= startEvalTimepoint){
                //initiate meval procedure:
                Mbuf2take_function_Until func = this::update_until;
                mbuf2t_take(func);
                HashMap<Long, Table> evalUntilResult;

                if(muaux.keySet().size() == 0){
                    evalUntilResult = eval_until(event.getTimestamp());
                }else{
                    assert(muaux.containsKey(smallestFullTimestamp));
                    evalUntilResult = eval_until(smallestFullTimestamp);
                }

                HashMap<Long, Table> muaux_zs = evalUntilResult;
                for(Long timestamp : muaux_zs.keySet()){
                    Table evalSet = muaux_zs.get(timestamp);
                    for(Assignment oa : evalSet){
                        if(event.getTimepoint() != -1L && oa.size() != 0){
                            collector.collect(new PipelineEvent(timestamp, event.getTimepoint(), false, oa));
                        }/*else{
                            throw new Exception("problem retrieving timepoint");
                        }*/
                    }
                    //at the end, we output the terminator! --> for each of the timepoints in zs???
                    /*if(event.getTimepoint() != -1L){
                        collector.collect(new PipelineEvent(timestamp, event.getTimepoint(), true, Assignment.nones(event.get().size())));
                        //not sure about the nones(), and the number of free variables
                    }else{
                        throw new Exception("problem retrieving timepoint");
                    }*/
                }
            }
            //I have an issue understanding when to output terminators, so I am currently outputting them when I get them.
            if(terminSub1.contains(event.getTimepoint()) && terminSub2.contains(event.getTimepoint())){
                collector.collect(event);
            }
            if(largestInOrderTPsub1 <= largestInOrderTPsub2){
                startEvalTimepoint = largestInOrderTPsub1 +1; //not sure about the +1
            }else{
                startEvalTimepoint = largestInOrderTPsub2  +1;
            }
        }
    }

    public void update_until(Long timepoint, HashMap<Long, Table> rel1, HashMap<Long, Table> rel2){
        //in Verimon, nt is the current timestamp
        //this method updates muaux, based on the timestamp

        assert(timepointToTimestamp.keySet().contains(timepoint));
        Long currentTimestamp = timepointToTimestamp.get(timepoint);
        for(Long timestamp : muaux.keySet()){
            Tuple<Table, Table> tables = muaux.get(timestamp);
            Table a2UnionAfter;
            Table firstTable;
            //first table:
            if(this.pos){
                assert(rel1.containsKey(timepoint));
                firstTable = join(tables.fst(), true, rel1.get(timepoint));
            }else{
                firstTable = Table.fromTable(tables.fst());
                assert(rel1.containsKey(timepoint));
                firstTable.addAll(rel1.get(timepoint));
            }
            //second table: timestamp and currentTimestamp can have the same value
            if(mem(currentTimestamp - timestamp, interval)){
                assert(rel2.containsKey(timepoint));
                Table rel2a1Join = join(rel2.get(timepoint), pos, tables.fst());
                a2UnionAfter = Table.fromTable(tables.snd());
                a2UnionAfter.addAll(rel2a1Join);
            }
            else{
                a2UnionAfter = tables.snd();
            }
            muaux.put(timestamp, new Tuple<>(firstTable, a2UnionAfter)); //first I put it in currentTimestamp, which was wrong
        }
        Table addedTableForZeroLHS;
        if(interval.lower() == 0L){
            addedTableForZeroLHS = rel2.get(timepoint);
        }else{
            addedTableForZeroLHS = Table.empty();
        } //below: not sure if it's correct to just "put" a new tuple here --> not the same as "@" in Isabelle!
        muaux.put(currentTimestamp, new Tuple<>(rel1.get(timepoint), addedTableForZeroLHS));
    }

    public HashMap<Long, Table> eval_until(long currentTimestamp){
        //In the Verimon code on afp it holds that:
        //current timestamp: nt;   smallest full timestamp: t
        if(muaux.isEmpty()){
            return new HashMap<>();
        }else{
            assert(muaux.containsKey(smallestFullTimestamp));
            Tuple<Table, Table> a1a2 = muaux.get(smallestFullTimestamp);
            if(interval.upper().isDefined() && smallestFullTimestamp.intValue() + (int)interval.upper().get() < currentTimestamp){
                HashMap<Long, Table> xs = eval_until(currentTimestamp);
                //xsAux.fst().put(currentTimestamp, a1a2.snd());
                //xsAux.fst().put(smallestFullTimestamp, a1a2.snd());
                xs.put(smallestFullTimestamp, a1a2.snd());
                //Not sure which of the two above possibilities I should use!
                return xs;
            }else if(smallestFullTimestamp == currentTimestamp){
                HashMap<Long, Table> xs = new HashMap<>();
                xs.put(smallestFullTimestamp, a1a2.snd());
                return xs;
            }else{
                return new HashMap<>();
            }
        }
    }


    public void mbuf2t_take(Mbuf2take_function_Until func){
        //do nothing with mbuf2 if one of the two subformulas corresponds to an empty mbuf2 --> skip if condition
        if(mbuf2.fst().size() >= 1 && mbuf2.snd().size() >= 1 && muaux.keySet().size() >= 1){ //I don't think checking these lengths is necessary
            //Table x = mbuf2.fst().get(startEvalTimepoint); //x and y are not used! :(
            //Table y = mbuf2.snd().get(startEvalTimepoint);
            //Ok to erase above two lines because the tables x and y are used to update muaux in update_until(), from which they
            //are retrieved directly.

            func.run(startEvalTimepoint, mbuf2.fst(),mbuf2.snd()); //you could also put the function in here directly!
            //the below two lines used to be before the function call
            mbuf2.fst().remove(startEvalTimepoint);
            mbuf2.snd().remove(startEvalTimepoint);
            startEvalTimepoint++; //gets incremented and method is called recursively until one of the two HashMaps in
            //mbuf2 becomes empty.
            mbuf2t_take(func);
        }
    }

    public static boolean mem(Long n, Interval I){
        //not sure of I should use the method isDefined or isEmpty below
        //and I am not sure if it's ok to do the cast (int)I.upper().get()
        if(I.lower() <= n.intValue() && (!I.upper().isDefined() || (I.upper().isDefined() && n.intValue() <= ((int)I.upper().get())))){
            return true;
        }
        return false;
    }

    public static Optional<Assignment> join1(Assignment a, Assignment b){

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
            }else if(x.isPresent() && y.isPresent() || x.get().toString() != y.get().toString()) {
                //is it ok to do things with toString here above?
                if(!subResult.isPresent()) {
                    Optional<Assignment> result = Optional.empty();
                    return result;
                }else {
                    if(x.get().toString() ==y.get().toString()) { //should enter this clause automatically
                        //is it ok to do things with toString here above?
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

        java.util.HashSet<Assignment> result = new java.util.HashSet<>();
        assert(table != null && table2 != null);
        for(Assignment op1 : table){
            for (Assignment optionals : table2) {
                Optional<Assignment> tupleRes = join1(op1, optionals);
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
}

interface Mbuf2take_function_Until{
    void run(Long timepoint, HashMap<Long, Table> rel1, HashMap<Long, Table> rel2);
}

