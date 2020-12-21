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

    boolean pos;//correct that this indicates whether the first subformula is negated or not?
    public Mformula formula1;
    ch.ethz.infsec.policy.Interval interval;
    public Mformula formula2;
    Tuple<HashMap<Long, Table>, HashMap<Long, Table>> mbuf2;
    //List<Integer> tsList;
    HashMap<Long, Tuple<Table, Table>> muaux;
    //for muaux, is it ok to have a HashMap with a Tuple in the Range, instead of a set of triples?

    Long smallestCompleteTPleft;
    Long smallestCompleteTPright;
    Long smallestFullTimestamp;
    HashMap<Long, Long> timepointToTimestamp;
    HashSet<Long> terminLeft;
    HashSet<Long> terminRight;



    public MUntil(boolean b, Mformula accept, ch.ethz.infsec.policy.Interval interval,
                  Mformula accept1) {
        this.pos = b;
        this.formula1 = accept;
        this.formula2 = accept1;
        this.interval = interval;
        //this.tsList = new LinkedList<Integer>();
        this.muaux = new HashMap<>();


        this.timepointToTimestamp = new HashMap<>();
        this.smallestCompleteTPleft = -1L;
        this.smallestCompleteTPright = -1L;
        this.smallestFullTimestamp = -1L;
        HashMap<Long, Table> fst = new HashMap<>();
        HashMap<Long, Table> snd = new HashMap<>();
        this.mbuf2 = new Tuple<>(fst, snd);
        this.terminLeft = new HashSet<>();
        this.terminRight = new HashSet<>();

    }



    @Override
    public <T> DataStream<PipelineEvent> accept(MformulaVisitor<T> v) {
        return (DataStream<PipelineEvent>) v.visit(this);
        //Is it ok that I did the cast here above?
    }



    @Override
    public void flatMap1(PipelineEvent event, Collector<PipelineEvent> collector) throws Exception {
        if(!timepointToTimestamp.containsKey(event.getTimepoint())){
            timepointToTimestamp.put(event.getTimepoint(), event.getTimestamp());
        }
        //NB: I think you don't actually need tsList
        //but here we are adding the timestamp to it; even if we still have to sort
        //tsList.add(event.getTimestamp());
        //filling mbuf2 appropriately:
        if(mbuf2.fst().containsKey(event.getTimepoint())){
            mbuf2.fst().get(event.getTimepoint()).add(event.get());
        }else{
            mbuf2.fst().put(event.getTimepoint(), Table.one(event.get()));
        }
        //filling muaux appropriately:
        if(muaux.containsKey(event.getTimestamp())){
            //muaux declaration: HashMap<Long, Tuple<Table, Table>> muaux
            muaux.get(event.getTimestamp()).fst().add(event.get());
        }else{
            //does this make sense?
            muaux.put(event.getTimestamp(), new Tuple<>(Table.one(event.get()), Table.empty()));
        }
        if(!event.isPresent()){ //if we have a terminator
            if(!terminLeft.contains(event.getTimepoint())){
                terminLeft.add(event.getTimepoint());
            }else{
                //error!
                //Should I throw an Exception here?
            }
            if(terminLeft.contains(smallestCompleteTPleft + 1L)){
                smallestCompleteTPleft++;
            }
            if(smallestCompleteTPright.equals(smallestCompleteTPleft)){
                //initiate meval procedure:
                Mbuf2take_function_Until func = (Long timepoint,
                                           HashMap<Long, Table> rel1,
                                           HashMap<Long, Table> rel2) -> {return update_until(timepoint,rel1, rel2);};

                //LinkedList<Long> tsList_arg = new LinkedList<>(tsList);
                mbuf2t_take(func);
                //update_until accepts a timepoint, but then within the method, we retrieve the timestamp
                //and work with that! On the other hand, eval_until accepts a timestamp because it only works
                //with timestamps and never with timepoints.
                //NB: you still have to check that mbuf2t_take works correctly!!

                Tuple<HashMap<Long, Table>,HashMap<Long, Tuple<Table, Table>>> evalUntilResult;
                if(muaux.keySet().size() == 0){
                    evalUntilResult = eval_until(event.getTimestamp());

                }else{
                    evalUntilResult = eval_until(smallestFullTimestamp);
                }
                HashMap<Long, Table> zs = evalUntilResult.fst();
                for(Long timestamp : zs.keySet()){
                    //not sure if keySet() of zs consists of timestamp or timepoints
                    //This problem is also present in the body of eval_until
                    Table evalSet = zs.get(timestamp);
                    for(Assignment oa : evalSet){
                        collector.collect(new PipelineEvent(timepointToTimestamp.get(event.getTimepoint()),event.getTimepoint(), false, oa));
                    }
                }
                //at the end, we output the terminator! --> is it ok that the assignment is the input assignment?
                collector.collect(event);
            }
        }
    }

    @Override
    public void flatMap2(PipelineEvent event, Collector<PipelineEvent> collector) throws Exception {
        if(!timepointToTimestamp.containsKey(event.getTimepoint())){
            timepointToTimestamp.put(event.getTimepoint(), event.getTimestamp());
        }
        //NB: I think you don't actually need tsList
        //but here we are adding the timestamp to it; even if we still have to sort
        //tsList.add(event.getTimestamp());
        //filling mbuf2 appropriately:
        if(mbuf2.snd().containsKey(event.getTimepoint())){
            mbuf2.snd().get(event.getTimepoint()).add(event.get());
        }else{
            mbuf2.snd().put(event.getTimepoint(), Table.one(event.get()));
        }
        //filling muaux appropriately:
        if(muaux.containsKey(event.getTimestamp())){
            //muaux declaration: HashMap<Long, Tuple<Table, Table>> muaux
            muaux.get(event.getTimestamp()).snd().add(event.get());
        }else{
            //does this make sense?
            muaux.put(event.getTimestamp(), new Tuple<>(Table.empty(), Table.one(event.get())));
        }

        if(!event.isPresent()){ //if we have a terminator
            if(!terminRight.contains(event.getTimepoint())){
                terminRight.add(event.getTimepoint());
            }else{
                //error!
                //Should I throw an Exception here?
            }
            if(terminRight.contains(smallestCompleteTPright + 1L)){
                smallestCompleteTPright++;
            }
            if(smallestCompleteTPright.equals(smallestCompleteTPleft)){
                //initiate meval procedure:
                Mbuf2take_function_Until func = (Long timepoint,
                                                 HashMap<Long, Table> rel1,
                                                 HashMap<Long, Table> rel2) -> {return update_until(timepoint,rel1, rel2);};

                //LinkedList<Long> tsList_arg = new LinkedList<>(tsList);
                mbuf2t_take(func);

                //Tuple<HashMap<Long, Table>,HashMap<Long, Tuple<Table, Table>>> eval_until(long currentTimestamp)
                Tuple<HashMap<Long, Table>,HashMap<Long, Tuple<Table, Table>>> evalUntilResult;
                if(muaux.keySet().size() == 0){
                    evalUntilResult = eval_until(event.getTimestamp());
                }else{
                    evalUntilResult = eval_until(smallestFullTimestamp);
                }

                HashMap<Long, Table> zs = evalUntilResult.fst();
                for(Long timestamp : zs.keySet()){
                    //not sure if keySet() of zs consists of timestamp or timepoints
                    //This problem is also present in the body of eval_until
                    Table evalSet = zs.get(timestamp);
                    for(Assignment oa : evalSet){
                        collector.collect(new PipelineEvent(timepointToTimestamp.get(event.getTimepoint()),event.getTimepoint(), false, oa));
                    }
                }
                //at the end, we output the terminator! --> is it ok that the assignment is the input assignment?
                collector.collect(event);
            }
        }
    }

    public HashMap<Long, Tuple<Table, Table>> update_until(Long timepoint, HashMap<Long, Table> rel1, HashMap<Long, Table> rel2){
        //in Verimon, nt is the current timestamp!
        HashMap<Long, Tuple<Table, Table>> result = new HashMap<>();
        Long currentTimestamp = timepointToTimestamp.get(timepoint);
        for(Long timestamp : muaux.keySet()){
            Tuple<Table, Table> tables = muaux.get(timestamp);
            Table a2UnionAfter;
            Table firstTable;
            //first table:
            if(this.pos){
                firstTable = join(tables.fst(), true, rel1.get(timepoint));
            }else{
                firstTable = Table.fromTable(tables.fst());
                //do Java sets have bag sematics?
                firstTable.addAll(rel1.get(timepoint));
            }
            //second table:
            if(mem(currentTimestamp - timestamp, interval)){
                Table rel2a1Join = join(rel2.get(timepoint), pos, tables.fst());
                a2UnionAfter = Table.fromTable(tables.snd());
                a2UnionAfter.addAll(rel2a1Join);
            }
            else{
                a2UnionAfter = tables.snd();
            }
            result.put(currentTimestamp, new Tuple<>(firstTable, a2UnionAfter));
        }

        Table addedTableForZeroLHS;
        if(interval.lower() == 0L){
            //interval.lower should be a long, actually, not an int.
            addedTableForZeroLHS = rel2.get(timepoint);
        }else{
            addedTableForZeroLHS = Table.empty();
        }
        result.put(currentTimestamp, new Tuple<>(rel1.get(timepoint), addedTableForZeroLHS));
        return result;
    }

    public Tuple<HashMap<Long, Table>,
            HashMap<Long, Tuple<Table, Table>>> eval_until(long currentTimestamp){
        //Translation to Verimon code on afp:
        //current timestamp: nt
        //smallest full timestamp: t

        if(muaux.isEmpty()){
            return new Tuple<>(new HashMap<>(), new HashMap<>());
        }else{
            Tuple<Table, Table> a1a2 = muaux.get(smallestFullTimestamp);
            if(smallestFullTimestamp + (Long)interval.upper().get() < currentTimestamp){
                Tuple<HashMap<Long, Table>,
                        HashMap<Long, Tuple<Table, Table>>> xsAux = eval_until(currentTimestamp);
                //xsAux.fst().put(currentTimestamp, a1a2.snd());
                //xsAux.fst().put(smallestFullTimestamp, a1a2.snd());
                xsAux.fst().put(smallestFullTimestamp, a1a2.snd());
                //Not sure which of the two above possibilities I should use!
                return xsAux;
            }else{
                return new Tuple<>(new HashMap<>(), muaux);
            }
        }
    }


    public void mbuf2t_take(Mbuf2take_function_Until func){

        if(mbuf2.fst().size() >= 1 && mbuf2.snd().size() >= 1){ //I don't think checking these lengths is necessary
            Table x = mbuf2.fst().get(smallestCompleteTPleft);
            Table y = mbuf2.snd().get(smallestCompleteTPleft);
            mbuf2.fst().remove(smallestCompleteTPleft, mbuf2.fst().get(smallestCompleteTPleft));
            //does it make sense to remove these entries here?
            mbuf2.snd().remove(smallestCompleteTPleft, mbuf2.snd().get(smallestCompleteTPleft));
            //Long t = tsList.remove(0);
            HashMap<Long, Tuple<Table, Table>> fxytz = func.run(smallestCompleteTPleft, mbuf2.fst(),mbuf2.snd());
            //is it ok to pass smallestCompleteTPleft in the above method call?
            muaux = fxytz; //not 100% sure about this! --> does this change muaux?
            mbuf2t_take(func);
        }
        else{
            //cannot work with mbuf2 if one of the two subformulas corresponds to an empty mbuf2...
            //return null;
        }
    }

    public static boolean mem(long n, Interval I){
        //Is it a bad thing that I copy the code to different classes?
        return I.lower() <= n && (!I.upper().isDefined() || (I.upper().isDefined() && n <= ((long) I.upper().get())));
    }

    public static Optional<Assignment> join1(Assignment a, Assignment b){

        if(a.size() == 0 && b.size() == 0) {
            Assignment emptyList = new Assignment();
            return Optional.of(emptyList);
        }else {
            Optional<Object> x = a.remove(0);
            Optional<Object> y = b.remove(0);
            Optional<Assignment> subResult = join1(a, b);
            if(!x.isPresent() && !y.isPresent()) {

                if(!subResult.isPresent()) {
                    return Optional.empty();
                }else {
                    Assignment consList = new Assignment();
                    consList.add(Optional.empty());
                    consList.addAll(subResult.get());
                    //Problem: get() can only return a value if the wrapped object is not null;
                    //otherwise, it throws a no such element exception
                    return Optional.of(consList);
                }
            }else if(x.isPresent() && !y.isPresent()) {


                if(!subResult.isPresent()) {
                    return Optional.empty();
                }else {
                    Assignment consList = new Assignment();
                    consList.add(x);
                    consList.addAll(subResult.get());
                    return Optional.of(consList);
                }
            }else if(!x.isPresent() && y.isPresent()) {

                if(!subResult.isPresent()) {
                    return Optional.empty();
                }else {
                    Assignment consList = new Assignment();
                    consList.add(y);
                    consList.addAll(subResult.get());
                    return Optional.of(consList);
                }
            }else if(x.isPresent() && y.isPresent() || x!=y) {
                if(!subResult.isPresent()) {
                    return Optional.empty();
                }else {
                    if(x==y) {
                        Assignment consList = new Assignment();
                        consList.add(x);
                        consList.addAll(subResult.get());
                        return Optional.of(consList);
                    }
                }
            }else {
                return Optional.empty();
            }
        }
        return Optional.empty(); //not sure why this is necessary

    }

    public static Table join(java.util.HashSet<Assignment> table, boolean pos, java.util.HashSet<Assignment> table2){

        java.util.HashSet<Assignment> result = new java.util.HashSet<>();
        Iterator<Assignment> it = table.iterator();

        while(it.hasNext()) {
            for (Assignment optionals : table2) {
                Optional<Assignment> tupleRes = join1(it.next(), optionals);
                if (tupleRes.isPresent()) {
                    Assignment tuple = tupleRes.get();
                    result.add(tuple);
                }
            }
        }
        if(pos) {
            return Table.fromSet(result);
        }else {
            //we don't have to do anything here right?
            table.removeAll(result);
            return Table.fromSet(table);

        }

    }
}

interface Mbuf2take_function_Until{
    HashMap<Long, Tuple<Table, Table>> run(Long timepoint,
                                           HashMap<Long, Table> rel1,
                                           HashMap<Long, Table> rel2);
}

