package ch.ethz.infsec.monitor;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

import ch.ethz.infsec.util.*;
import ch.ethz.infsec.monitor.visitor.*;
import scala.collection.mutable.HashSet;

import java.util.*;

public class MSince implements Mformula, CoFlatMapFunction<PipelineEvent, PipelineEvent, PipelineEvent> {
    //msaux and mbuf2 have datastructures which have to be based on Assignments,
    //not pipelineevents. This is because they don't store terminators. We need an additional structures
    //to link the associated timestamps to the assignments; this is given by msaux.
    //We also need a mechanism to ensure that we handle out of order events and deal with the fact that we
    //are not receiving whole databases (Verimon receives whole databases at once).
    //TO achieve this and to handle out-of-order arrival of pipelineEvents, we can use hashmaps within
    //mbuf2 and msaux in order to associate timepoints (in order) to the correct set of assignments (table)
    //The semantics of mbuf2 in Verimon: two buffers, where you have values for each timePOINT.
    //Mbuf2 is necessary because MSince is a binary operator, so you may get different progress on the
    //two subformulas. So all binary operators have mbuf2. The left buffer is the buffer for subformula1 and
    //the right buffer is the buffer for subformula2. We need to keep both buffers sorted with respect to
    //the timepoint, and the best way to do this is with a HashMap. Operations on mbuf2 in Verimon were
    //efficient because the list was sorted. Using a HashMap is fine if you know what the minimum entry
    //is, i.e. the minimum timestamp. But you need to know what the smallest timepoint is in the
    //domain of the HashMap, such that you can start sorting from that.
    //Beware: you will create NEW timepoints to send up the parsing tree, not the ones that correspond
    //to the assignments in mbuf2! SO we need to start taking from the minimum element upward, in the map.
    //So the HashMaps are processed in order of timepoints in the map. And you don't repeat until you have
    //have exhaused all entries in the HashMap, but rather until you reach an entry which is not full (i.e.
    //which has not yet received a Terminator.
    // We need two datastructuctures for the terminator, one for the terminators to the left and one for
    //the terminators to the right. If you have both terminators for timepoint 3, you don't have both terminators
    //for timepoint 4 and you do have both terminators for timepoint 5, then you should just call the MSince
    //procedure for timepoint 3. You don't output anything for timepoint 5.
    //I retransform the Assignments into PipelineEvents using an additional structure from timepoints to
    //timestamps, which was also used in MPrev and in MNext.

    //Why we use Assignment instead of Optional<Assignment>: If the join used in MAnd resulted in no satisfaction,
    //then an empty Assignment was outputted and passed to operators higher up in the Parsing tree. When the result
    //of the join was empty, it DIDN'T happen that Optionl<Assignment> were outputted, it was Assignments.

    //The length of an Assignment corresponds to the number of free variables that you assign.
    //you don't need the method mbuf2_add, because you store the incoming PipelienEvents directly into mbuf2

    //In this implementation, we will only call mbuf2t_take when we get a terminator.
    //YOu will initiate the "meval MSince procedure" from one of the two flatMaps, specifically from the one from
    //which you received a terminator last.
    //zs in Verimon: a variable to capture the overall output --> It's a list of tables, but you will just be outputting
    //one table at the time (all elements of the table consecutively), and when you are finished outputting
    //the single table, we will also output the corresponding terminator.

    //Observation on Verimon+ paper: See paper pdf


    boolean pos; //flag indicating whether left subformula is positive (non-negated)
    public Mformula formula1; //left subformula
    ch.ethz.infsec.policy.Interval interval;
    public Mformula formula2; //right subformula
    Tuple<HashMap<Long, Table>, HashMap<Long, Table>> mbuf2; //"buf" in Verimon

    //List<Long> tsList; //"nts" in Verimon
    HashMap<Long, Table> msaux;//"aux" in Verimon
    //for every timestamp, lists the satisfying assumptions!

    Long smallestCompleteTPleft;
    Long smallestCompleteTPright;
    Long smallestFullTimestamp;
    HashMap<Long, Long> timepointToTimestamp;

    HashSet<Long> terminLeft;
    HashSet<Long> terminRight;


    public MSince(boolean b, Mformula accept, ch.ethz.infsec.policy.Interval interval, Mformula accept1) {
        this.pos = b;
        this.formula1 = accept;
        this.formula2 = accept1;
        this.interval = interval;

        //this.tsList = new LinkedList<>();
        this.msaux = new HashMap<>();
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
        //filling msaux appropriately:
        if(msaux.containsKey(event.getTimestamp())){
            msaux.get(event.getTimestamp()).add(event.get());
        }else{
            msaux.put(event.getTimestamp(), Table.one(event.get()));
        }
        if(!event.isPresent()){ //if we have a terminator
            if(!terminRight.contains(event.getTimepoint())){
                terminRight.add(event.getTimepoint());
            }else{
                //error!
            }
            if(terminRight.contains(smallestCompleteTPright + 1L)){
                smallestCompleteTPright++;
            }
            if(smallestCompleteTPright.equals(smallestCompleteTPleft)){
                smallestFullTimestamp = timepointToTimestamp.get(smallestCompleteTPleft);
                //initiate meval procedure!!
                Mbuf2take_function func = (Table r1,
                                           Table r2,
                                           Long t, Tuple<HashMap<Long, Table>, HashMap<Long, Table>> zsAux) -> {Tuple<Table,
                        HashMap<Long, Table>> us_result = update_since(r1, r2, t,msaux);
                    HashMap<Long, Table> intermRes = new HashMap<>(zsAux.fst);Tuple<HashMap<Long, Table>,
                            HashMap<Long, Table>> funcRes = new Tuple(intermRes.put(t, us_result.fst()), us_result.snd());
                    return funcRes;};
                //LinkedList<Long> tsList_arg = new LinkedList<>(tsList);
                Tuple<Tuple<HashMap<Long, Table>, HashMap<Long, Table>>,
                        Tuple<HashMap<Long, Table>, HashMap<Long, Table>>> mbuf2t_take_result = mbuf2t_take(func, new Tuple(new HashMap<Long, Table>(), msaux), mbuf2, event.getTimepoint());


                HashMap<Long, Table> zs = mbuf2t_take_result.fst.fst;
                for(int j = 0; j < zs.size(); j++){
                    Table evalSet = zs.get(j);
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
        //filling msaux appropriately:
        if(msaux.containsKey(event.getTimestamp())){
            msaux.get(event.getTimestamp()).add(event.get());
        }else{
            msaux.put(event.getTimestamp(), Table.one(event.get()));
        }
        if(!event.isPresent()){ //if we have a terminator
            if(!terminLeft.contains(event.getTimepoint())){
                terminLeft.add(event.getTimepoint());
            }else{
                //error!
            }
            if(terminLeft.contains(smallestCompleteTPleft + 1L)){
                smallestCompleteTPleft++;
            }
            if(smallestCompleteTPright.equals(smallestCompleteTPleft)){
                //initiate meval procedure:
                Mbuf2take_function func = (Table r1,
                                           Table r2,
                                           Long t, Tuple<HashMap<Long, Table>, HashMap<Long, Table>> zsAux) -> {Tuple<Table,
                        HashMap<Long, Table>> us_result = update_since(r1, r2, t,msaux);
                    HashMap<Long, Table> intermRes = new HashMap<>(zsAux.fst);Tuple<HashMap<Long, Table>,
                            HashMap<Long, Table>> funcRes = new Tuple(intermRes.put(t, us_result.fst()), us_result.snd());
                    return funcRes;};
                //LinkedList<Long> tsList_arg = new LinkedList<>(tsList);
                Tuple<Tuple<HashMap<Long, Table>, HashMap<Long, Table>>,
                        Tuple<HashMap<Long, Table>, HashMap<Long, Table>>> mbuf2t_take_result = mbuf2t_take(func, new Tuple(new HashMap<Long, Table>(), msaux), mbuf2, event.getTimepoint());


                HashMap<Long, Table> zs = mbuf2t_take_result.fst.fst;
                for(int j = 0; j < zs.size(); j++){
                    Table evalSet = zs.get(j);
                    for(Assignment oa : evalSet){
                        collector.collect(new PipelineEvent(timepointToTimestamp.get(event.getTimepoint()),event.getTimepoint(), false, oa));
                    }
                }
                //at the end, we output the terminator! --> is it ok that the assignment is the input assignment?
                collector.collect(event);
            }
        }
    }

    public Tuple<Table,
            HashMap<Long, Table>> update_since(Table rel1, Table rel2, Long nt, HashMap<Long, Table> aux){

        //In the return type, I actually don't need a tuple because in the flink implementation the state is kept by the
        ///java class.

        HashMap<Long, Table> auxIntervalList = new HashMap<>();
        for(Long t : aux.keySet()){
            Table rel = aux.get(t);
            if(!interval.upper().isDefined() || (interval.upper().isDefined() && (nt - t <= ((Long) interval.upper().get())))){
                //is it ok to use "isDefined" here above? And that I had to cast to Long?

                auxIntervalList.put(t, join(rel, pos, rel1));
                //check that the Optionals in the joins all make sense!
            }
        }
        HashMap<Long, Table> auxp = new HashMap<>();
        if(auxIntervalList.size() == 0){
            auxp.put(nt, rel2);
        }else{
            Table x = auxIntervalList.get(smallestFullTimestamp);
            if(smallestFullTimestamp.equals(nt)){
                Table unionSet = Table.fromTable(x);
                unionSet.addAll(rel2);
                auxp = auxIntervalList;
                auxp.put(smallestFullTimestamp, unionSet);
            }else{
                auxIntervalList.put(smallestFullTimestamp,x);
                auxp = auxIntervalList;
                auxp.put(nt, rel2);
            }
        }
        Table bigUnion = new Table();
        //It also computes the satisfactions by taking the union over all tables in the list that satisfy c element of I.
        for(Long t : auxp.keySet()){
            Table rel = auxp.get(t);
            if(nt - t >= interval.lower()){
                bigUnion.addAll(rel);
            }
        }
        return new Tuple<>(bigUnion, auxp);
    }


    public Tuple<Tuple<HashMap<Long, Table>, LinkedList<Triple<Long, Table, Table>>>, //this tuple is the type of "b" in Verimon
            Tuple<HashMap<Long, Table>, HashMap<Long, Table>>> mbuf2t_take(Mbuf2take_function func,
                                       Tuple<HashMap<Long, Table>, HashMap<Long, Table>> z,
                                       Tuple<HashMap<Long, Table>, HashMap<Long, Table>> mbuf2,
                                    Long currentTimepoint){

        if(mbuf2.fst().size() > 1 && mbuf2.snd().size() > 1){ //I don't think checking these lengths is necessary
            Table x = mbuf2.fst().get(currentTimepoint);
            Table y = mbuf2.snd().get(currentTimepoint);
            mbuf2.fst().remove(currentTimepoint, mbuf2.fst().get(currentTimepoint));
            //does it make sense to remove these entries here?
            mbuf2.snd().remove(currentTimepoint, mbuf2.snd().get(currentTimepoint));
            //Long t = tsList.remove(0);
            Tuple<HashMap<Long, Table>, HashMap<Long, Table>> fxytz = func.run(x,y,timepointToTimestamp.get(currentTimepoint),z);
            return mbuf2t_take(func, fxytz, new Tuple<>(mbuf2.fst(), mbuf2.snd()), currentTimepoint);
        }
        else if(mbuf2.fst().size() == 1 && mbuf2.snd().size() == 1){
            Tuple<Tuple<HashMap<Long, Table>, LinkedList<Triple<Long, Table, Table>>>,
                    Tuple<HashMap<Long, Table>, HashMap<Long, Table>>> result = new Tuple(z,mbuf2);
            return result;
        }else{
            //error!!!
        }
        return null;
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
                    if(x==y) { //should enter this clause automatically
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
    /*public Tuple<HashMap<Long, Table>,
            HashMap<Long, Table>> mbuf2_add(Tuple<HashMap<Long, Table>,
            HashMap<Long, Table>> xsys, HashMap<Long, Table> xsp,
                                            HashMap<Long, Table> ysp){
        //I don't actually need this method!!
        xsys.fst().get(smallestCompleteTPleft).addAll(xsp.get(smallestCompleteTPleft));
        xsys.snd().get(smallestCompleteTPright).addAll(ysp.get(smallestCompleteTPright));
        return xsys;

    }*/
}

interface Mbuf2take_function{
    Tuple<HashMap<Long, Table>,
            HashMap<Long, Table>> run(Table t1,
                                 Table t2,
                                 Long ts, Tuple<HashMap<Long, Table>, HashMap<Long, Table>> b);
}
