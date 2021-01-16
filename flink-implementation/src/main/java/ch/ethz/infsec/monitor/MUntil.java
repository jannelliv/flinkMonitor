package ch.ethz.infsec.monitor;
import ch.ethz.infsec.policy.Interval;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;
import java.util.*;
import ch.ethz.infsec.util.*;
import ch.ethz.infsec.monitor.visitor.*;




public class MUntil implements Mformula, CoFlatMapFunction<PipelineEvent, PipelineEvent, PipelineEvent> {


    boolean pos;//indicates whether the first subformula is negated or not
    public Mformula formula1;
    ch.ethz.infsec.policy.Interval interval;
    public Mformula formula2;
    Tuple<HashMap<Long, Table>, HashMap<Long, Table>> mbuf2;
    HashMap<Long, Tuple<Table, Table>> muaux;
    Long startEvalMuauxTP;
    //for muaux, contrary to the Verimon impl, we have a HashMap with a Tuple in the Range, instead of a set of triples

    Long largestInOrderTPsub1;
    Long largestInOrderTPsub2;
    //startEvalTimepoint is the timepoint that corresponds to smallestFullTimestamp!
    Long startEvalTimepoint; //this gives us the timepoint from which to retrieve the timestamp and corresponding entries
    //in mbuf2 and muaux, in order to process these datastructures from the last entry which was not evaluated, and hence
    //was also not "taken" from the buffer.
    //Long smallestFullTimestamp;

    //At the end of the flatMap functions, remember to remove the necessary entries from timepointToTimestamp. Otw there
    //may be a memory leak/stack overflow.

    HashMap<Long, Long> timepointToTimestamp;
    HashSet<Long> terminSub1;
    HashSet<Long> terminSub2;
    Boolean fromFlatMap2;

    public MUntil(boolean b, Mformula accept, ch.ethz.infsec.policy.Interval interval,
                  Mformula accept1) {
        this.pos = b;
        this.formula1 = accept;
        this.formula2 = accept1;
        this.interval = interval;
        this.muaux = new HashMap<>();
        startEvalMuauxTP = -1L;

        this.timepointToTimestamp = new HashMap<>();
        this.largestInOrderTPsub1 = -1L;
        this.largestInOrderTPsub2 = -1L;
        this.startEvalTimepoint = 0L;
        HashMap<Long, Table> fst = new HashMap<>();
        HashMap<Long, Table> snd = new HashMap<>();
        this.mbuf2 = new Tuple<>(fst, snd);
        this.terminSub1 = new HashSet<>();
        this.terminSub2 = new HashSet<>();

    }

    @Override
    public <T> DataStream<PipelineEvent> accept(MformulaVisitor<T> v) {
        return (DataStream<PipelineEvent>) v.visit(this);
    }

    @Override
    public void flatMap1(PipelineEvent event, Collector<PipelineEvent> collector) throws Exception {
        //this.fromFlatMap2 = false;
        if(!timepointToTimestamp.containsKey(event.getTimepoint())){
            timepointToTimestamp.put(event.getTimepoint(), event.getTimestamp());
        }
        //NB: I think I don't actually need tsList,
        //because we store the timestamps in msaux, the timepoints in mbuf2, and we have
        //a specific datastructure which maps timepoints to timestamps

        if(event.isPresent()){
            if(mbuf2.fst().containsKey(event.getTimepoint())){
                mbuf2.fst().get(event.getTimepoint()).add(event.get());
            }else{
                mbuf2.fst().put(event.getTimepoint(), Table.one(event.get()));
            }
            if(muaux.containsKey(event.getTimepoint())){
                muaux.get(event.getTimepoint()).fst().add(event.get());

            }else{
                muaux.put(event.getTimepoint(), new Tuple<>(Table.one(event.get()), Table.empty()));
                if(startEvalMuauxTP == -1L || event.getTimepoint() < startEvalMuauxTP){
                    startEvalMuauxTP = event.getTimepoint();
                }
            }
        }

        if(!event.isPresent()){
            if(!terminSub1.contains(event.getTimepoint())){
                terminSub1.add(event.getTimepoint());
            }else{
                throw new Exception("Not possible to receive two terminators for the same timepoint.");
            }
            while(terminSub1.contains(largestInOrderTPsub1 + 1L)){
                largestInOrderTPsub1++;
            }

            if(largestInOrderTPsub2 >= startEvalTimepoint && largestInOrderTPsub1>= startEvalTimepoint &&
                    !(largestInOrderTPsub2 == -1 || largestInOrderTPsub1 == -1)){
                Mbuf2take_function_Until func = this::update_until;
                mbuf2t_take(func, startEvalTimepoint);
                //update_until accepts a timepoint, but then within the method, we retrieve the timestamp
                //and work with that!

                HashMap<Long, Table> evalUntilResult;

                if(muaux.isEmpty()){ //was !muaux.containsKey(startEvalTimepoint)
                    evalUntilResult = eval_until(event.getTimestamp(), startEvalMuauxTP);
                }else{
                    evalUntilResult = eval_until(timepointToTimestamp.get(startEvalTimepoint), startEvalMuauxTP);
                }

                HashMap<Long, Table> muaux_zs = evalUntilResult;
                for(Long timepoint : muaux_zs.keySet()){    //I'm not starting from "startEvalTimepoint"
                    Table evalSet = muaux_zs.get(timepoint);
                    for(Assignment oa : evalSet){
                        if(event.getTimepoint() != -1 && oa.size() !=0){
                            collector.collect(PipelineEvent.event(timepointToTimestamp.get(timepoint), timepoint, oa));
                        }else if(event.getTimepoint() == -1 ){
                            throw new Exception("problem retrieving timepoint");
                        }else{
                            throw new Exception("size zero");
                        }
                    }
                    //at the end, we output the terminator! --> for each of the timepoints in zs
                    //The terminators are not output as soon as we get them, but only when we are sure that
                    //we have output all of the satisfying assignments for that timepoint.
                    //Since Until is a future formula, we can produce assignments for a timepoint even after we have
                    //received the terminator for that timepoint.
                    collector.collect(PipelineEvent.terminator(timepointToTimestamp.get(timepoint), timepoint));

                }
                startEvalTimepoint = startEvalTimepoint + muaux_zs.keySet().size();
                //cleanUpDatastructures();
            }

        }
    }

    @Override
    public void flatMap2(PipelineEvent event, Collector<PipelineEvent> collector) throws Exception {
        //this.fromFlatMap2 = true;
        if(!timepointToTimestamp.containsKey(event.getTimepoint())){
            timepointToTimestamp.put(event.getTimepoint(), event.getTimestamp());
        }
        if(event.isPresent()){
            if(event.isPresent()){
                if(mbuf2.snd().containsKey(event.getTimepoint())){
                    mbuf2.snd().get(event.getTimepoint()).add(event.get());
                }else{
                    mbuf2.snd().put(event.getTimepoint(), Table.one(event.get()));
                }
                if(muaux.containsKey(event.getTimepoint())){
                    muaux.get(event.getTimepoint()).snd().add(event.get());
                }else{
                    muaux.put(event.getTimepoint(), new Tuple<>(Table.empty(), Table.one(event.get())));
                    if(startEvalMuauxTP == -1L || event.getTimepoint() < startEvalMuauxTP){
                        startEvalMuauxTP = event.getTimepoint();
                    }
                }
            }

        }else{
            if(!terminSub2.contains(event.getTimepoint())){
                terminSub2.add(event.getTimepoint());
            }else{
                throw new Exception("Not possible to receive two terminators for the same timepoint.");
            }
            while(terminSub2.contains(largestInOrderTPsub2 + 1L)){
                largestInOrderTPsub2++;
            }
            if(largestInOrderTPsub2 >= startEvalTimepoint && largestInOrderTPsub1>= startEvalTimepoint &&
                    !(largestInOrderTPsub2 == -1 || largestInOrderTPsub1 == -1)){
                //not 100% sure about above condition
                Mbuf2take_function_Until func = this::update_until;
                mbuf2t_take(func, startEvalTimepoint);
                HashMap<Long, Table> evalUntilResult;

                if(muaux.isEmpty()){ //was !muaux.containsKey(startEvalTimepoint)
                    evalUntilResult = eval_until(event.getTimestamp(), startEvalMuauxTP);
                }else{
                    evalUntilResult = eval_until(timepointToTimestamp.get(startEvalTimepoint), startEvalMuauxTP);
                }
                //eval_until will only output something if we are considering a full timepoint. ---> MAKE SURE THIS IS ENFORCED
                //So we don't need to check anything here.
                HashMap<Long, Table> muaux_zs = evalUntilResult;
                for(Long timepoint : muaux_zs.keySet()){
                    Table evalSet = muaux_zs.get(timepoint);
                    for(Assignment oa : evalSet){
                        if(event.getTimepoint() != -1L && oa.size() != 0){
                            collector.collect(PipelineEvent.event(timepointToTimestamp.get(timepoint), timepoint, oa));
                        }
                    }
                    //at the end, we output the terminator! --> for each of the timepoints in zs
                    collector.collect(PipelineEvent.terminator(timepointToTimestamp.get(timepoint), timepoint));

                }

                startEvalTimepoint = startEvalTimepoint + muaux_zs.keySet().size();
                //cleanUpDatastructures();
            }

        }
    }

    public void update_until(Long timepointToIndexRels, HashMap<Long, Table> rel1, HashMap<Long, Table> rel2){
        //in Verimon, nt is the current timestamp
        //this method updates muaux, based on the timestamp

        Long currentTimestamp = timepointToTimestamp.get(startEvalTimepoint); //OR IS IT: timepointToIndexRels

        for(Long timepointMuaux : muaux.keySet()){
            Tuple<Table, Table> tables = muaux.get(timepointMuaux);
            Table a2UnionAfter;
            Table firstTable;
            if(this.pos){
                assert(rel1.containsKey(timepointToIndexRels));
                firstTable = join(tables.fst(), true, rel1.get(timepointToIndexRels));
            }else{
                firstTable = Table.fromTable(tables.fst());
                assert(rel1.containsKey(timepointToIndexRels));
                firstTable.addAll(rel1.get(timepointToIndexRels));
            }
            if(mem(currentTimestamp - timepointToTimestamp.get(timepointMuaux), interval)){
                assert(rel2.containsKey(timepointToIndexRels));
                Table rel2a1Join = join(rel2.get(timepointToIndexRels), pos, tables.fst());
                a2UnionAfter = Table.fromTable(tables.snd());
                a2UnionAfter.addAll(rel2a1Join);
            }
            else{
                a2UnionAfter = tables.snd();
            }
            muaux.put(timepointMuaux, new Tuple<>(firstTable, a2UnionAfter));
        }
        Table addedTableForZeroLHS;
        if(interval.lower() == 0L){
            addedTableForZeroLHS = rel2.get(timepointToIndexRels);
        }else{
            addedTableForZeroLHS = Table.empty();
        }
        muaux.put(startEvalTimepoint, new Tuple<>(rel1.get(timepointToIndexRels), addedTableForZeroLHS));
    }

    public HashMap<Long, Table> eval_until(long currentTimestamp, long startEvalMuaux){
        //In Verimon, the table built below (xs), is per timepoint, not per timestamp!
        //In the Verimon code on afp it holds that:
        //current timestamp: nt;   smallest full timestamp: t
        if(muaux.isEmpty()){
            return new HashMap<>();
        }else{
            assert(muaux.containsKey(startEvalMuaux));
            Tuple<Table, Table> a1a2 = muaux.get(startEvalMuaux);
            //Question: I think I can remove this entry after I have evaluated it
            if(interval.upper().isDefined() && timepointToTimestamp.get(startEvalMuaux) + (int)interval.upper().get() < currentTimestamp){
                Long nextSmallest = Long.MAX_VALUE;
                for(Long time : muaux.keySet()){
                    if(time > startEvalMuaux && time < nextSmallest){
                        nextSmallest = time;
                    }
                }
                if(nextSmallest != Long.MAX_VALUE){
                    //startEvalMuauxTP = nextSmallest;         //not sure about this
                    //muaux.remove(startEvalMuaux);       //double check these 2 lines
                    HashMap<Long, Table> xs = eval_until(currentTimestamp, nextSmallest);
                    xs.put(startEvalMuaux, a1a2.snd());
                    return xs;
                }else{
                    HashMap<Long, Table> xs = new HashMap<>();
                    xs.put(startEvalMuaux, a1a2.snd()); //first argument used to be startEvalTImepoint
                    return xs;
                }
            }else{
                return new HashMap<>();
            }
        }
    }

    public void mbuf2t_take(Mbuf2take_function_Until func, Long tp){
        if(mbuf2.fst().containsKey(tp) && mbuf2.snd().containsKey(tp) && muaux.containsKey(tp) &&
        terminSub1.contains(tp) && terminSub2.contains(tp)){            ///PROBLEM -> SEE TEST

            func.run(tp, mbuf2.fst(),mbuf2.snd());
            //mbuf2.fst().remove(tp);
            //mbuf2.snd().remove(tp);
            tp++; //gets incremented and method is called recursively until one of the two HashMaps in
            //mbuf2 becomes empty.
            mbuf2t_take(func, tp);
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

    public void cleanUpDatastructures(){
        mbuf2.fst.keySet().removeIf(tp -> tp < startEvalTimepoint);
        mbuf2.snd.keySet().removeIf(tp -> tp < startEvalTimepoint);
        if(timepointToTimestamp.containsKey(startEvalTimepoint)){
            muaux.keySet().removeIf(ts -> ts < startEvalTimepoint);
        }
        timepointToTimestamp.keySet().removeIf(tp -> tp < startEvalTimepoint);
        terminSub2.removeIf(tp -> tp < startEvalTimepoint);
        terminSub1.removeIf(tp -> tp < startEvalTimepoint);
        //ADD startEvalMuauxTP!
    }
}

interface Mbuf2take_function_Until{
    void run(Long timepoint, HashMap<Long, Table> rel1, HashMap<Long, Table> rel2);
}