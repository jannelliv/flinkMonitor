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
    //startEvalTimepoint is the timepoint that corresponds to smallestFullTimestamp
    Long startEvalTimepoint; //this gives us the timepoint from which to retrieve the timestamp and corresponding entries
    //in mbuf2 and muaux, in order to process these datastructures from the last entry which was not evaluated, and hence
    //was also not "taken" from the buffer.
    //Long smallestFullTimestamp;

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
        if(!timepointToTimestamp.containsKey(event.getTimepoint())){
            timepointToTimestamp.put(event.getTimepoint(), event.getTimestamp());
        }

        if(event.isPresent()){
            if(mbuf2.fst().containsKey(event.getTimepoint())){
                mbuf2.fst().get(event.getTimepoint()).add(event.get());
            }else{
                mbuf2.fst().put(event.getTimepoint(), Table.one(event.get()));
            }
            if(muaux.containsKey(event.getTimepoint())){
                muaux.get(event.getTimepoint()).fst().add(event.get());
                if(startEvalMuauxTP == -1L || event.getTimepoint() < startEvalMuauxTP){
                    startEvalMuauxTP = event.getTimepoint();
                }

            }else{
                muaux.put(event.getTimepoint(), new Tuple<>(Table.one(event.get()), Table.empty()));
                if(startEvalMuauxTP == -1L || event.getTimepoint() < startEvalMuauxTP){
                    startEvalMuauxTP = event.getTimepoint();
                }
            }
        }

        if(!event.isPresent()){
            if(!mbuf2.fst().containsKey(event.getTimepoint())){
                mbuf2.fst().put(event.getTimepoint(), Table.empty());
            }
            if(!muaux.containsKey(event.getTimepoint())){
                muaux.put(event.getTimepoint(), new Tuple<>(Table.empty(), Table.empty()));
                if(startEvalMuauxTP == -1L || event.getTimepoint() < startEvalMuauxTP){
                    startEvalMuauxTP = event.getTimepoint();
                }
            }
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

                HashMap<Long, Table> evalUntilResult;

                if(muaux.isEmpty()){
                    evalUntilResult = eval_until(timepointToTimestamp.get(largestInOrderTPsub1), startEvalMuauxTP);
                }else{
                    evalUntilResult = eval_until(timepointToTimestamp.get(largestInOrderTPsub1), startEvalMuauxTP);
                }

                HashMap<Long, Table> muaux_zs = evalUntilResult;
                for(Long timepoint : muaux_zs.keySet()){
                    Table evalSet = muaux_zs.get(timepoint);
                    for(Assignment oa : evalSet){
                        collector.collect(PipelineEvent.event(timepointToTimestamp.get(timepoint), timepoint, oa));
                    }
                    collector.collect(PipelineEvent.terminator(timepointToTimestamp.get(timepoint), timepoint));

                }
                startEvalTimepoint = startEvalTimepoint + muaux_zs.keySet().size();
                startEvalMuauxTP = startEvalMuauxTP + muaux_zs.keySet().size();
                //cleanUpDatastructures();
            }

        }
    }

    @Override
    public void flatMap2(PipelineEvent event, Collector<PipelineEvent> collector) throws Exception {
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
                    if(startEvalMuauxTP == -1L || event.getTimepoint() < startEvalMuauxTP){
                        startEvalMuauxTP = event.getTimepoint();
                    }
                }else{
                    muaux.put(event.getTimepoint(), new Tuple<>(Table.empty(), Table.one(event.get())));
                    if(startEvalMuauxTP == -1L || event.getTimepoint() < startEvalMuauxTP){
                        startEvalMuauxTP = event.getTimepoint();
                    }
                }
            }

        }else{
            if(!mbuf2.snd().containsKey(event.getTimepoint())){
                mbuf2.snd().put(event.getTimepoint(), Table.empty());
            }
            if(!muaux.containsKey(event.getTimepoint())){
                muaux.put(event.getTimepoint(), new Tuple<>(Table.empty(), Table.empty()));
                if(startEvalMuauxTP == -1L || event.getTimepoint() < startEvalMuauxTP){
                    startEvalMuauxTP = event.getTimepoint();
                }
            }
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
                Mbuf2take_function_Until func = this::update_until;
                mbuf2t_take(func, startEvalTimepoint);
                HashMap<Long, Table> evalUntilResult;

                if(muaux.isEmpty()){
                    evalUntilResult = eval_until(timepointToTimestamp.get(largestInOrderTPsub2), startEvalMuauxTP);
                }else{
                    evalUntilResult = eval_until(timepointToTimestamp.get(largestInOrderTPsub2), startEvalMuauxTP);
                }

                HashMap<Long, Table> muaux_zs = evalUntilResult;
                for(Long timepoint : muaux_zs.keySet()){
                    Table evalSet = muaux_zs.get(timepoint);
                    for(Assignment oa : evalSet){
                        collector.collect(PipelineEvent.event(timepointToTimestamp.get(timepoint), timepoint, oa));
                    }
                    collector.collect(PipelineEvent.terminator(timepointToTimestamp.get(timepoint), timepoint));

                }

                startEvalTimepoint = startEvalTimepoint + muaux_zs.keySet().size();
                startEvalMuauxTP = startEvalMuauxTP + muaux_zs.keySet().size();
                //cleanUpDatastructures();
            }

        }
    }

    public void update_until(Long timepointToIndexRels, HashMap<Long, Table> rel1, HashMap<Long, Table> rel2){

        Long currentTimestamp = timepointToTimestamp.get(timepointToIndexRels);
        for(Long timepointMuaux : muaux.keySet()){
            Tuple<Table, Table> tables = muaux.get(timepointMuaux);
            Table a2UnionAfter;
            Table firstTable;
            if(this.pos){
                assert(rel1.containsKey(timepointToIndexRels));
                firstTable = Table.join(tables.fst(), true, rel1.get(timepointToIndexRels));
            }else{
                firstTable = Table.fromTable(tables.fst());
                assert(rel1.containsKey(timepointToIndexRels));
                firstTable.addAll(rel1.get(timepointToIndexRels));
            }
            if(IntervalCondition.mem2(currentTimestamp - timepointToTimestamp.get(timepointMuaux), interval)){
                assert(rel2.containsKey(timepointToIndexRels));
                Table rel2a1Join = Table.join(rel2.get(timepointToIndexRels), pos, tables.fst());
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
        muaux.put(timepointToIndexRels, new Tuple<>(rel1.get(timepointToIndexRels), addedTableForZeroLHS));
    }

    public HashMap<Long, Table> eval_until(long currentTimestamp, long startEvalMuaux){
        if(muaux.isEmpty()){
            return new HashMap<>();
        }else{
            assert(muaux.containsKey(startEvalMuaux));
            Tuple<Table, Table> a1a2 = muaux.get(startEvalMuaux);
            if(interval.upper().isDefined() && timepointToTimestamp.get(startEvalMuaux) + (int)interval.upper().get() < currentTimestamp){
                Long nextSmallest = Long.MAX_VALUE;
                for(Long time : muaux.keySet()){
                    if(time > startEvalMuaux && time < nextSmallest){
                        nextSmallest = time;
                    }
                }
                if(nextSmallest != Long.MAX_VALUE){
                    HashMap<Long, Table> xs = eval_until(currentTimestamp, nextSmallest);
                    xs.put(startEvalMuaux, a1a2.snd());
                    return xs;
                }else{
                    HashMap<Long, Table> xs = new HashMap<>();
                    xs.put(startEvalMuaux, a1a2.snd());
                    return xs;
                }
            }else{
                return new HashMap<>();
            }
        }
    }

    public void mbuf2t_take(Mbuf2take_function_Until func, Long tp){
        if(mbuf2.fst().containsKey(tp) && mbuf2.snd().containsKey(tp) &&
        terminSub1.contains(tp) && terminSub2.contains(tp)){

            func.run(tp, mbuf2.fst(),mbuf2.snd());
            tp++;
            mbuf2t_take(func, tp);
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
    }
}

interface Mbuf2take_function_Until{
    void run(Long timepoint, HashMap<Long, Table> rel1, HashMap<Long, Table> rel2);
}