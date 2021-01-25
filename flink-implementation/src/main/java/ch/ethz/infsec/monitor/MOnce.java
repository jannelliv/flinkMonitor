package ch.ethz.infsec.monitor;
import ch.ethz.infsec.policy.Interval;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import java.util.*;
import ch.ethz.infsec.util.*;
import ch.ethz.infsec.monitor.visitor.*;

public class MOnce implements Mformula, FlatMapFunction<PipelineEvent, PipelineEvent> {

    ch.ethz.infsec.policy.Interval interval;
    public Mformula formula;

    HashMap<Long, HashSet<PipelineEvent>> buckets; //maybe you can make these assignments instead of PipelineEvents!
    HashMap<Long, Long> timepointToTimestamp;
    HashMap<Long, Long> terminators;
    Long largestInOrderTP;
    Long largestInOrderTS;

    public MOnce(ch.ethz.infsec.policy.Interval interval, Mformula mform) {
        this.formula = mform;
        this.interval = interval;

        this.buckets = new HashMap<>();
        this.timepointToTimestamp = new HashMap<>();
        this.terminators = new HashMap<>();
        largestInOrderTP = -1L;
        largestInOrderTS = -1L;
    }

    @Override
    public <T> DataStream<PipelineEvent> accept(MformulaVisitor<T> v) {
        return (DataStream<PipelineEvent>) v.visit(this);
    }


    @Override
    public void flatMap(PipelineEvent event, Collector<PipelineEvent> out) throws Exception {

        if(event.isPresent()){
            if(!timepointToTimestamp.containsKey(event.getTimepoint())){
                timepointToTimestamp.put(event.getTimepoint(), event.getTimestamp());
            }
            if(!buckets.containsKey(event.getTimepoint())){
                HashSet<PipelineEvent> set = new HashSet<>();
                set.add(event);
                buckets.put(event.getTimepoint(), set);
            }else{
                buckets.get(event.getTimepoint()).add(event);
            }

        }else{
            if(!terminators.containsKey(event.getTimepoint())){
                terminators.put(event.getTimepoint(), event.getTimestamp());
            }else{
                throw new RuntimeException("cannot receive Terminator twice");
            }
            while(terminators.containsKey(largestInOrderTP + 1L)){ //not sure if this is necessary also for ONce
                largestInOrderTP++;
                largestInOrderTS = terminators.get(largestInOrderTP);
            }
        }

        if(event.isPresent()){
            HashSet<Long> toRemove1 = new HashSet<>();
            for(Long tp : buckets.keySet()){
                if(mem(timepointToTimestamp.get(tp) - event.getTimestamp(), interval)){
                    HashSet<PipelineEvent> satisfEvents = buckets.get(tp);
                    for(PipelineEvent pe : satisfEvents){
                        if(pe.isPresent()){
                            out.collect(pe);
                            //we don't remove anything here, because the events in the buckets may still help us identify satisf.
                            //I think in theory I could output all the events in a bucket except one!
                        }
                    }
                    //after this, you cannot automatically do buckets.remove(tp) because you don't know if you have all events yet
                    if(terminators.containsKey(tp)){
                        out.collect(PipelineEvent.terminator(terminators.get(tp), tp));
                        toRemove1.add(tp);
                        terminators.remove(tp);  //the order of the terminators is anyway stored in largestInOrderTP
                    }else{
                        //do nothing
                    }
                }
            }
            for(Long elem : toRemove1){
                buckets.remove(elem);
            }


            HashSet<Long> toRemove = new HashSet<>();
            for(Long term : terminators.keySet()){
                if(mem(terminators.get(term) - event.getTimestamp(), interval)){
                    out.collect(PipelineEvent.event(terminators.get(term), term, event.get())); //???
                    out.collect(PipelineEvent.terminator(terminators.get(term), term));
                    toRemove.add(term);
                }else if(term > event.getTimepoint()){ //not at all sure about this!!
                    out.collect(PipelineEvent.terminator(terminators.get(term), term));
                    toRemove.add(term);
                }
            }
            for(Long tp : toRemove){
                terminators.remove(tp);
            }
        }else{
            //TERMINATOR CASE
        }
        handleBuffered(out);

    }

    public void handleBuffered(Collector collector){
        HashSet<Long> toRemove = new HashSet<>();
        for(Long term : terminators.keySet()){
            //we only consider terminators and not buckets because we evaluate wrt largestInOrderTP
            if(terminators.get(term).intValue() - interval.lower() <= largestInOrderTS.intValue() &&
                    interval.upper().isDefined()
                    && terminators.get(term).intValue() - (int)interval.upper().get() <= largestInOrderTS.intValue()){
                collector.collect(PipelineEvent.terminator(terminators.get(term), term));
                toRemove.add(term);
            }
            for(Long tp : toRemove){
                terminators.remove(tp);
            }
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

}