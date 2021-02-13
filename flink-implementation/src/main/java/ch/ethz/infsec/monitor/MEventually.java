package ch.ethz.infsec.monitor;
import ch.ethz.infsec.policy.Interval;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import java.nio.channels.Pipe;
import java.util.*;
import ch.ethz.infsec.util.*;
import ch.ethz.infsec.monitor.visitor.*;

public class MEventually implements Mformula, FlatMapFunction<PipelineEvent, PipelineEvent> {

    ch.ethz.infsec.policy.Interval interval;
    public Mformula formula;

    HashMap<Long, HashSet<Assignment>> buckets;
    HashMap<Long, Long> timepointToTimestamp;
    HashMap<Long, Long> terminators;
    Long largestInOrderTP;
    Long largestInOrderTS;
    HashMap<Long, HashSet<Assignment>> outputted;

    public MEventually(ch.ethz.infsec.policy.Interval interval, Mformula mform) {
        this.formula = mform;
        this.interval = interval;
        outputted = new HashMap<>();

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
                HashSet<Assignment> set = new HashSet<>();
                set.add(event.get());
                buckets.put(event.getTimepoint(), set);
            }else{
                buckets.get(event.getTimepoint()).add(event.get());
            }

        }else{
            if(!terminators.containsKey(event.getTimepoint())){
                terminators.put(event.getTimepoint(), event.getTimestamp());
            }else{
                throw new RuntimeException("cannot receive Terminator twice");
            }
            while(terminators.containsKey(largestInOrderTP + 1L)){
                largestInOrderTP++;
                largestInOrderTS = terminators.get(largestInOrderTP);
            }
        }

        if(event.isPresent()){

            for(Long term : terminators.keySet()){
                if(mem(event.getTimestamp() - terminators.get(term) , interval)){
                    out.collect(PipelineEvent.event(terminators.get(term), term, event.get()));
                    if(outputted.containsKey(term)){
                        outputted.get(term).add(event.get());
                    }else{
                        outputted.put(term, new HashSet<>());
                        outputted.get(term).add(event.get());
                    }
                }
            }
        }else{
            //TERMINATOR CASE
        }
        handleBuffered(out);
    }

    public void handleBuffered(Collector collector){
        HashSet<Long> toRemove = new HashSet<>();
        HashSet<Long> toRemoveOutputted = new HashSet<>();
        HashSet<Long> toRemoveTPTS = new HashSet<>();
        HashSet<Long> toRemoveBuckets = new HashSet<>();

        for(Long term : terminators.keySet()){
            HashSet<Long> toRemove1 = new HashSet<>();
            for(Long tp : buckets.keySet()){
                if(mem(timepointToTimestamp.get(tp) - terminators.get(term), interval)){
                    HashSet<Assignment> satisfEvents = buckets.get(tp);
                    for(Assignment pe : satisfEvents){
                        if(!outputted.containsKey(term) || outputted.containsKey(term) && !(outputted.get(term).contains(pe))){
                            //== returns true if two objects point to the same thing in the memory heap!
                            collector.collect(PipelineEvent.event(terminators.get(term), term, pe));
                            if(outputted.containsKey(term)){
                                outputted.get(term).add(pe);
                            }else{
                                outputted.put(term, new HashSet<>());
                                outputted.get(term).add(pe);
                            }
                        }
                    }
                    //after this, you cannot automatically do buckets.remove(tp) because you don't know if you have all events yet

                }
            }


            //we only consider terminators and not buckets because we evaluate wrt largestInOrderTP
            if(terminators.containsKey(term) && terminators.get(term).intValue() + interval.lower() <= largestInOrderTS.intValue() &&
                    interval.upper().isDefined()
                    && terminators.get(term).intValue() + (int)interval.upper().get() <= largestInOrderTS.intValue()){
                collector.collect(PipelineEvent.terminator(terminators.get(term), term));
                toRemove.add(term);
                toRemoveOutputted.add(term);
            }

        }
        for(Long tp : toRemove){
            terminators.remove(tp);
        }

        for(Long tp : toRemoveOutputted){
            outputted.remove(tp);
        }

        for(Long buc : buckets.keySet()){
            if(interval.upper().isDefined() && timepointToTimestamp.get(buc).intValue() + (int)interval.upper().get() < largestInOrderTS.intValue()){
                toRemoveBuckets.add(buc);
                toRemoveTPTS.add(buc);
            }
        }

        for(Long tp : toRemoveBuckets){
            buckets.remove(tp);
        }

        for(Long tp : toRemoveTPTS){
            timepointToTimestamp.remove(tp);
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