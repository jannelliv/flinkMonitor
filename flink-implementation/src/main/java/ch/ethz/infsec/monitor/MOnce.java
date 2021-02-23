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

    HashMap<Long, HashSet<Assignment>> buckets; //indexed by timepoints, stores assignments that will be analyzed by future timepoints
    HashMap<Long, Long> timepointToTimestamp;
    HashMap<Long, Long> terminators;
    Long largestInOrderTP;
    Long largestInOrderTS;
    HashMap<Long, HashSet<Assignment>> outputted;

    public MOnce(ch.ethz.infsec.policy.Interval interval, Mformula mform) {
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
                if(IntervalCondition.mem2(terminators.get(term) - event.getTimestamp(), interval)){
                    out.collect(PipelineEvent.event(terminators.get(term), term, event.get()));
                }
            }

        }else{
            Long termtp = event.getTimepoint();
            for(Long tp : buckets.keySet()){
                if(IntervalCondition.mem2(terminators.get(termtp) - timepointToTimestamp.get(tp), interval)){
                    HashSet<Assignment> satisfEvents = buckets.get(tp);
                    for(Assignment pe : satisfEvents){
                        out.collect(PipelineEvent.event(terminators.get(termtp), termtp, pe));
                    }
                }
            }
        }
        handleBuffered(out);

    }

    public void handleBuffered(Collector collector){
        HashSet<Long> toRemove = new HashSet<>();
        HashSet<Long> toRemoveTPTS = new HashSet<>();
        HashSet<Long> toRemoveBuckets = new HashSet<>();

        for(Long term : terminators.keySet()){

            //we only consider terminators and not buckets because we evaluate wrt largestInOrderTP
            if(terminators.containsKey(term) && terminators.get(term).intValue() - interval.lower() <= largestInOrderTS.intValue() ){
                collector.collect(PipelineEvent.terminator(terminators.get(term), term));
                toRemove.add(term);
            }

        }
        for(Long tp : toRemove){
            terminators.remove(tp);
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
}

