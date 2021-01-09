package ch.ethz.infsec.monitor;
import ch.ethz.infsec.policy.Interval;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import java.util.*;
import ch.ethz.infsec.util.*;
import ch.ethz.infsec.monitor.visitor.*;

public class MEventually implements Mformula, FlatMapFunction<PipelineEvent, PipelineEvent> {

    ch.ethz.infsec.policy.Interval interval;
    public Mformula formula;

    HashMap<Long, HashSet<PipelineEvent>> buckets; //maybe you can make these assignments instead of PipelineEvents!
    HashMap<Long, Long> timepointToTimestamp;
    HashMap<Long, Long> terminators;

    public MEventually(ch.ethz.infsec.policy.Interval interval, Mformula mform) {
        this.formula = mform;
        this.interval = interval;

        this.buckets = new HashMap<>();
        this.timepointToTimestamp = new HashMap<>();
        this.terminators = new HashMap<>();
    }

    @Override
    public <T> DataStream<PipelineEvent> accept(MformulaVisitor<T> v) {
        return (DataStream<PipelineEvent>) v.visit(this);
    }


    @Override
    public void flatMap(PipelineEvent event, Collector<PipelineEvent> out) throws Exception {
        if(!timepointToTimestamp.containsKey(event.getTimepoint())){
            timepointToTimestamp.put(event.getTimepoint(), event.getTimestamp());
        }
        if(event.isPresent()){
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
        }

        if(event.isPresent()){
            for(Long tp : buckets.keySet()){
                if(mem(event.getTimestamp() - timepointToTimestamp.get(tp), interval)){
                    HashSet<PipelineEvent> satisfEvents = buckets.get(tp);
                    for(PipelineEvent pe : satisfEvents){
                        if(pe.isPresent()){
                            out.collect(pe);
                            satisfEvents.remove(pe); //might get ConcurrentModificationException here
                        }
                    }
                    //after this, you cannot automatically do buckets.remove(tp) because you don't know if you have all events yet
                    if(terminators.containsKey(tp)){
                        out.collect(new PipelineEvent(terminators.get(tp), tp, true, Assignment.nones(0)));
                        buckets.remove(tp);  //I think it's safe to do this --> but: COncurrentModException
                        terminators.remove(tp);  //and this
                    }else{
                        //do nothing
                    }
                }else{
                    //do nothing
                }
            }
        }else{
            //STILL HAVE TO HANDLE TERMINATORS
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