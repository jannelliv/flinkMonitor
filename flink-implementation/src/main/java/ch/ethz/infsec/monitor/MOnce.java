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

    HashMap<Long, HashSet<PipelineEvent>> pastBuckets; //maybe you can make these assignments instead of PipelineEvents!
    HashMap<Long, Long> timepointToTimestamp;
    HashMap<Long, Long> terminators;
    HashMap<Long, HashSet<PipelineEvent>> bufferedEvents; //for evaluating events received OoO

    public MOnce(ch.ethz.infsec.policy.Interval interval, Mformula mform) {
        this.formula = mform;
        this.interval = interval;

        this.pastBuckets = new HashMap<>();
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
            if(!pastBuckets.containsKey(event.getTimepoint())){
                HashSet<PipelineEvent> set = new HashSet<>();
                set.add(event);
                pastBuckets.put(event.getTimepoint(), set);
            }else{
                pastBuckets.get(event.getTimepoint()).add(event);
            }
        }else{
            if(!terminators.containsKey(event.getTimepoint())){
                terminators.put(event.getTimepoint(), event.getTimestamp());
            }else{
                throw new RuntimeException("cannot receive Terminator twice");
            }
        }
        if(event.isPresent()){
            boolean collected = false;
            for(Long tp : pastBuckets.keySet()){
                if(mem(event.getTimestamp() - timepointToTimestamp.get(tp), interval)){
                    out.collect(event);
                    collected = true;
                    break;
                }
            }
            if(!collected){
                //events in the past that allow this satisfaction might still show up
                if(!bufferedEvents.containsKey(event.getTimepoint())){
                    HashSet<PipelineEvent> set = new HashSet<>();
                    set.add(event);
                    bufferedEvents.put(event.getTimepoint(), set);
                }else{
                    bufferedEvents.get(event.getTimepoint()).add(event);
                }
            }
            //do I have to handle the buffered events also here?
        }else{
            //STILL HAVE TO HANDLE TERMINATORS
            if(!bufferedEvents.keySet().contains(event.getTimepoint()) ||
                    bufferedEvents.keySet().contains(event.getTimepoint()) && bufferedEvents.get(event.getTimepoint()).isEmpty()){
                //terminator can be output if none of the events corresponding to its timepoint are buffered
                out.collect(event);
            }else if(bufferedEvents.keySet().contains(event.getTimepoint()) && bufferedEvents.get(event.getTimepoint()).size() == 1){
                for(PipelineEvent pe : bufferedEvents.get(event.getTimepoint())){
                    if(pe.equals(event)){
                        out.collect(event);
                    }
                }
            }else{
                //otherwise the terminator also needs to wait and be stored somewhere. We also store it within bufferdEvents
                if(bufferedEvents.containsKey(event.getTimepoint())){
                    bufferedEvents.get(event.getTimepoint()).add(event);
                }else{
                    HashSet<PipelineEvent> set = new HashSet<>();
                    set.add(event);
                    bufferedEvents.put(event.getTimepoint(), set);
                }
            }
        }

    }

    public void handleBuffered(PipelineEvent termEvent, Collector<PipelineEvent> out){
        if(pastBuckets.keySet().contains(termEvent.getTimepoint())){
            HashSet<PipelineEvent> pastEvents = pastBuckets.get(termEvent.getTimepoint());
            for(PipelineEvent pe : pastEvents){
                //...
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