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
            //pastBuckets doesn't contain any terminators! --> important for when we do mem test
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
            //do I have to handle the buffered events also here? Yes
            //Every time I receive an event I have to check if that event helps find a satisfaction in the future
            //so the handleBeffered method should looks for satisfs in the future!
            handleBuffered(event, out);
        }else{
            //Contrary to the implem of pastBuckets, bufferedEvents should contain terminators
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

            //Do I also have to check for terminators arriving at the pastBuckets datastructure?
            //When you receive a terminator, you should see if it's time to clean up some entries in pastBuckets
            //You should do this here.
        }

    }

    public void handleBuffered(PipelineEvent incomingPastEvent, Collector<PipelineEvent> out){
        for(Long tp : bufferedEvents.keySet()){
            if(timepointToTimestamp.get(tp) - incomingPastEvent.getTimestamp() >= 0L){
                //not sure if this if condition is expressed correctly
                HashSet<PipelineEvent> bes = bufferedEvents.get(tp);
                boolean terminatorPresent = false;
                if(mem(timepointToTimestamp.get(tp) - incomingPastEvent.getTimestamp(), interval)){
                    for(PipelineEvent be : bes){
                        if(be.isPresent()){
                            out.collect(be);
                            bes.remove(be); //I think this is ok, since PipelineEvent implements equals() method
                            //Could result in concurrentmodificationexception
                        }else{
                            terminatorPresent = true;
                        }
                    }
                    if(terminatorPresent){
                        //the terminator is output last (after all the other buffered events) and only if it's the last thing
                        //in the hashset (but I actually think bes.size()==1 will always hold).
                        if(bes.size() == 1){
                            for(PipelineEvent pe : bes){
                                if(!pe.isPresent()){
                                    out.collect(pe);
                                }
                            }
                        }
                    }
                }
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