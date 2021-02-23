package ch.ethz.infsec.monitor;
import ch.ethz.infsec.policy.Interval;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import java.util.*;
import ch.ethz.infsec.util.*;
import ch.ethz.infsec.monitor.visitor.*;

public class MNext implements Mformula, FlatMapFunction<PipelineEvent, PipelineEvent> {
    //Next tells us the satisfying assignments of the formula for the next position.
    //It anticipate the assignments that we receive by 1.
    // So we simply discard the first assignment that you receive

    ch.ethz.infsec.policy.Interval interval;
    public Mformula formula;
    boolean bool;
    LinkedList<Long> tsList;
    HashMap<Long, HashSet<PipelineEvent>> A; //mapping from timepoint to set of assignments (set of PEs)
    HashMap<Long, Long> T; //mapping from timepoint to timestamps for non-terminator events
    HashMap<Long, Long> TT;//mapping from timepoint to timestamps for terminator events

    public MNext(ch.ethz.infsec.policy.Interval interval, Mformula mform, boolean bool, LinkedList<Long> tsList) {
        this.interval = interval;
        this.formula = mform;
        this.bool = bool;
        this.tsList = tsList;
        this.A = new HashMap<>();
        this.T = new HashMap<>();
        this.TT = new HashMap<>();
    }

    @Override
    public <T> DataStream<PipelineEvent> accept(MformulaVisitor<T> v) {
        return (DataStream<PipelineEvent>) v.visit(this);
    }


    @Override
    public void flatMap(PipelineEvent value, Collector<PipelineEvent> out) throws Exception {
        //In the Verimon impl., meval returns a List<Table> (called xs), which corresponds to
        //the satisfying assignments of the subformula, and phi (the subformula). In this streaming context
        //instead of the Mformula phi, this part will be saved in the state (events within a same timestamp
        //are separated by a Terminator.
        //The flag "first" is true until you send out the first Terminator. This means you have passed
        //the first point. Then you set the flag to false. But while you are passing the first, you check
        //the boolean and return nothing.



        if(value.isPresent()){
            if(!T.containsKey(value.getTimepoint())){
                T.put(value.getTimepoint(), value.getTimestamp());
            }
            //1)
            if(T.keySet().contains(value.getTimepoint() - 1) || TT.containsKey(value.getTimepoint() - 1)){
                if(T.containsKey(value.getTimepoint() - 1)){
                    if(IntervalCondition.mem2(value.getTimestamp() - T.get(value.getTimepoint() - 1), interval)){
                        out.collect(PipelineEvent.event(T.get(value.getTimepoint() - 1),
                                value.getTimepoint() - 1, value.get()));
                    }
                }else{
                    if(IntervalCondition.mem2(value.getTimestamp() - TT.get(value.getTimepoint() - 1), interval)){
                        out.collect(PipelineEvent.event(TT.get(value.getTimepoint() - 1),
                                value.getTimepoint() - 1, value.get()));
                    }
                }

            }else{
                if(A.keySet().contains(value.getTimepoint())){
                    A.get(value.getTimepoint()).add(PipelineEvent.event(value.getTimepoint(),
                            value.getTimestamp(), value.get()));
                }else{
                    HashSet<PipelineEvent> hspe = new HashSet<>();
                    hspe.add(PipelineEvent.event(value.getTimepoint(), value.getTimestamp(),  value.get()));
                    A.put(value.getTimepoint(), hspe);
                }
            }
        }else{
            if(TT.keySet().contains(value.getTimepoint() - 1)){
                out.collect(PipelineEvent.terminator(TT.get(value.getTimepoint() - 1), value.getTimepoint()- 1));
                TT.put(value.getTimepoint(), value.getTimestamp());
                TT.remove(value.getTimepoint() - 1);
            }else{
                TT.put(value.getTimepoint(), value.getTimestamp());
            }
            //2
        }
        handleBuffered(value, out);
    }

    public void handleBuffered(PipelineEvent value, Collector<PipelineEvent> out) throws Exception {
        if(value.isPresent() && !T.keySet().contains(value.getTimepoint())){
            T.put(value.getTimepoint(), value.getTimestamp());
        }
        if(A.keySet().contains(value.getTimepoint() + 1)){
            HashSet<PipelineEvent> eventsAtPrev = A.get(value.getTimepoint() + 1);
            for (PipelineEvent buffAss : eventsAtPrev){
                if(IntervalCondition.mem2( buffAss.getTimestamp() - value.getTimestamp(), interval)){
                    assert(buffAss.getTimestamp() - value.getTimestamp() >= 0);

                    out.collect(PipelineEvent.event(value.getTimepoint(),
                            value.getTimestamp(),  buffAss.get()));
                }
            }
            A.remove(value.getTimepoint() + 1);
        }
        if(TT.keySet().contains(value.getTimepoint() + 1)){
            out.collect(PipelineEvent.terminator(value.getTimepoint(), value.getTimestamp()));
            TT.remove(value.getTimepoint() + 1);
            T.remove(value.getTimepoint());
        }
    }

}