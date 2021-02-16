package ch.ethz.infsec.monitor;
import ch.ethz.infsec.policy.Interval;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import java.util.*;
import ch.ethz.infsec.util.*;
import ch.ethz.infsec.monitor.visitor.*;

public class MPrev implements Mformula, FlatMapFunction<PipelineEvent, PipelineEvent> {
    ch.ethz.infsec.policy.Interval interval;
    public Mformula formula;
    boolean bool;
    HashMap<Long, HashSet<PipelineEvent>> A; //mapping from timepoint to set of assignments (set of PEs)
    HashMap<Long, Long> T; //mapping from timepoint to timestamps for non-terminator events
    HashMap<Long, Long> TT;//mapping from timepoint to timestamps for terminator events


    

    public MPrev(ch.ethz.infsec.policy.Interval interval, Mformula mform, boolean bool, LinkedList<Long> tsList) {
        this.interval = interval;
        this.formula = mform;
        this.bool = bool;
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


        //ADD ASSERT: forall i. T.keys().contains(i+1) ==> ! A.keys().contains(i)
        if(value.isPresent()){

            //i.e. the event we are receiving is NOT a terminator
            //1)
            if(T.keySet().contains(value.getTimepoint() + 1) || TT.containsKey(value.getTimepoint() + 1)){
                //added second condition on top
                if(T.keySet().contains(value.getTimepoint() + 1)){
                    if(IntervalCondition.mem2(T.get(value.getTimepoint() + 1) - value.getTimestamp(), interval)){
                        out.collect(PipelineEvent.event(T.get(value.getTimepoint() + 1),
                                value.getTimepoint() + 1, value.get()));
                    }
                }else{
                    if(IntervalCondition.mem2(TT.get(value.getTimepoint() + 1) - value.getTimestamp(), interval)){
                        out.collect(PipelineEvent.event(TT.get(value.getTimepoint() + 1),
                                value.getTimepoint() + 1, value.get()));
                    }
                }

            }else{

                if(A.keySet().contains(value.getTimepoint())){
                    A.get(value.getTimepoint()).add(PipelineEvent.event(value.getTimestamp(), value.getTimepoint(), value.get()));
                }else{
                    HashSet<PipelineEvent> hspe = new HashSet<>();
                    hspe.add(PipelineEvent.event( value.getTimestamp(), value.getTimepoint(), value.get()));
                    A.put(value.getTimepoint(), hspe);
                }
            }
            //2
            //as soon as you have received the timestamp, you can process the stored/buffered assignments.

        }else{

            if(TT.containsKey(value.getTimepoint() + 1)){
                out.collect(PipelineEvent.terminator( TT.get(value.getTimepoint() + 1),value.getTimepoint()+ 1));
                TT.remove(value.getTimepoint() + 1); //double check
            }
            TT.put(value.getTimepoint(), value.getTimestamp());

            //2

        }
        handleBuffered(value, out);
    }

    public void handleBuffered(PipelineEvent value, Collector<PipelineEvent> out) throws Exception {
       if(value.getTimepoint() == 0){
            return;
        }else{
            if(value.isPresent() && !T.containsKey(value.getTimepoint())){
                T.put(value.getTimepoint(), value.getTimestamp());
            }
        }

        if(A.containsKey(value.getTimepoint() - 1)){ //checks previous values
            HashSet<PipelineEvent> eventsAtPrev = A.get(value.getTimepoint() - 1);
            for (PipelineEvent buffAss : eventsAtPrev){
                if(IntervalCondition.mem2((value.getTimestamp() - buffAss.getTimestamp()), interval)){
                    out.collect(PipelineEvent.event(value.getTimestamp(), value.getTimepoint(), buffAss.get()));
                }
            }
            A.remove(value.getTimepoint() - 1);

        }
        if(TT.containsKey(value.getTimepoint() - 1)){
            if(value.getTimepoint() - 1 == 0L){
                out.collect(PipelineEvent.terminator(TT.get(0L), value.getTimepoint() - 1));
            }
            out.collect(PipelineEvent.terminator(value.getTimestamp(), value.getTimepoint()));
            TT.remove(value.getTimepoint() - 1);
            T.remove(value.getTimepoint());
        }
    }

}