package ch.ethz.infsec.monitor;
import ch.ethz.infsec.policy.Interval;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import java.util.*;
import ch.ethz.infsec.util.*;
import ch.ethz.infsec.monitor.visitor.*;

//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
//NOT AT ALL SURE ABOUT THIS IMPLEMENTATION
//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
//NB: In a streaming context you don't really have timepoints. You might get something at timepoint i
//and then something at timepoint a. In a streaming context, timepoints are present, they are attached to
//the pipelineEvent, but they don't come in order. SO I just have to take the events in the order in which they come.

public class MNext implements Mformula, FlatMapFunction<PipelineEvent, PipelineEvent> {
    //Next tells you the satisfying assignments of the formula for the next position.
    //The principle is that you want to anticipate the assignments that you receive by 1.
    // So you simply discard the first assignment that you receive


    //The alternative (less creative) is to wait for the Terminator, build the table and invoke
    //meval with a table (db) --> just like Verimon.
    ch.ethz.infsec.policy.Interval interval;
    public Mformula formula;
    boolean bool;
    LinkedList<Long> tsList; //nts --> name used in Verimon
    HashMap<Long, HashSet<PipelineEvent>> A; //mapping from timepoint to set of assignments (set of PEs)
    HashMap<Long, Long> T; //mapping from timepoint to timestamps for non-terminator events
    HashMap<Long, Long> TT;//mapping from timepoint to timestamps for terminator events

    public MNext(ch.ethz.infsec.policy.Interval interval, Mformula mform, boolean bool, LinkedList<Long> tsList) {
        //the P.E. value is the satisfaction of the subformula!
        this.interval = interval;
        //The interval is attached to the formula; it is a property of the formula.
        //Is it a problem that the interval extremes are expressed as integers, whereas I express timepoints
        // and timestamps as longs?
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

        //ADD ASSERT: forall i. T.keys().contains(i-1) ==> ! A.keys().contains(i)
        //Understand the above assertion and if it makes sense!

        //special case for timepoint 0:
        //if(value.getTimepoint() == 0){
            //exit if condition to store timestamp for 0 in handleBuffered
            //but you don't need to output or store anything for timepoint 0
            //we ignore both the assignments at zero and the terminators, but then
            //the handleBuffered still has to take place, because we still need to
            //save the timestamp at zero.


        if(value.isPresent()){
            //i.e. the event we are receiving is NOT a terminator
            //1)
            if(T.keySet().contains(value.getTimepoint() - 1)){
                if(mem(value.getTimestamp() - T.get(value.getTimepoint() - 1), interval)){
                    out.collect(PipelineEvent.event(T.get(value.getTimepoint() - 1),
                            value.getTimepoint() - 1, value.get()));
                }
            }else{ //CHECK WHEN TO UPDATE T
                //It might be that you don't know the timestamp, e.g. assuming that you have not
                //received any tuple from the current timepoint. You will only know the timestamp
                //when you receive the first PipelineEvent for our current timepoint. SO if we don't have the
                //timestamp, then we have to buffer the pipeline event
                if(A.keySet().contains(value.getTimepoint())){
                    A.get(value.getTimepoint()).add(PipelineEvent.event(value.getTimepoint(),
                            value.getTimestamp(), value.get()));
                }else{
                    HashSet<PipelineEvent> hspe = new HashSet<>();
                    hspe.add(PipelineEvent.event(value.getTimepoint(), value.getTimestamp(),  value.get()));
                    A.put(value.getTimepoint(), hspe);
                }
            }
            //2
            //as soon as you have received the timestamp, you can process the stored/buffered assignments.

        }else{
            if(T.keySet().contains(value.getTimepoint() - 1)){
                out.collect(PipelineEvent.terminator(T.get(value.getTimepoint() - 1), value.getTimepoint()- 1));
            }else{
                TT.put(value.getTimepoint(), value.getTimestamp());
            }
            //2
        }
        handleBuffered(value, out);
    }

    public void handleBuffered(PipelineEvent value, Collector<PipelineEvent> out) throws Exception {
        //Added below line, not 100% sure about it:
        if(value.isPresent() && !T.keySet().contains(value.getTimepoint())){
            T.put(value.getTimepoint(), value.getTimestamp());
        }

        if(A.keySet().contains(value.getTimepoint() + 1)){
            HashSet<PipelineEvent> eventsAtPrev = A.get(value.getTimepoint() + 1);
            for (PipelineEvent buffAss : eventsAtPrev){
                //Why don't we check the interval condition here?
                //Why is it better for performance if the interval condition is checked outside the for looP?
                if(mem( buffAss.getTimestamp() - value.getTimestamp(), interval)){
                    //Above, we are checking the interval constraint, i.e. that
                    //the difference between the timestamps is within the interval.
                    assert(buffAss.getTimestamp() - value.getTimestamp() > 0);
                    //how is it be that both assertions hold?
                    out.collect(PipelineEvent.event(value.getTimepoint(),
                            value.getTimestamp(),  buffAss.get()));
                }
            }
            A.remove(value.getTimepoint() + 1);
            if(TT.keySet().contains(value.getTimepoint() + 1)){
                out.collect(PipelineEvent.terminator(value.getTimepoint(), value.getTimestamp()));
                TT.remove(value.getTimepoint() + 1);
                T.remove(value.getTimepoint());
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