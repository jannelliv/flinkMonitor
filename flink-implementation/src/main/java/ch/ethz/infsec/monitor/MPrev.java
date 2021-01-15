package ch.ethz.infsec.monitor;
import ch.ethz.infsec.policy.Interval;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import java.util.*;
import ch.ethz.infsec.util.*;
import ch.ethz.infsec.monitor.visitor.*;

public class MPrev implements Mformula, FlatMapFunction<PipelineEvent, PipelineEvent> {
    //Prev tells you the satisfying assignments of the formula for the previous position.
    //The principle is that you want to delay/postpone the assignments that you receive by 1. So
    //you simply build the state with the first assignment that you receive, but you don't
    // output anything. Then when you receive a terminator, you output it, and this is as if you
    //said false for the "first" position in the trace. While you receive those, you don't output
    //anything downstream. This effectively delays the output by 1. Concatenating an empty table
    //in front of zs just means we are at the first position.
    // TODO: understand how to implement mprev_next in a streaming setting
    //The alternative (less creative) is to wait for the Terminator, build the table and invoke
    //meval with a table (db) --> just like Verimon.
    ch.ethz.infsec.policy.Interval interval;
    public Mformula formula;
    boolean bool;
    ArrayList<ArrayList<PipelineEvent>> tableList; //buf --> Verimon
    LinkedList<Long> tsList; //nts --> name used in Verimon

    HashMap<Long, HashSet<PipelineEvent>> A; //mapping from timepoint to set of assignments (set of PEs)
    HashMap<Long, Long> T; //mapping from timepoint to timestamps for non-terminator events
    HashMap<Long, Long> TT;//mapping from timepoint to timestamps for terminator events


    

    public MPrev(ch.ethz.infsec.policy.Interval interval, Mformula mform, boolean bool, LinkedList<Long> tsList) {
        //the P.E. value is the satisfaction of the subformula!
        this.interval = interval;
        //The interval is attached to the formula; it is a property of the formula.

        this.formula = mform;
        this.bool = bool;
        this.tsList = tsList;


        this.tableList = new ArrayList<>();
        //we actually don't use this in the below code; even if it WAS used in Verimon

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

        //ADD ASSERT: forall i. T.keys().contains(i+1) ==> ! A.keys().contains(i)
        if(value.isPresent()){
            if(!T.containsKey(value.getTimepoint())){
                T.put(value.getTimepoint(), value.getTimestamp());
            }
            //i.e. the event we are receiving is NOT a terminator
            //1)
            if(T.keySet().contains(value.getTimepoint() + 1) || TT.containsKey(value.getTimepoint() + 1)){
                //added second condition on top
                if(T.keySet().contains(value.getTimepoint() + 1)){
                    if(mem(T.get(value.getTimepoint() + 1) - value.getTimestamp(), interval)){
                        out.collect(PipelineEvent.event(T.get(value.getTimepoint() + 1),
                                value.getTimepoint() + 1, value.get()));
                    }
                }else{
                    if(mem(TT.get(value.getTimepoint() + 1) - value.getTimestamp(), interval)){
                        out.collect(PipelineEvent.event(TT.get(value.getTimepoint() + 1),
                                value.getTimepoint() + 1, value.get()));
                    }
                }

            }else{

                //It might be that you don't know the timestamp, e.g. assuming that you have not
                //received any tuple from the current timepoint. You will only know the timpestamp
                //when you receive the first PipelineEvent for our current timepoint. SO if we don't have the
                //timestamp, then we have to buffer the pipeline event
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
            handleBuffered(value, out);
        }else{

            if(TT.keySet().contains(value.getTimepoint() + 1)){
                out.collect(PipelineEvent.terminator( TT.get(value.getTimepoint() + 1),value.getTimepoint()+ 1));
                TT.put(value.getTimepoint(), value.getTimestamp());
                TT.remove(value.getTimepoint() + 1); //double check
            }else{
                TT.put(value.getTimepoint(), value.getTimestamp());
            }
            //2
            handleBuffered(value, out);
        }

    }

    public void handleBuffered(PipelineEvent value, Collector<PipelineEvent> out) throws Exception {
        /*if(!value.isPresent() && value.getTimepoint() == 0){
            out.collect(PipelineEvent.terminator(value.getTimestamp(), value.getTimepoint()));
            return;
        } else*/ if(value.getTimepoint() == 0){
            //output an EMPTY SET because previous is not satisfied at the first position.
            //for next we don't need this special case, as we don't realy have a "last"
            //timepoint, given that traces are infinite.
            return;
        }else{
            if(value.isPresent() && !T.keySet().contains(value.getTimepoint())){
                T.put(value.getTimepoint(), value.getTimestamp());
            }
        }

        if(A.keySet().contains(value.getTimepoint() - 1)){ //controllo valori precedenti
            HashSet<PipelineEvent> eventsAtPrev = A.get(value.getTimepoint() - 1);
            for (PipelineEvent buffAss : eventsAtPrev){
                if(mem((value.getTimestamp() - buffAss.getTimestamp()), interval)){
                    assert(value.getTimestamp() - buffAss.getTimestamp() >= 0);
                    //Above, we are checking the itnerval cosntraint, i.e. that
                    //the difference between the timestamps is within the interval.
                    //how is it be that both assertions hold?
                    out.collect(PipelineEvent.event(value.getTimestamp(), value.getTimepoint(), buffAss.get()));
                }
            }
            A.remove(value.getTimepoint() - 1);

        }
        if(TT.keySet().contains(value.getTimepoint() - 1)){
            if(value.getTimepoint() - 1 == 0L){
                out.collect(PipelineEvent.terminator(TT.get(0L), value.getTimepoint() - 1));
            }
            out.collect(PipelineEvent.terminator(value.getTimestamp(), value.getTimepoint()));
            TT.remove(value.getTimepoint() - 1);
            T.remove(value.getTimepoint());
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