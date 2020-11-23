package ch.ethz.infsec.src;
import ch.ethz.infsec.policy.Interval;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;
import java.util.*;


public class MPrev implements Mformula, FlatMapFunction<PipelineEvent, PipelineEvent> {
    //Prev tells you the satisfying assignments of the formula for the previous position.
    //The principle is that you want to delay the assignments that you receive by 1. So
    //you simply build the state with the first assignment that you receive, but you don't
    // output anything. Then when you receive a terminator, you output it, and this is as if you
    //said false for the "first" position in the trace. While you receive those, you don't output
    //anything downstream. This effectively delays the output by 1. Concatenating an empty table
    //in front of zs just means we are at the first position.
    // TODO: understand how to implement mprev_next in a streaming setting
    //The alternative (less creative) is to wait for the Terminator, build the table and invoke
    //meval with a table (db) --> just like Verimon.
    ch.ethz.infsec.policy.Interval interval;
    Mformula formula;
    boolean bool;
    ArrayList<ArrayList<PipelineEvent>> tableList; //buf --> Verimon
    LinkedList<Long> tsList; //nts --> name used in Verimon
    LinkedList<Long> tpList;
    LinkedList<Long> terminators; //timepoints of the databases that have reached a terminator

    public MPrev(ch.ethz.infsec.policy.Interval interval, Mformula mform, boolean bool, LinkedList<Long> tsList) {
        this.interval = interval;
        this.formula = mform;
        this.bool = bool;
        this.tsList = tsList;

        //Optional<Object> el = Optional.empty();
        //Assignment listEl = new Assignment();
        //listEl.add(el); Do I have to add an optional empty here?
        //Optional<Assignment> el1 = Optional.of(listEl);
        //LinkedList<Optional<Assignment>> listEl2 = new LinkedList<>();
        //listEl2.add(el1);
        ArrayList<ArrayList<PipelineEvent>> listEl3 = new ArrayList<ArrayList<PipelineEvent>>();
        //listEl3.add(listEl2);
        this.tableList = listEl3;

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

        if(!value.isPresent()){
            //it's a terminator
            this.terminators.add(value.getTimepoint());
            long indexPrev = value.getTimepoint() - 1;
            if(terminators.contains(indexPrev)){
                //here, before you delete all entries from the prev, you should release all assignments
                //(which had previously been buffered) from the list at the current timestamp! --> if applicable:
                for(int i = 0 ; i < tableList.size(); i++){
                    //maybe create a hashmap with a mapping to avoid the excessive for loops
                    if(tableList.get(i).get(0) != null && tableList.get(i).get(0).getTimepoint()== value.getTimepoint()){
                        ArrayList<PipelineEvent> pipeEventList = tableList.get(i);
                        for(int j = 0; j < pipeEventList.size(); j++){
                            PipelineEvent currentBufferedEvent = pipeEventList.get(j);
                            long bufEventTimestamp = currentBufferedEvent.getTimestamp();
                            ArrayList<PipelineEvent> pipeEventPrev = tableList.get(i + 1); //assume new elem appended at front
                            for (int k = 0; k < pipeEventPrev.size(); k++){
                                PipelineEvent prevBufEvent = pipeEventPrev.get(k);
                                long prevEventTimestamp = prevBufEvent.getTimestamp();
                                if(mem(bufEventTimestamp - prevEventTimestamp, this.interval)){
                                    out.collect(currentBufferedEvent);
                                    break;
                                }

                            }
                        }
                    }
                }
                //now you can delete all entries from the prev!
                for(int i = 0 ; i < tableList.size(); i++){
                    if(tableList.get(i).get(0) != null && tableList.get(i).get(0).getTimepoint()== indexPrev){
                        tableList.set(i, new ArrayList<PipelineEvent>());
                        break;
                    }
                }
            }else{ // !terminators.contains(indexPrev)
                for(int i = 0 ; i < tableList.size(); i++){
                    //maybe create a hashmap with a mapping to avoid the excessive for loops
                    if(tableList.get(i).get(0) != null && tableList.get(i).get(0).getTimepoint()== value.getTimepoint()){
                        ArrayList<PipelineEvent> pipeEventList = tableList.get(i);
                        for(int j = 0; j < pipeEventList.size(); j++){
                            PipelineEvent currentBufferedEvent = pipeEventList.get(j);
                            long bufEventTimestamp = currentBufferedEvent.getTimestamp();
                            ArrayList<PipelineEvent> pipeEventPrev = tableList.get(i + 1); //assume new elem appended at front
                            for (int k = 0; k < pipeEventPrev.size(); k++){
                                PipelineEvent prevBufEvent = pipeEventPrev.get(k);
                                long prevEventTimestamp = prevBufEvent.getTimestamp();
                                if(mem(bufEventTimestamp - prevEventTimestamp, this.interval)){
                                    out.collect(currentBufferedEvent);
                                    break;
                                }

                            }
                        }
                    }
                    //add what you do if you reach the end of the list and don't find a timepoint corresp to curr event
                    //...
                    if(i == 0 || i == tableList.size() - 1){
                        tableList.add(0, new ArrayList<PipelineEvent>());
                        tableList.get(0).add(0, value);
                        terminators.add(0, value.getTimepoint());
                    }
                }


            }
        }else{ //condition: value.isPresent()
            //add element to list for future prev evaluations in the trace
            for(int i = 0; i< tableList.size(); i++){
                if(tableList.get(i).get(0) != null && tableList.get(i).get(0).getTimepoint()== value.getTimepoint()){
                    tableList.get(i).add(0, value);
                    break;
                }
                if(i == tableList.size() - 1 || i == 0 ){
                    tableList.add(0, new ArrayList<PipelineEvent>());
                }
            }
            //now you can see if it should be output to the collector
            for(int i = 0 ; i < tableList.size(); i++){
                //maybe create a hashmap with a mapping to avoid the excessive for loops
                if(tableList.get(i).get(0) != null && tableList.get(i).get(0).getTimepoint()== value.getTimepoint()){
                    ArrayList<PipelineEvent> pipeEventList = tableList.get(i);
                    for(int j = 0; j < pipeEventList.size(); j++){
                        PipelineEvent currentBufferedEvent = pipeEventList.get(j);
                        long bufEventTimestamp = currentBufferedEvent.getTimestamp();
                        ArrayList<PipelineEvent> pipeEventPrev = tableList.get(i + 1); //assume new elem appended at front
                        for (int k = 0; k < pipeEventPrev.size(); k++){
                            PipelineEvent prevBufEvent = pipeEventPrev.get(k);
                            long prevEventTimestamp = prevBufEvent.getTimestamp();
                            if(mem(bufEventTimestamp - prevEventTimestamp, this.interval)){
                                out.collect(currentBufferedEvent);
                                break;
                            }

                        }
                    }
                }
            }
        }

        /*if(value.isPresent()){
            List<Table> bufXS = new ArrayList<>();
            //a table is a set of assignments
            Table xs = Table.one(value.get()); //NOT SURE IF THIS IS CORRECT
            bufXS.add(0,xs);
            List<Table> bufXsNew = new ArrayList();
            bufXsNew.addAll(this.tableList);
            bufXsNew.addAll(bufXS);
            /////////////////////////////////////
            List<Integer> mprev_nextSecondArgument = new ArrayList<>();
            //HOW DO I KNO WHAT THE TIMESTAMP OF value IS????
            //Triple<List<Table>, List<Table>, List<Integer>> tripleResult = mprev_next(this.interval, bufXsNew, );
        }*/


    }

    public static Triple<List<Table>, List<Table>, List<Integer>> mprev_next(Interval i, List<Table> xs,
                                                                             List<Integer> ts){
        if(xs.size() == 0) {
            List<Table> fstResult = new ArrayList<>();
            List<Table> sndResult = new ArrayList<>();
            return new Triple(fstResult,sndResult, ts);
        }else if(ts.size() == 0) {
            List<Table> fstResult = new ArrayList<>();
            List<Integer> thrdResult = new ArrayList<>();
            return new Triple(fstResult,xs, thrdResult);
        }else if(ts.size() == 1) {
            List<Table> fstResult = new ArrayList<>();
            List<Integer> thrdResult = new ArrayList<>();
            thrdResult.add(ts.get(0));
            return new Triple(fstResult,xs, thrdResult);
        }else if(xs.size() >= 1 && ts.size() >= 2) {
            Integer t = ts.remove(0);
            Integer tp = ts.get(0);
            Table x = xs.remove(0);
            Triple<List<Table>, List<Table>, List<Integer>> yszs = mprev_next(i, xs, ts); //ts includes tp here
            //above, should return a triple, not a tuple --> problem with verimon
            List<Table> fst = yszs.fst;
            List<Table> snd = yszs.snd;
            List<Integer> thr = yszs.thrd;
            if(mem(tp - t, i)) {
                fst.add(0, x);
            }else{
                Table empty_table = new Table();
                fst.add(0, empty_table);
            }
            return new Triple<>(fst, snd, thr);
        }
        return null;

    }

    public static boolean mem(long n, Interval I){
        if(I.lower() <= n && (!I.upper().isDefined() || (I.upper().isDefined() && n <= ((long) I.upper().get())))){
            return true;
        }
        return false;
    }


}