package ch.ethz.infsec.monitor;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;
import java.util.*;
import ch.ethz.infsec.util.*;
import ch.ethz.infsec.monitor.visitor.*;

public class MOr implements Mformula, CoFlatMapFunction<PipelineEvent, PipelineEvent, PipelineEvent> {
    public Mformula op1;
    public Mformula op2;
    Tuple<LinkedList<HashSet<Optional<Assignment>>>, LinkedList<HashSet<Optional<Assignment>>>> mbuf2;
    //in addition to the params used in verimon, we need this to implement the streaming or:
    List<Set<Assignment>> tempOutput;
    //not sure if the implementation with "contains" is correct
    boolean terminatorLHS;
    boolean terminatorRHS;
    Long indexlhs, indexrhs;

    public MOr(Mformula accept, Mformula accept1) {
        this.op1 = accept;
        this.op2 = accept1;
        Optional<Object> el = Optional.empty(); //not sure if it's correct that I add this to the lists
        Assignment listEl = new Assignment();
        listEl.add(el);
        Optional<Assignment> el1 = Optional.of(listEl);

        HashSet<Optional<Assignment>> setEl = new HashSet<>();
        setEl.add(el1);
        LinkedList<HashSet<Optional<Assignment>>> fst = new LinkedList<>();
        LinkedList<HashSet<Optional<Assignment>>> snd = new LinkedList<>();
        fst.add(setEl);
        snd.add(setEl);
        this.mbuf2 = new Tuple<>(fst, snd);
        //additional to verilog implementation, necessary for streaming:
        terminatorLHS = false;
        terminatorRHS = false;
        HashSet<Assignment> tempOutputSet = new HashSet<>();
        tempOutput = new LinkedList<>();
        tempOutput.add(tempOutputSet);

        indexlhs = -1L;
        indexrhs = -1L;
    }

    @Override
    public <T> DataStream<PipelineEvent> accept(MformulaVisitor<T> v) {
        return (DataStream<PipelineEvent>) v.visit(this);
        //Is it ok that I did the cast here above?
    }


    @Override
    public void flatMap1(PipelineEvent fact, Collector<PipelineEvent> collector) throws Exception {
        if(!fact.isPresent()){
            terminatorLHS = true;
            indexlhs++;
            if(terminatorRHS){
                tempOutput.remove(tempOutput.size() - 1);
                collector.collect(fact);
                terminatorRHS = false;
                terminatorLHS = false;
            }
        }else if(!terminatorLHS){
            //normal case
            if(this.tempOutput.size() == 0){
                this.tempOutput.add(new HashSet<>());
            }
            if(!tempOutput.get(0).contains(fact)){ //check that cointains() works correctly
                tempOutput.get(0).add(fact.get());
                collector.collect(fact);
            }
        }else{
            assert(this.tempOutput.size() != 0);
            if(this.tempOutput.size() < indexlhs + 1){ // not sure about these conditions
                this.tempOutput.add(new HashSet<>());
            }
            if(!tempOutput.get(indexlhs.intValue()).contains(fact)){ //check that cointains() works correctly
                tempOutput.get(indexlhs.intValue()).add(fact.get());
                collector.collect(fact);
            }
        }
    }

    @Override
    public void flatMap2(PipelineEvent fact, Collector<PipelineEvent> collector) throws Exception {
        if(!fact.isPresent()){
            terminatorRHS = true;
            indexrhs++;
            if(terminatorLHS){
                tempOutput.remove(tempOutput.size() - 1);
                collector.collect(fact);
                terminatorRHS = false;
                terminatorLHS = false;
            }
        }else if(!terminatorRHS){
            //normal case
            if(this.tempOutput.size() == 0){
                this.tempOutput.add(new HashSet<>());
            }
            if(!tempOutput.get(0).contains(fact)){ //check that cointains() works correctly
                tempOutput.get(0).add(fact.get());
                collector.collect(fact);
            }
        }else{
            assert(this.tempOutput.size() != 0);
            if(this.tempOutput.size() < indexrhs + 1){ // not sure about these conditions
                this.tempOutput.add(new HashSet<>());
            }
            if(!tempOutput.get(indexrhs.intValue()).contains(fact)){ //check that cointains() works correctly
                tempOutput.get(indexrhs.intValue()).add(fact.get());
                collector.collect(fact);
            }
        }
    }
}

