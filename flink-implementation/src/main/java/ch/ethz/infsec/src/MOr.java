package ch.ethz.infsec.src;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;
import java.util.*;

public class MOr implements Mformula, CoFlatMapFunction<Optional<List<Optional<Object>>>, Optional<List<Optional<Object>>>, Optional<List<Optional<Object>>>> {

    Mformula op1;
    Mformula op2;
    Tuple<LinkedList<HashSet<Optional<LinkedList<Optional<Object>>>>>, LinkedList<HashSet<Optional<LinkedList<Optional<Object>>>>>> mbuf2;
    //in addition to the params used in verimon, we need this to implement the streaming or:
    List<Set<Optional<List<Optional<Object>>>>> tempOutput;
    //not sure if the implementation with "contains" is correct
    boolean terminatorLHS;
    boolean terminatorRHS;
    Integer indexlhs, indexrhs;


    public MOr(Mformula accept, Mformula accept1) {
        this.op1 = accept;
        this.op2 = accept1;
        Optional<Object> el = Optional.empty();
        LinkedList<Optional<Object>> listEl = new LinkedList<>();
        listEl.add(el);
        Optional<LinkedList<Optional<Object>>> el1 = Optional.of(listEl);

        HashSet<Optional<LinkedList<Optional<Object>>>> setEl = new HashSet<>();
        setEl.add(el1);
        LinkedList<HashSet<Optional<LinkedList<Optional<Object>>>>> fst = new LinkedList<>();
        LinkedList<HashSet<Optional<LinkedList<Optional<Object>>>>> snd = new LinkedList<>();
        fst.add(setEl);
        snd.add(setEl);
        this.mbuf2 = new Tuple<>(fst, snd);
        //additional to verilog implementation, necessary for streaming:
        terminatorLHS = false;
        terminatorRHS = false;
        HashSet<Optional<List<Optional<Object>>>> tempOutputSet = new HashSet<>();
        List<Set<Optional<List<Optional<Object>>>>> tempOutput = new LinkedList<>();
        tempOutput.add(tempOutputSet);

        indexlhs = 0;
        indexrhs = 0;
    }

    @Override
    public <T> DataStream<Optional<List<Optional<Object>>>> accept(MformulaVisitor<T> v) {
        return (DataStream<Optional<List<Optional<Object>>>>) v.visit(this);
        //Is it ok that I did the cast here above?
    }


    @Override
    public void flatMap1(Optional<List<Optional<Object>>> fact, Collector<Optional<List<Optional<Object>>>> collector) throws Exception {
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
                tempOutput.get(0).add(fact);
                collector.collect(fact);
            }

        }else{
            assert(this.tempOutput.size() != 0);
            if(this.tempOutput.size() < indexlhs + 1){ // not sure about these conditions
                this.tempOutput.add(new HashSet<>());
            }
            if(!tempOutput.get(indexlhs).contains(fact)){ //check that cointains() works correctly
                tempOutput.get(indexlhs).add(fact);
                collector.collect(fact);
            }

        }
    }

    @Override
    public void flatMap2(Optional<List<Optional<Object>>> fact, Collector<Optional<List<Optional<Object>>>> collector) throws Exception {
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
                tempOutput.get(0).add(fact);
                collector.collect(fact);
            }

        }else{
            assert(this.tempOutput.size() != 0);
            if(this.tempOutput.size() < indexrhs + 1){ // not sure about these conditions
                this.tempOutput.add(new HashSet<>());
            }
            if(!tempOutput.get(indexrhs).contains(fact)){ //check that cointains() works correctly
                tempOutput.get(indexrhs).add(fact);
                collector.collect(fact);
            }

        }
    }
    
}

