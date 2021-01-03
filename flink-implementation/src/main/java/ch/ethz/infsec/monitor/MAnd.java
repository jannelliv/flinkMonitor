package ch.ethz.infsec.monitor;
import java.util.*;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

import ch.ethz.infsec.util.*;
import ch.ethz.infsec.monitor.visitor.*;



public class MAnd implements Mformula, CoFlatMapFunction<PipelineEvent, PipelineEvent, PipelineEvent> {
    //Problem: I think MAnd assumes ordered pipelineevents in my implementation!
    boolean bool;
    public Mformula op1;
    public Mformula op2;
    Tuple<LinkedList<Table>, LinkedList<Table>> mbuf2;
    boolean terminatorLHS;
    boolean terminatorRHS;
    Long indexlhs, indexrhs;

    public MAnd(Mformula arg1, boolean bool, Mformula arg2) {
        this.bool = bool;
        this.op1 = arg1;
        this.op2 = arg2;

        this.mbuf2 = new Tuple<>(new LinkedList<>(), new LinkedList<>());
        terminatorLHS = false;
        terminatorRHS = false;
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
        System.out.println("Inside the MAnd flatMap1() method");
        //here we have a streaming implementation. We can produce an output potentially for every event.
        //We don't buffer things, contrary to what the Verimon algorithm does.
        if(!fact.isPresent()){
            terminatorLHS = true;
            indexlhs++;
            if(terminatorRHS){
                this.mbuf2.fst.remove(0);
                this.mbuf2.snd.remove(0);
                collector.collect(fact);
                terminatorRHS = false;
                terminatorLHS = false;
            }
        }else if(!terminatorLHS){
            if(this.mbuf2.fst.size() == 0){
                this.mbuf2.fst.add(Table.empty());
            }
            this.mbuf2.fst.get(0).add(fact.get());

            if(this.mbuf2.snd.size()>0){
                for(Assignment rhs : this.mbuf2.snd.get(0)){
                    Optional<Assignment> joinResult = join1(fact.get(), rhs);
                    if(joinResult.isPresent()){
                        PipelineEvent result = new PipelineEvent(fact.getTimestamp(),fact.getTimepoint(), false, joinResult.get());
                        collector.collect(result);
                    }
                }
            }
        }else{
            if(this.mbuf2.fst.size() < indexlhs + 1){
                this.mbuf2.fst.add(Table.empty());
            }
            this.mbuf2.fst.get(indexlhs.intValue()).add(fact.get());
            if(this.mbuf2.snd.get(indexlhs.intValue()) != null){
                for(Assignment rhs : this.mbuf2.snd.get(indexlhs.intValue())){
                    Optional<Assignment> joinResult = join1(fact.get(), rhs);
                    if(joinResult.isPresent()){
                        PipelineEvent result = new PipelineEvent(fact.getTimestamp(),fact.getTimepoint(),false, joinResult.get());
                        collector.collect(result);
                    }
                }
            }
        }
    }

    @Override
    public void flatMap2(PipelineEvent fact, Collector<PipelineEvent> collector) throws Exception {
        System.out.println("Inside the MAnd flatMap2() method");

        //one terminator fact has to be sent out once it is received on both incoming streams!!
        if(!fact.isPresent()){
            terminatorRHS = true;
            indexrhs++;
            if(terminatorLHS){
                this.mbuf2.fst.remove(0);
                this.mbuf2.snd.remove(0);
                collector.collect(fact);
                terminatorRHS = false;
                terminatorLHS = false;
            }
        }else if(!terminatorRHS){
            if(this.mbuf2.snd.size() == 0){
                this.mbuf2.snd.add(Table.empty());
            }
            this.mbuf2.snd.get(0).add(fact.get());
            if(this.mbuf2.fst.size()>0){
                for(Assignment lhs : this.mbuf2.fst.get(0)){
                    Optional<Assignment> joinResult = join1(lhs, fact.get());
                    if(joinResult.isPresent()){
                        PipelineEvent result = new PipelineEvent(fact.getTimestamp(),fact.getTimepoint(), false, joinResult.get());
                        collector.collect(result);
                    }
                }
            }

        }else{
            if(this.mbuf2.snd.size() < indexrhs + 1){
                //to make sure that there is something to get() here
                this.mbuf2.snd.add(Table.empty());
                //YOU CAN ALSO GET MORE THAN 1 TERMINATOR ON ONE SIDE!!
            }
            this.mbuf2.snd.get(indexrhs.intValue()).add(fact.get());
            if(this.mbuf2.fst.get(indexrhs.intValue()) != null){
                for(Assignment lhs : this.mbuf2.fst.get(indexrhs.intValue())){
                    Optional<Assignment> joinResult = join1(lhs, fact.get());
                    if(joinResult.isPresent()){
                        //so if the assignment is not present, nothing is passed upwards in the parsingtree
                        PipelineEvent result = new PipelineEvent(fact.getTimestamp(), fact.getTimepoint(), false, joinResult.get());
                        collector.collect(result);
                    }
                }
            }
        }
    }

    public static Optional<Assignment> join1(Assignment a, Assignment b){

        if(a.size() == 0 && b.size() == 0) {
            Assignment emptyList = new Assignment();
            Optional<Assignment> result = Optional.of(emptyList);
            return result;
        }else if(a.size() == 0 || b.size() == 0){
            Optional<Assignment> result = Optional.empty();
            return result;
        }else {
            Optional<Object> x = a.remove(0);
            Optional<Object> y = b.remove(0);
            Optional<Assignment> subResult = join1(a, b);
            if(!x.isPresent() && !y.isPresent()) {
                if(!subResult.isPresent()) {
                    Optional<Assignment> result = Optional.empty();
                    return result;
                }else {
                    Assignment consList = new Assignment();
                    consList.add(Optional.empty());
                    consList.addAll(subResult.get());
                    //Problem: get() can only return a value if the wrapped object is not null;
                    //otherwise, it throws a no such element exception
                    Optional<Assignment> result = Optional.of(consList);
                    return result;
                }
            }else if(x.isPresent() && !y.isPresent()) {
                if(!subResult.isPresent()) {
                    Optional<Assignment> result = Optional.empty();
                    return result;
                }else {
                    Assignment consList = new Assignment();
                    consList.add(x);
                    consList.addAll(subResult.get());
                    Optional<Assignment> result = Optional.of(consList);
                    return result;
                }
            }else if(!x.isPresent() && y.isPresent()) {
                if(!subResult.isPresent()) {
                    Optional<Assignment> result = Optional.empty();
                    return result;
                }else {
                    Assignment consList = new Assignment();
                    consList.add(y);
                    consList.addAll(subResult.get());
                    Optional<Assignment> result = Optional.of(consList);
                    return result;
                }
            }else if(x.isPresent() && y.isPresent() || x.get().toString() != y.get().toString()) {
                //is it ok to do things with toString here above?
                if(!subResult.isPresent()) {
                    Optional<Assignment> result = Optional.empty();
                    return result;
                }else {
                    if(x.get().toString() ==y.get().toString()) { //should enter this clause automatically
                        //is it ok to do things with toString here above?
                        Assignment consList = new Assignment();
                        consList.add(x);
                        consList.addAll(subResult.get());
                        Optional<Assignment> result = Optional.of(consList);
                        return result;
                    }
                }
            }else {
                Optional<Assignment> result = Optional.empty();
                return result;
            }
        }

        Optional<Assignment> result = Optional.empty();
        return result;
    }

}
