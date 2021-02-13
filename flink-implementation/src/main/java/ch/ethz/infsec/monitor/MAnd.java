package ch.ethz.infsec.monitor;
import java.util.*;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

import ch.ethz.infsec.util.*;
import ch.ethz.infsec.monitor.visitor.*;



public class MAnd implements Mformula, CoFlatMapFunction<PipelineEvent, PipelineEvent, PipelineEvent> {

    boolean bool;
    public Mformula op1;
    public Mformula op2;
    Tuple<HashMap<Long, Table>,HashMap<Long, Table>> mbuf2;
    HashSet<Long> terminatorLHS;
    HashSet<Long> terminatorRHS;

    public MAnd(Mformula arg1, boolean bool, Mformula arg2) {
        this.bool = bool;
        this.op1 = arg1;
        this.op2 = arg2;

        this.mbuf2 = new Tuple<>(new HashMap<>(), new HashMap<>());
        terminatorLHS = new HashSet<>();
        terminatorRHS = new HashSet<>();
    }

    @Override
    public <T> DataStream<PipelineEvent> accept(MformulaVisitor<T> v) {
        return (DataStream<PipelineEvent>) v.visit(this);
        //Is it ok that I did the cast here above?
    }

    @Override
    public void flatMap1(PipelineEvent fact, Collector<PipelineEvent> collector) throws Exception {
        //here we have a streaming implementation. We can produce an output potentially for every event.
        //We don't buffer events until we receive a terminator event, contrary to what the Verimon algorithm does.
        if(!fact.isPresent()){
            terminatorLHS.add(fact.getTimepoint());
            if(terminatorRHS.contains(fact.getTimepoint())){
                this.mbuf2.fst.remove(fact.getTimepoint());
                this.mbuf2.snd.remove(fact.getTimepoint());
                collector.collect(fact);
                terminatorRHS.remove(fact.getTimepoint());
                terminatorLHS.remove(fact.getTimepoint());
            }
        }else if(!terminatorLHS.contains(fact.getTimepoint())){
            if(!this.mbuf2.fst.containsKey(fact.getTimepoint())){
                this.mbuf2.fst.put(fact.getTimepoint(), Table.empty());
            }
            this.mbuf2.fst.get(fact.getTimepoint()).add(fact.get());

            if(mbuf2.snd.containsKey(fact.getTimepoint()) &&  !this.mbuf2.snd.get(fact.getTimepoint()).isEmpty()){ //maybe it only contains a terminator :(
                for(Assignment rhs : this.mbuf2.snd.get(fact.getTimepoint())){
                    Optional<Assignment> joinResult = join1(fact.get(), rhs, 0);
                    if(joinResult.isPresent()){
                        PipelineEvent result = PipelineEvent.event(fact.getTimestamp(),fact.getTimepoint(), joinResult.get());
                        collector.collect(result);
                    }
                }
            }
        }
    }

    @Override
    public void flatMap2(PipelineEvent fact, Collector<PipelineEvent> collector) throws Exception {
        //one terminator fact has to be sent out once it is received on both incoming streams!!
        if(!fact.isPresent()){
            terminatorRHS.add(fact.getTimepoint());
            if(terminatorLHS.contains(fact.getTimepoint())){
                this.mbuf2.fst.remove(fact.getTimepoint());
                this.mbuf2.snd.remove(fact.getTimepoint());
                collector.collect(fact); //Only one terminator needs to be output, not both of them
                terminatorRHS.remove(fact.getTimepoint());
                terminatorLHS.remove(fact.getTimepoint());
            }
        }else if(!terminatorRHS.contains(fact.getTimepoint())){
            if(!this.mbuf2.snd.containsKey(fact.getTimepoint())){
                this.mbuf2.snd.put(fact.getTimepoint(), Table.empty());
            }
            this.mbuf2.snd.get(fact.getTimepoint()).add(fact.get());

            if(mbuf2.fst.containsKey(fact.getTimepoint()) && !this.mbuf2.fst.get(fact.getTimepoint()).isEmpty()){ //maybe it only contains a terminator :(
                for(Assignment lhs : this.mbuf2.fst.get(fact.getTimepoint())){
                    Optional<Assignment> joinResult = join1(fact.get(), lhs, 0);
                    if(joinResult.isPresent()){
                        PipelineEvent result = PipelineEvent.event(fact.getTimestamp(),fact.getTimepoint(), joinResult.get());
                        collector.collect(result);
                    }
                }
            }

        }
    }

    public static Optional<Assignment> join1(Assignment a, Assignment b, int i){
        if(a.size() == 0 && b.size() == 0) {
            return Optional.of(new Assignment());
        }else if(a.size() == 0 || b.size() == 0){
            return Optional.empty();
        }else {
            if( i < a.size() && i < b.size()){
                Optional<Object> x = a.get(i);
                Optional<Object> y = b.get(i);
                Optional<Assignment> subResult = join1(a, b, i+1);
                if(!x.isPresent() && !y.isPresent()) {
                    if(!subResult.isPresent()) {
                        return Optional.empty();
                    }else {
                        Assignment consList = new Assignment();
                        consList.add(Optional.empty());
                        consList.addAll(subResult.get());
                        //Problem: get() can only return a value if the wrapped object is not null;
                        //otherwise, it throws a no such element exception
                        return Optional.of(consList);
                    }
                }else if(x.isPresent() && !y.isPresent()) {
                    if(!subResult.isPresent()) {
                        return Optional.empty();
                    }else {
                        Assignment consList = new Assignment();
                        consList.add(x);
                        consList.addAll(subResult.get());
                        return Optional.of(consList);
                    }
                }else if(!x.isPresent() && y.isPresent()) {
                    if(!subResult.isPresent()) {
                        return Optional.empty();
                    }else {
                        Assignment consList = new Assignment();
                        consList.add(y);
                        consList.addAll(subResult.get());
                        return Optional.of(consList);
                    }
                }else if(x.isPresent() && y.isPresent() || x.get().equals(y.get())) {
                    //is it ok to do things with toString here above?
                    if(!subResult.isPresent()) {
                        return Optional.empty();
                    }else {
                        if(x.get().equals(y.get())) {
                            Assignment consList = new Assignment();
                            consList.add(x);
                            consList.addAll(subResult.get());
                            return Optional.of(consList);
                        }
                    }
                }else {
                    return Optional.empty();
                }
            }else{
                if(a.size() != b.size()){
                    return Optional.empty();
                }else{
                    return Optional.of(new Assignment());
                }
            }

        }

        return Optional.empty();
    }

}
