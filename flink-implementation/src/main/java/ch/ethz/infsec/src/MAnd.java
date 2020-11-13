package ch.ethz.infsec.src;
import java.util.*;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;
import ch.ethz.infsec.src.Table;


public class MAnd implements Mformula, CoFlatMapFunction<Optional<Assignment>, Optional<Assignment>, Optional<Assignment>> {
    boolean bool;
    Mformula op1;
    Mformula op2;
    Tuple<LinkedList<Table>, LinkedList<Table>> mbuf2;
    boolean terminatorLHS;
    boolean terminatorRHS;
    Integer indexlhs, indexrhs;



    public MAnd(Mformula arg1, boolean bool, Mformula arg2) {
        this.bool = bool;
        this.op1 = arg1;
        this.op2 = arg2;

        this.mbuf2 = new Tuple<>(new LinkedList<>(), new LinkedList<>());
        terminatorLHS = false;
        terminatorRHS = false;
        indexlhs = 0;
        indexrhs = 0;

    }
    @Override
    public <T> DataStream<Optional<List<Optional<Object>>>> accept(MformulaVisitor<T> v) {
        return (DataStream<Optional<List<Optional<Object>>>>) v.visit(this);
        //Is it ok that I did the cast here above?
    }

    @Override
    public void flatMap1(Optional<Assignment> fact, Collector<Optional<Assignment>> collector) throws Exception {
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
            for(Assignment rhs : this.mbuf2.snd.get(0)){
                Optional<Assignment> joinResult = join1(fact.get(), rhs);
                if(joinResult.isPresent()){
                    collector.collect(joinResult);
                }

            }
        }else{
            if(this.mbuf2.fst.size() < indexlhs + 1){
                this.mbuf2.fst.add(Table.empty());

            }
            this.mbuf2.fst.get(indexlhs).add(fact.get());
            if(this.mbuf2.snd.get(indexlhs) != null){
                for(Assignment rhs : this.mbuf2.snd.get(indexlhs)){
                    Optional<Assignment> joinResult = join1(fact.get(), rhs);
                    collector.collect(joinResult);
                }
            }
        }
    }

    @Override
    public void flatMap2(Optional<Assignment> fact, Collector<Optional<Assignment>> collector) throws Exception {
        //one terminator fact has to be sent out once it is received on both incoming streams!!
        if(!fact.isPresent()){
            terminatorRHS = true;
            indexrhs++;
            if(terminatorLHS){
                this.mbuf2.fst.remove(0);
                this.mbuf2.snd.remove(0);
                //does the way I structured the sets in the linkedlist make sense?
                collector.collect(fact);
                terminatorRHS = false;
                terminatorLHS = false;
            }
        }else if(!terminatorRHS){
            if(this.mbuf2.snd.size() == 0){
                this.mbuf2.snd.add(Table.empty());
            }
            this.mbuf2.snd.get(0).add(fact.get());
            for(Assignment lhs : this.mbuf2.fst.get(0)){
                Optional<Assignment> joinResult = join1(lhs, fact.get());
                collector.collect(joinResult);
            }
        }else{
            if(this.mbuf2.snd.size() < indexrhs + 1){
                //to make sure that there is something to get() here
                this.mbuf2.snd.add(Table.empty());
                //YOU CAN ALSO GET MORE THAN 1 TERMINATOR ON ONE SIDE!!
            }
            this.mbuf2.snd.get(indexrhs).add(fact.get());
            if(this.mbuf2.fst.get(indexrhs) != null){
                for(Assignment lhs : this.mbuf2.fst.get(indexrhs)){
                    Optional<Assignment> joinResult = join1(lhs, fact.get());
                    collector.collect(joinResult);
                }
            }
        }
    }

    public static Optional<Assignment> join1(Assignment aOp, Assignment bOp){
        if(true){
            Assignment a = aOp;
            Assignment b = bOp;
            if(a.size() == 0 && b.size() == 0) {
                Assignment emptyList = new Assignment();
                Optional<Assignment> result = Optional.of(emptyList);
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
                }else if(x.isPresent() && y.isPresent() || x!=y) {
                    if(!subResult.isPresent()) {
                        Optional<Assignment> result = Optional.empty();
                        return result;
                    }else {
                        if(x==y) { //should enter this clause automatically
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
        }
        return null; //not sure why this is necessary

    }

}
