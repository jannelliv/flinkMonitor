package ch.ethz.infsec.util;


import java.util.HashSet;
import java.util.Optional;

public class Table extends HashSet<Assignment> {
    public static Table empty(){
        return new Table();
    }

    @Override
    public boolean isEmpty() {
        return super.isEmpty();
    }

    public static Table one(Assignment a){
        Table t = new Table();
        t.add(a);
        return t;
    }

    public static Table fromTable(Table a){
        Table t = new Table();
        for(Assignment ass : a){
            t.add(ass);
        }
        return t;
    }

    public static Table fromSet(HashSet<Assignment> a){
        Table t = new Table();
        for(Assignment ass : a){
            t.add(ass);
        }
        return t;
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

    public static Table join(java.util.HashSet<Assignment> table, boolean pos, java.util.HashSet<Assignment> table2){

        java.util.HashSet<Assignment> result = new java.util.HashSet<>();
        assert(table != null && table2 != null);
        for(Assignment op1 : table){
            for (Assignment optionals : table2) {
                Optional<Assignment> tupleRes = join1(op1, optionals, 0);
                if (tupleRes.isPresent()) {
                    Assignment tuple = tupleRes.get();
                    result.add(tuple);
                }
            }
        }
        if(pos) {
            return Table.fromSet(result);
        }else {
            table.removeAll(result);
            return Table.fromSet(table);
        }
    }
}