package ch.ethz.infsec.util;


import java.util.HashSet;

public class Table extends HashSet<Assignment> {
    public static Table empty(){
        return new Table();
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
}