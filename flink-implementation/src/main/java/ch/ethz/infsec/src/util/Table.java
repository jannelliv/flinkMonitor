package ch.ethz.infsec.src.util;


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
}