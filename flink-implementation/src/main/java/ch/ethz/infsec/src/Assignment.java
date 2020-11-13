package ch.ethz.infsec.src;

import java.util.*;

public class Assignment extends LinkedList<Optional<Object>> {
    public static Assignment nones(int n){
        Assignment el = new Assignment();
        for(int i = 0; i < n; i++){
            //not sure if this is efficient
            el.add(Optional.empty());
        }
        return el;
    }

}

class Table extends HashSet<Assignment>{
    public static Table empty(){
        return new Table();
    }
    public static Table one(Assignment a){
        Table t = new Table();
        t.add(a);
        return t;
    }
}


