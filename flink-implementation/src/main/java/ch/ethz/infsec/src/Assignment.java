package ch.ethz.infsec.src;

import ch.ethz.infsec.monitor.Fact;

import java.util.*;
// We need to distinguish btw pure assignments and assignments which we use to encode the trace
public class Assignment extends LinkedList<Optional<Object>> {

    //now we have a non-static terminator for checking
    public static Assignment nones(int n){
        Assignment el = new Assignment();

        for(int i = 0; i < n; i++){
            //not sure if this is efficient
            el.add(Optional.empty());
        }
        return el;
    }

    public static Assignment someAssignment(List<Optional<Object>> list){
        assert(list != null);
        Assignment el = new Assignment();

        for(int i = 0; i < list.size(); i++){
            //not sure if this is efficient
            el.add(list.get(i));
        }
        return el;
    }

    /*public static Assignment terminator(long timestamp){

        return new Assignment(timestamp, true);
        //boolean is set to true and the list is empty,
        // so the appropriate static constructor is used

    }*/

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




