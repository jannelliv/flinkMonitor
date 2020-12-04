package ch.ethz.infsec.src.util;

import java.util.*;
import java.util.stream.Collectors;
//this and the toString method make the assignment readable in the output

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

        //not sure if this is efficient
        el.addAll(list);
        return el;
    }

    /*public static Assignment terminator(long timestamp){

        return new Assignment(timestamp, true);
        //boolean is set to true and the list is empty,
        // so the appropriate static constructor is used

    }*/

    @Override
    public String toString() {
        return "(" + this.stream().map(o -> {
            return o.orElseGet(Optional::empty);
            //check if this is correct
        }).map(Object::toString).collect(Collectors.joining(", ")) + ")";
    }

}






