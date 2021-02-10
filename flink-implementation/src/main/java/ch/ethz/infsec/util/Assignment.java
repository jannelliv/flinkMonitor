package ch.ethz.infsec.util;

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
        el.addAll(list);
        return el;
    }

    public static Assignment one(Optional<Object> elem){
        List<Optional<Object>> list = new LinkedList<>();
        list.add(elem);
        Assignment el = new Assignment();
        el.addAll(list);
        return el;
    }

    public static Assignment one(){
        return new Assignment();
    }


    @Override
    public String toString() {
        return "(" + this.stream().map(o -> {
            return o.orElseGet(Optional::empty);
        }).map(Object::toString).collect(Collectors.joining(", ")) + ")";
    }

    @Override
    public boolean equals(Object o){
        if(this == o){
            return true;
        }
        if(!(o instanceof Assignment)){
            return false;
        }
        Assignment assig = (Assignment) o;
        if(this.size() != assig.size()){
            return false;
        }
        for(int i = 0; i < this.size(); i++){
            if(Boolean.compare(this.get(i).isPresent(), assig.get(i).isPresent()) != 0){
                return false;
            }
            if(this.get(i).isPresent() && !this.get(i).get().toString().equals(assig.get(i).get().toString())){
                return false;
            }
        }
        return true;
    }

}






