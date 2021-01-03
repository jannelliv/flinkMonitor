package ch.ethz.infsec.util;


public class PipelineEvent  {

    private final long timestamp;
    private final long timepoint;
    private final boolean isTerminator;
    private final Assignment assignment;
    //now we have a non-static terminator for checking


    public PipelineEvent(long timestamp, long timepoint, boolean isTerminator, Assignment assignment) {

        this.isTerminator = isTerminator;
        this.timestamp = timestamp;
        this.assignment = assignment;
        this.timepoint = timepoint;

    }

    public boolean isPresent(){
        return !isTerminator;
    }

    public Assignment get(){
        return this.assignment;
    }

    public long getTimestamp(){
        return this.timestamp;
    }

    public long getTimepoint(){
        return this.timepoint;
    }

    @Override
    public boolean equals(Object o){
        if(o == this){
            return true;
        }
        if(!(o instanceof PipelineEvent)){
            return false;
        }
        PipelineEvent pe = (PipelineEvent) o;
        if(this.isTerminator != pe.isTerminator){
            return false;
        }else{
            if(!this.isTerminator){
                return this.timestamp == pe.timestamp
                        && this.timepoint == pe.timepoint
                        && this.get().equals(pe.get());
            }else{
                return this.timestamp == pe.timestamp
                        && this.timepoint == pe.timepoint;
            }
        }


    }

    @Override
    public String toString() {
        if (isTerminator()) {
            return "@" + timestamp + " : " + timepoint;
        } else {
            return "@" + timestamp + " : " + timepoint + " " + assignment.toString();
        }
    }

    private boolean isTerminator() {
        return isTerminator;
    }

    /*public static PipelineEvent nones(int n, long timestamp){
        PipelineEvent el = new PipelineEvent(timestamp, false);
        el.timestamp = timestamp;
        for(int i = 0; i < n; i++){
            //not sure if this is efficient
            el.add(Optional.empty());
        }
        return el;
    }*/

    /*public static Assignment someAssignment(long timestamp, List<Optional<Object>> list){
        assert(list != null);
        Assignment el = new Assignment(timestamp, false, list);
        return el;
    }

    public static Assignment terminator(long timestamp){

        return new Assignment(timestamp, true);
        //boolean is set to true and the list is empty,
        // so the appropriate static constructor is used

    }*/

}