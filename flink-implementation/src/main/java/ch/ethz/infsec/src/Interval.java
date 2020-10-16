package ch.ethz.infsec.src;



public class Interval {
    Integer left;
    Integer right;
    //nat and enat not yet expressed accurately

    public Interval(Integer left, Integer right) {
        this.left = left;
        this.right = right;
    }

    public boolean mem(Integer n) {
        if(this.left <= n && n <= this.right) {
            return true;
        }
        return false;
    }
}
