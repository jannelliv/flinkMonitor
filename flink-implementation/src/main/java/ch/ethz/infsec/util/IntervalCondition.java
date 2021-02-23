package ch.ethz.infsec.util;

public class IntervalCondition {

    public static boolean mem2(Long n, ch.ethz.infsec.policy.Interval I){
        if(I.lower() <= n.intValue() && (!I.upper().isDefined() || (I.upper().isDefined() && n.intValue() <= ((int)I.upper().get())))){
            return true;
        }
        return false;
    }
}
