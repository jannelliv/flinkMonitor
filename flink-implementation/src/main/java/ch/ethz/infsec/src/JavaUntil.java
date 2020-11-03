package ch.ethz.infsec.src;
import ch.ethz.infsec.policy.GenFormula;
import ch.ethz.infsec.policy.Interval;
import ch.ethz.infsec.policy.Until;

public abstract class JavaUntil extends Until implements JavaGenFormula {

    public JavaUntil(Interval interval, GenFormula arg1, GenFormula arg2) {
        super(interval, arg1, arg2);
    }
    public <T> T accept(FormulaVisitor<T> v) {
        return v.visit(this);
    }
}
