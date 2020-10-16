package ch.ethz.infsec.src;

import ch.ethz.infsec.policy.GenFormula;
import ch.ethz.infsec.policy.Interval;
import ch.ethz.infsec.policy.Next;


public abstract class JavaNext extends Next implements JavaGenFormula {

    public JavaNext(Interval interval, GenFormula arg) {
        super(interval, arg);
    }
    public <T> T accept(FormulaVisitor<T> v) {
        return v.visit(this);
    }
}
