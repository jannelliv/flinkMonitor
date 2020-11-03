package ch.ethz.infsec.src;

import ch.ethz.infsec.policy.GenFormula;
import ch.ethz.infsec.policy.Interval;
import ch.ethz.infsec.policy.Prev;

public abstract class JavaPrev extends Prev implements JavaGenFormula {


    public JavaPrev(Interval interval, GenFormula arg) {
        super(interval, arg);
    }
    public <T> T accept(FormulaVisitor<T> v) {
        return v.visit(this);
    }
}