package ch.ethz.infsec.src;

import ch.ethz.infsec.policy.Ex;
import ch.ethz.infsec.policy.GenFormula;


public abstract class JavaEx extends Ex implements JavaGenFormula {

    public JavaEx(Object variable, GenFormula arg) {
        super(variable, arg);
    }

    public <T> T accept(FormulaVisitor<T> v) {
        return v.visit(this);
    }
}