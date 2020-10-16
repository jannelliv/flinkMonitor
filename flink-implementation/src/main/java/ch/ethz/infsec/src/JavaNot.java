package ch.ethz.infsec.src;

import ch.ethz.infsec.policy.GenFormula;
import ch.ethz.infsec.policy.Not;


public abstract class JavaNot extends Not implements JavaGenFormula {

    public JavaNot(GenFormula arg) {
        super(arg);
    }

    public <T> T accept(FormulaVisitor<T> v) {
        return v.visit(this);
    }
}