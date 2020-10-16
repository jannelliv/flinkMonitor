package ch.ethz.infsec.src;

import ch.ethz.infsec.policy.All;
import ch.ethz.infsec.policy.GenFormula;


public abstract class JavaAll extends All implements JavaGenFormula {

    public JavaAll(Object variable, GenFormula arg) {
        super(variable, arg);
    }

    public <T> T accept(FormulaVisitor<T> v) {
        return v.visit(this);
    }
}
