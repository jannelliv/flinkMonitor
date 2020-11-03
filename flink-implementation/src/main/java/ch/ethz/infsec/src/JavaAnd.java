package ch.ethz.infsec.src;

import ch.ethz.infsec.policy.And;
import ch.ethz.infsec.policy.GenFormula;

public abstract class JavaAnd extends And implements JavaGenFormula {

    public JavaAnd(GenFormula arg1, GenFormula arg2) {
        super(arg1, arg2);
    }

    public <T> T accept(FormulaVisitor<T> v) {
        return v.visit(this);
    }
}