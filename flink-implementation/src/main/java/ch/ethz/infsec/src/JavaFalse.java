package ch.ethz.infsec.src;

import ch.ethz.infsec.policy.False;

public abstract class JavaFalse extends False implements JavaGenFormula {
    public <T> T accept(FormulaVisitor<T> v) {
        return v.visit(this);
    }
}