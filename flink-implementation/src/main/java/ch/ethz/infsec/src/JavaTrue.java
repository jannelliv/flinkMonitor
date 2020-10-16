package ch.ethz.infsec.src;

import ch.ethz.infsec.policy.True;

public abstract class JavaTrue extends True implements JavaGenFormula {
    public <T> T accept(FormulaVisitor<T> v) {
        return v.visit(this);
    }
}
