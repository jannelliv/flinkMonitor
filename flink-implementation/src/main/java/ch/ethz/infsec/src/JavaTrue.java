package ch.ethz.infsec.src;

import ch.ethz.infsec.policy.True;

public class JavaTrue<T> extends True<T> implements JavaGenFormula<T> {

    public <T> T accept(FormulaVisitor<T> v) {
        return v.visit(this);
    }

    @Override
    public Object productElement(int n) {
        return null;
    }

    @Override
    public int productArity() {
        return 0;
    }

    @Override
    public boolean canEqual(Object that) {
        return false;
    }


}
