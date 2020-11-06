package ch.ethz.infsec.src;

import ch.ethz.infsec.policy.False;
import ch.ethz.infsec.policy.VariableID;

public class JavaFalse<T> extends False<T> implements JavaGenFormula<T> {

    public <U> U accept(FormulaVisitor<U> v) {
        return v.visit((JavaFalse<VariableID>) this);
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