package ch.ethz.infsec.formula;

import ch.ethz.infsec.policy.True;
import ch.ethz.infsec.policy.VariableID;
import ch.ethz.infsec.formula.visitor.FormulaVisitor;

public class JavaTrue<T> extends True<T> implements JavaGenFormula<T> {

    public <R> R accept(FormulaVisitor<R> v) {
        return v.visit((JavaTrue<VariableID>) this);
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
