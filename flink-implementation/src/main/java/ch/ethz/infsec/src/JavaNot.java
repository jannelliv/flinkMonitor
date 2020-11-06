package ch.ethz.infsec.src;

import ch.ethz.infsec.policy.GenFormula;
import ch.ethz.infsec.policy.Not;
import ch.ethz.infsec.policy.VariableID;

import static ch.ethz.infsec.src.JavaGenFormula.convert;

public class JavaNot<T> extends Not<T> implements JavaGenFormula<T> {

    public JavaNot(GenFormula<T> arg) {
        super(arg);
    }

    public <R> R accept(FormulaVisitor<R> v) {
        return v.visit((JavaNot<VariableID>) this);
    }

    @Override
    public JavaGenFormula<T> arg(){
        return convert(super.arg());
    }

    @Override
    public boolean canEqual(Object that) {
        return false;
    }

    @Override
    public Object productElement(int n) {
        return null;
    }

    @Override
    public int productArity() {
        return 0;
    }
}