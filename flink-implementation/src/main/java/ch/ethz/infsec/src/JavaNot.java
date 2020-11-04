package ch.ethz.infsec.src;

import ch.ethz.infsec.policy.GenFormula;
import ch.ethz.infsec.policy.Not;

import static ch.ethz.infsec.src.Init0.convert;

public class JavaNot<T> extends Not<T> implements JavaGenFormula<T> {

    public JavaNot(GenFormula<T> arg) {
        super(arg);
    }

    public <T> T accept(FormulaVisitor<T> v) {
        return v.visit(this);
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