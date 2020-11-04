package ch.ethz.infsec.src;

import ch.ethz.infsec.policy.All;
import ch.ethz.infsec.policy.GenFormula;

import static ch.ethz.infsec.src.Init0.convert;

public class JavaAll<T> extends All<T> implements JavaGenFormula<T> {

    public JavaAll(T variable, GenFormula arg) {
        super(variable, arg);
    }

    public <T> T accept(FormulaVisitor<T> v) {
        return v.visit(this);
    }

    @Override
    public JavaGenFormula<T> arg(){
        return convert(super.arg());
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
