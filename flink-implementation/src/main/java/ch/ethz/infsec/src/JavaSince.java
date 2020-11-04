package ch.ethz.infsec.src;

import ch.ethz.infsec.policy.GenFormula;
import ch.ethz.infsec.policy.Interval;
import ch.ethz.infsec.policy.Since;

import static ch.ethz.infsec.src.Init0.convert;

public class JavaSince<T> extends Since<T> implements JavaGenFormula<T> {

    public JavaSince(Interval interval, GenFormula<T> arg1, GenFormula<T> arg2) {
        super(interval, arg1, arg2);
    }
    public <T> T accept(FormulaVisitor<T> v) {
        return v.visit(this);
    }

    @Override
    public JavaGenFormula<T> arg1(){
        return convert(super.arg1());
    }

    @Override
    public JavaGenFormula<T> arg2(){
        return convert(super.arg2());
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