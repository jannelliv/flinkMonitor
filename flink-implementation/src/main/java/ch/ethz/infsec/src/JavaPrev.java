package ch.ethz.infsec.src;

import ch.ethz.infsec.policy.GenFormula;
import ch.ethz.infsec.policy.Interval;
import ch.ethz.infsec.policy.Prev;

import static ch.ethz.infsec.src.Init0.convert;

public class JavaPrev<T> extends Prev<T> implements JavaGenFormula<T> {


    public JavaPrev(Interval interval, GenFormula<T> arg) {
        super(interval, arg);
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