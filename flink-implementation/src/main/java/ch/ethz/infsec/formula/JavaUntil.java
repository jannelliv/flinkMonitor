package ch.ethz.infsec.formula;
import ch.ethz.infsec.policy.GenFormula;
import ch.ethz.infsec.policy.Interval;
import ch.ethz.infsec.policy.Until;
import ch.ethz.infsec.policy.VariableID;

import ch.ethz.infsec.formula.visitor.*;
import static ch.ethz.infsec.formula.JavaGenFormula.convert;

public class JavaUntil<T> extends Until<T> implements JavaGenFormula<T> {

    public JavaUntil(Interval interval, GenFormula<T> arg1, GenFormula<T> arg2) {
        super(interval, arg1, arg2);
    }
    public <R> R accept(FormulaVisitor<R> v) {
        return v.visit((JavaUntil<VariableID>) this);
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
