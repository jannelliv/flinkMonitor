package ch.ethz.infsec.src;

import ch.ethz.infsec.policy.GenFormula;
import ch.ethz.infsec.policy.Interval;
import ch.ethz.infsec.policy.Next;
import ch.ethz.infsec.policy.VariableID;

import static ch.ethz.infsec.src.JavaGenFormula.convert;

public class JavaNext<T> extends Next<T> implements JavaGenFormula<T> {

    public JavaNext(Interval interval, GenFormula arg) {
        super(interval, arg);
    }
    public <T> T accept(FormulaVisitor<T> v) {
        return v.visit((JavaNext<VariableID>) this);
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
