package ch.ethz.infsec.src.formula;

import ch.ethz.infsec.policy.GenFormula;
import ch.ethz.infsec.policy.Interval;
import ch.ethz.infsec.policy.Prev;
import ch.ethz.infsec.policy.VariableID;

import ch.ethz.infsec.src.formula.visitor.FormulaVisitor;
import static ch.ethz.infsec.src.formula.JavaGenFormula.convert;

public class JavaPrev<T> extends Prev<T> implements JavaGenFormula<T> {


    public JavaPrev(Interval interval, GenFormula<T> arg) {
        super(interval, arg);
    }
    public <R> R accept(FormulaVisitor<R> v) {
        return v.visit((JavaPrev<VariableID>) this);
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