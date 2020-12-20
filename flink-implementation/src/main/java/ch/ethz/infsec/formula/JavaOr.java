package ch.ethz.infsec.formula;

import ch.ethz.infsec.policy.GenFormula;
import ch.ethz.infsec.policy.Or;
import ch.ethz.infsec.policy.VariableID;

import ch.ethz.infsec.formula.visitor.FormulaVisitor;
import static ch.ethz.infsec.formula.JavaGenFormula.convert;

public class JavaOr<T> extends Or<T> implements JavaGenFormula<T> {

    public JavaOr(GenFormula<T> arg1, GenFormula<T> arg2) {
        super(arg1, arg2);
    }

    public <R> R accept(FormulaVisitor<R> v) {
        return v.visit((JavaOr<VariableID>) this);
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