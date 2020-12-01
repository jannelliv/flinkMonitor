package ch.ethz.infsec.src.formula;

import ch.ethz.infsec.policy.And;
import ch.ethz.infsec.policy.GenFormula;
import ch.ethz.infsec.policy.VariableID;
import ch.ethz.infsec.src.formula.visitor.FormulaVisitor;
import static ch.ethz.infsec.src.formula.JavaGenFormula.convert;

public class JavaAnd<T> extends And<T> implements JavaGenFormula<T> {

    public JavaAnd(GenFormula<T> arg1, GenFormula<T> arg2) {
        super(arg1, arg2);
    }

    public <U> U accept(FormulaVisitor<U> v) {
        return v.visit((JavaAnd<VariableID>) this);
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