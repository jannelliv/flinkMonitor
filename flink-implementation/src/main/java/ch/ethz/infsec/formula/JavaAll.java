package ch.ethz.infsec.formula;

import ch.ethz.infsec.policy.All;
import ch.ethz.infsec.policy.GenFormula;
import ch.ethz.infsec.policy.VariableID;

import ch.ethz.infsec.formula.visitor.FormulaVisitor;


import static ch.ethz.infsec.formula.JavaGenFormula.convert;

public class JavaAll<T> extends All<T> implements JavaGenFormula<T> {

    public JavaAll(T variable, GenFormula arg) {
        super(variable, arg);
    }

    public <U> U accept(FormulaVisitor<U> v) {
        return v.visit((JavaAll<VariableID>) this);
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
