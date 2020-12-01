package ch.ethz.infsec.src.formula;

import ch.ethz.infsec.policy.Pred;
import ch.ethz.infsec.policy.VariableID;
import scala.collection.Seq;

import ch.ethz.infsec.src.formula.visitor.FormulaVisitor;

public class JavaPred<T> extends Pred<T> implements JavaGenFormula<T> {

    public JavaPred(String relation, Seq args) {
        super(relation, args);
    }

    public <R> R accept(FormulaVisitor<R> v) {
        return v.visit((JavaPred<VariableID>) this);
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