package ch.ethz.infsec.src;

import ch.ethz.infsec.policy.Pred;
import scala.collection.Seq;

import static ch.ethz.infsec.src.Init0.convert;

public class JavaPred<T> extends Pred<T> implements JavaGenFormula<T> {

    public JavaPred(String relation, Seq args) {
        super(relation, args);
    }
    //problem with Seq bein raw here above
    public <T> T accept(FormulaVisitor<T> v) {
        return v.visit(this);
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