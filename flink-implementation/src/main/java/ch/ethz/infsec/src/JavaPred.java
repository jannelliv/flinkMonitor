package ch.ethz.infsec.src;

import ch.ethz.infsec.policy.Pred;
import scala.collection.Seq;


public abstract class JavaPred extends Pred implements JavaGenFormula {

    public JavaPred(String relation, Seq args) {
        super(relation, args);
    }
    public <T> T accept(FormulaVisitor<T> v) {
        return v.visit(this);
    }
}