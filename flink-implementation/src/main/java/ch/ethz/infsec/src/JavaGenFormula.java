package ch.ethz.infsec.src;
import ch.ethz.infsec.policy.JavaGenFormulaUnsealed;

public interface JavaGenFormula<T> extends JavaGenFormulaUnsealed<T> {
    <T> T accept(FormulaVisitor<T> v);

}




