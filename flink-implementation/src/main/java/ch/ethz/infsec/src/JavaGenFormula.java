package ch.ethz.infsec.src;
import ch.ethz.infsec.policy.JavaGenFormulaUnsealed;

import java.util.List;
import java.util.Set;

public interface JavaGenFormula extends JavaGenFormulaUnsealed {
    <T> T accept(FormulaVisitor<T> v);

    void setFreeVars(Set<Object> fv, List<Object> fvio);

}




