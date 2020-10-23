package ch.ethz.infsec.src;


import ch.ethz.infsec.policy.JavaTermUnsealed;
import ch.ethz.infsec.policy.Term;


public interface JavaTerm<T> extends JavaTermUnsealed<T> {
    public <T> T accept(FormulaVisitor<T> v);


}
