package ch.ethz.infsec.src;


import ch.ethz.infsec.policy.JavaTermUnsealed;

public interface JavaTerm<T> extends JavaTermUnsealed<T> {
    <T> T accept(FormulaVisitor<T> v);


}
