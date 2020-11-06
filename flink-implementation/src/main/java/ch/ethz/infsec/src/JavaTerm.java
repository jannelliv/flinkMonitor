package ch.ethz.infsec.src;


import ch.ethz.infsec.policy.JavaTermUnsealed;

public interface JavaTerm<T> extends JavaTermUnsealed<T> {
    <R> R accept(FormulaVisitor<R> v);


}
