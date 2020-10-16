package ch.ethz.infsec.src;


import ch.ethz.infsec.policy.Term;


public interface JavaTerm extends Term {
    public <T> T accept(FormulaVisitor<T> v);


}
