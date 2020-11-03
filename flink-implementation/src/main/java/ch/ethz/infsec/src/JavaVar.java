package ch.ethz.infsec.src;


import ch.ethz.infsec.policy.Var;


public abstract class JavaVar<T> extends Var<T> implements JavaTerm<T> {
    public JavaVar(T variable) {
        super(variable);
    }
}
