package ch.ethz.infsec.src;


import ch.ethz.infsec.policy.Var;


public abstract class JavaVar extends Var implements JavaTerm {


    public JavaVar(Object variable) {
        super(variable);
    }
}
