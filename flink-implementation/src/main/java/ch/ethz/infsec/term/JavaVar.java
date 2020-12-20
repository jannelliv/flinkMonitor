package ch.ethz.infsec.term;


import ch.ethz.infsec.policy.Var;
import ch.ethz.infsec.term.visitor.TermVisitor;

public class JavaVar<T> extends Var<T> implements JavaTerm<T> {
    public JavaVar(T variable) {
        super(variable);
    }

    @Override
    public <T1> T1 accept(TermVisitor<T1> v) {
        return null;
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
