package ch.ethz.infsec.term;

import ch.ethz.infsec.policy.Const;
import ch.ethz.infsec.term.visitor.TermVisitor;

public class JavaConst<T> extends Const<T> implements JavaTerm<T> {

    public JavaConst(T value) {
        super(value);
    }


    @Override
    public <R> R accept(TermVisitor<R> v) {
        return v.visit(this);
    }

    @Override
    public T productElement(int n) {
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
