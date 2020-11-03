package ch.ethz.infsec.src;

import ch.ethz.infsec.policy.GenFormula;
import ch.ethz.infsec.policy.Or;

public abstract class JavaOr extends Or implements JavaGenFormula {

    public JavaOr(GenFormula arg1, GenFormula arg2) {
        //Why did I get an error "Modifier 'public' not allowed here"?
        super(arg1, arg2);
    }

    public <T> T accept(FormulaVisitor<T> v) {
        return v.visit(this);
    }

    @Override
    public boolean canEqual(Object that) {
        return false;
    }

    @Override
    public Object productElement(int n) {
        return null;
    }

    @Override
    public int productArity() {
        return 0;
    }
}