package ch.ethz.infsec.src;

import ch.ethz.infsec.policy.Const;

import static ch.ethz.infsec.src.Init0.convert;


public class JavaConst<T> extends Const<T> implements JavaTerm<T> {

    public JavaConst(Object value) {
        super(value);
    }

    /*@Override
    public Object value(){
        return convertTerm(super.value());
    }*/

    @Override
    public Object accept(FormulaVisitor v) {
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
