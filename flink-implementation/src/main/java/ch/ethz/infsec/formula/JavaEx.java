package ch.ethz.infsec.formula;

import ch.ethz.infsec.policy.*;
import scala.collection.Iterator;
import scala.collection.Seq;
import scala.collection.immutable.List;
import scala.collection.immutable.Set;

import ch.ethz.infsec.formula.visitor.FormulaVisitor;
import static ch.ethz.infsec.formula.JavaGenFormula.convert;

public class JavaEx<T> extends Ex<T> implements JavaGenFormula<T> {

    public JavaEx(T variable, GenFormula<T> arg) {
        super(variable, arg);
    }

    public <U> U accept(FormulaVisitor<U> v) {
        return v.visit((JavaEx<VariableID>) this);
    }

    @Override
    public JavaGenFormula<T> arg(){
        return convert(super.arg());
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

    @Override
    public <V> Ex<V> copy(V variable, GenFormula<V> arg) {
        return super.copy(variable, arg);
    }

    @Override
    public Iterator<Object> productIterator() {
        return super.productIterator();
    }

    @Override
    public String productPrefix() {
        return super.productPrefix();
    }

    @Override
    public GenFormula<T> close(boolean neg) {
        return super.close(neg);
    }

    @Override
    public String toQTLString(boolean neg) {
        return super.toQTLString(neg);
    }

    @Override
    public T variable() {
        return super.variable();
    }

    @Override
    public Set<Pred<T>> atoms() {
        return super.atoms();
    }

    @Override
    public Seq<Pred<T>> atomsInOrder() {
        return super.atomsInOrder();
    }

    @Override
    public Set<T> freeVariables() {
        return super.freeVariables();
    }

    @Override
    public Seq<T> freeVariablesInOrder() {
        return super.freeVariablesInOrder();
    }

    @Override
    public <W> Ex<W> map(VariableMapper<T, W> mapper) {
        return super.map(mapper);
    }

    @Override
    public List<String> check() {
        return super.check();
    }

    @Override
    public String toString() {
        return super.toString();
    }

    @Override
    public String toQTL() {
        return super.toQTL();
    }
}