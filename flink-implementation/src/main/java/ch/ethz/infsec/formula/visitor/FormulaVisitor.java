package ch.ethz.infsec.formula.visitor;

import ch.ethz.infsec.policy.VariableID;

import ch.ethz.infsec.formula.*;

public interface FormulaVisitor<T> {
    T visit(JavaPred<VariableID> f);
    T visit(JavaNot<VariableID> f);
    T visit(JavaAnd<VariableID> f);
    T visit(JavaAll<VariableID> f);
    T visit(JavaEx<VariableID> f);
    T visit(JavaFalse<VariableID> f);
    T visit(JavaTrue<VariableID> f);
    T visit(JavaNext<VariableID> f);
    T visit(JavaOr<VariableID> f);
    T visit(JavaPrev<VariableID> f);
    T visit(JavaSince<VariableID> f);
    T visit(JavaUntil<VariableID> f);
    T visit(JavaOnce<VariableID> f);
    T visit(JavaEventually<VariableID> f);
}
