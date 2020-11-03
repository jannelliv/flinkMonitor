package ch.ethz.infsec.src;

public interface FormulaVisitor<T> {
    T visit(JavaPred f);
    T visit(JavaNot f);
    T visit(JavaAnd f);
    T visit(JavaAll f);
    T visit(JavaEx f);
    T visit(JavaFalse f);
    T visit(JavaTrue f);
    T visit(JavaNext f);
    T visit(JavaOr f);
    T visit(JavaPrev f);
    T visit(JavaSince f);
    T visit(JavaUntil f);

}
