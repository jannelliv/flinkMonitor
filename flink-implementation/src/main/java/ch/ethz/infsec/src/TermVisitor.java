package ch.ethz.infsec.src;

public interface TermVisitor<T> {
    T visit(JavaVar f);
    T visit(JavaConst f);
}