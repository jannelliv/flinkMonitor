package ch.ethz.infsec.term.visitor;


import ch.ethz.infsec.term.JavaConst;
import ch.ethz.infsec.term.JavaVar;

public interface TermVisitor<T> {
	    T visit(JavaVar f);
	    T visit(JavaConst f);
}
