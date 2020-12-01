package ch.ethz.infsec.src.term.visitor;


import ch.ethz.infsec.src.term.JavaConst;
import ch.ethz.infsec.src.term.JavaVar;

public interface TermVisitor<T> {
	    T visit(JavaVar f);
	    T visit(JavaConst f);
}
