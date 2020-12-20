package ch.ethz.infsec.term;


import ch.ethz.infsec.policy.*;
import ch.ethz.infsec.term.visitor.TermVisitor;

public interface JavaTerm<T> extends JavaTermUnsealed<T> {
    <R> R accept(TermVisitor<R> v);

    static <T> JavaTerm<T> convert(Term<T> gf){

        if(gf instanceof Const){
            return new JavaConst<>((T) ((Const<T>) gf).value());
        }else if(gf instanceof Var){
            return new JavaVar<>(((Var<T>) gf).variable());
        }else{
            return null;
        }
    }


}
