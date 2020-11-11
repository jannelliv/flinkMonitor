package ch.ethz.infsec.src;


import ch.ethz.infsec.policy.*;

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
