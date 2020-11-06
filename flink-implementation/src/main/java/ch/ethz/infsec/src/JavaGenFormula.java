package ch.ethz.infsec.src;
import ch.ethz.infsec.policy.*;

public interface JavaGenFormula<T> extends JavaGenFormulaUnsealed<T> {
    <R> R accept(FormulaVisitor<R> v);

    static <T> JavaGenFormula<T> convert(GenFormula<T> gf){

        if(gf instanceof Ex){ //if I included the generic type in the if, I got an error message
            return new JavaEx<>(((Ex<T>) gf).variable(), ((Ex<T>) gf).arg());
        }else if(gf instanceof All){
            return new JavaAll<>(((All<T>) gf).variable(), ((All<T>) gf).arg());
        }else if(gf instanceof And){
            return new JavaAnd<>(((And<T>) gf).arg1(), ((And<T>) gf).arg2());
        }else if(gf instanceof False){
            return new JavaFalse<>();
        }else if(gf instanceof True){
            return new JavaTrue<>();
        }else if(gf instanceof Next){
            return new JavaNext<>(((Next<T>) gf).interval(), ((Next<T>) gf).arg());
        }else if(gf instanceof Not){
            return new JavaNot<>(((Not<T>) gf).arg());
        }else if(gf instanceof Or){
            return new JavaOr<>(((Or<T>) gf).arg1(), ((Or<T>) gf).arg2());
        }else if(gf instanceof Pred){
            return new JavaPred<>(((Pred<T>) gf).relation(),((Pred<T>) gf).args());
        }else if(gf instanceof JavaPrev){
            return new JavaPrev<>(((JavaPrev<T>) gf).interval(), ((JavaPrev<T>) gf).arg());
        }else if(gf instanceof JavaSince){
            return new JavaSince<>(((JavaSince<T>) gf).interval(), ((JavaSince<T>) gf).arg1(), ((JavaSince<T>) gf).arg2());
        }else if(gf instanceof JavaUntil){
            return new JavaUntil<>(((JavaUntil<T>) gf).interval(), ((JavaUntil<T>) gf).arg1(), ((JavaUntil<T>) gf).arg2());
        }else{
            return null;
        }
    }

}




