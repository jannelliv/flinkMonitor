package ch.ethz.infsec.formula;
import ch.ethz.infsec.policy.*;
import ch.ethz.infsec.formula.visitor.FormulaVisitor;

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
        }else if(gf instanceof Prev){
            return new JavaPrev<>(((Prev<T>) gf).interval(), ((Prev<T>) gf).arg());
        }else if(gf instanceof Since){
            return new JavaSince<>(((Since<T>) gf).interval(), ((Since<T>) gf).arg1(), ((Since<T>) gf).arg2());
        }else if(gf instanceof Until){
            return new JavaUntil<>(((Until<T>) gf).interval(), ((Until<T>) gf).arg1(), ((Until<T>) gf).arg2());
        }else if(gf instanceof Eventually){
            return new JavaEventually<>(((Eventually<T>) gf).interval(), ((Eventually<T>) gf).arg());
        }else if(gf instanceof Once){
            return new JavaOnce<>(((Once<T>) gf).interval(), ((Once<T>) gf).arg());
        }else{
            return null;
        }
    }

}




