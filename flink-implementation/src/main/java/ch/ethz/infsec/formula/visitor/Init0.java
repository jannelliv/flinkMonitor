package ch.ethz.infsec.formula.visitor;
import ch.ethz.infsec.policy.*;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import java.util.*;
import ch.ethz.infsec.formula.*;
import ch.ethz.infsec.monitor.*;
import ch.ethz.infsec.util.*;


public class Init0 implements FormulaVisitor<Mformula> {

    List<VariableID> freeVariablesInOrder;

    public Init0(Seq<VariableID> fvio){
        ArrayList<VariableID> temp = new ArrayList<>(JavaConverters.seqAsJavaList(fvio));
        //If you would use List<VariableID> there, you could avoid the two conversions steps
        // (one before the recursive call and the other in the Init0 constructor).
        this.freeVariablesInOrder = temp;
    }

    public Mformula visit(JavaPred<VariableID> f){
        return new MPred(f.relation(), f.args(), this.freeVariablesInOrder);
    }


    public Mformula visit(JavaNot<VariableID> f) {
        if(f.arg() instanceof JavaOr){
            //don't know how to handle Eq
            if(((JavaOr<VariableID>) f.arg()).arg1() instanceof JavaNot){
                //check if arg2 is a safe_formula
                //make sure it's correct that you have
                ArrayList<Object> freeVarsInOrder1 = new ArrayList<>(JavaConverters.seqAsJavaList(((JavaOr<VariableID>) f.arg()).arg1().freeVariablesInOrder()));
                ArrayList<Object> freeVarsInOrder2 = new ArrayList<>(JavaConverters.seqAsJavaList(((JavaOr<VariableID>) f.arg()).arg2().freeVariablesInOrder()));
                boolean isSubset = freeVarsInOrder1.containsAll(freeVarsInOrder2);
                if(isSubset && safe_formula(((JavaOr<VariableID>) f.arg()).arg2())){

                    return new MAnd((((JavaOr<VariableID>)f.arg()).arg1()).accept(new Init0((((JavaOr<VariableID>) f.arg()).arg1()).freeVariablesInOrder())),
                            false, (((JavaOr<VariableID>) f.arg()).arg2()).accept(new Init0((((JavaOr<VariableID>)f.arg()).arg2()).freeVariablesInOrder())));
                }else{
                    if(((JavaOr<VariableID>)f.arg()).arg2() instanceof JavaNot){
                        return new MAnd((((JavaOr<VariableID>)f.arg()).arg1()).accept(new Init0((((JavaOr<VariableID>) f.arg()).arg1()).freeVariablesInOrder())),
                                false, (((JavaOr<VariableID>) f.arg()).arg2()).accept(new Init0((((JavaOr<VariableID>) f.arg()).arg2()).freeVariablesInOrder())));
                    }else{
                        return null;
                    }
                }
            }else{
                return null;  //"undefined" in Isabelle
            }
        }else{
            return null; //"undefined" in Isabelle
        }


    }

    public Mformula visit(JavaAnd<VariableID> f) {
        JavaGenFormula<VariableID> arg1 = f.arg1();
        JavaGenFormula<VariableID> arg2 = f.arg2();
        if(safe_formula(arg2)){
            return new MAnd(arg1.accept(new Init0(arg1.freeVariablesInOrder())), true, arg2.accept(new Init0(arg2.freeVariablesInOrder())));
        }else{
            ArrayList<Object> freeVarsInOrder1 = new ArrayList<>(JavaConverters.seqAsJavaList(arg1.freeVariablesInOrder()));
            ArrayList<Object> freeVarsInOrder2 = new ArrayList<>(JavaConverters.seqAsJavaList(arg2.freeVariablesInOrder()));
            boolean isSubset = freeVarsInOrder1.containsAll(freeVarsInOrder2);
            if(arg2 instanceof JavaNot && isSubset){

                return new MAnd(arg1.accept(new Init0(arg1.freeVariablesInOrder())), false, arg2.accept(new Init0(arg2.freeVariablesInOrder())));
            }else{
                return null;
            }
        }

    }

    public Mformula visit(JavaAll<VariableID> f) {
        return null;
    }

    public Mformula visit(JavaEx<VariableID> f) {
        //Explain how this procedure avoids using the de Bruijn indices
        VariableID variable = f.variable();
        List<VariableID> freeVariablesInOrderCopy = new ArrayList<>(this.freeVariablesInOrder);
        //we make a copy because Java is pass-by-reference.
        //It should also be a copy every time you pass this.freeVariablesInOrder! You have to
        //check this!
        freeVariablesInOrderCopy.add(0,variable);
        //the visitor for MExists has to extend the list of free variables by 1
        Seq<VariableID> fvios = JavaConverters.asScalaBufferConverter(freeVariablesInOrderCopy).asScala().toSeq();
        JavaGenFormula<VariableID> subformula = f.arg();
        return new MExists(subformula.accept(new Init0(fvios)), variable);
        //when you construct the Mformula with Init0 you start with the list of free variables of the top-level
        // formula which you pass around and prepend to it bound variables whenever you see JavaExists.
        // the variable you want to project is always going to be first in the list that you receive from the
        // upstream operator. So there is no need for indirect indexing -- you just want to pass the tail
        // of the list downstream.
    }

    public Mformula visit(JavaFalse<VariableID> f) {
        return new MRel(Table.empty()); // aka empty table
    }

    public Mformula visit(JavaTrue<VariableID> f) {
        int n = f.freeVariablesInOrder().size();
        return new MRel(Table.one(Assignment.nones(n)));
    }

    public Mformula visit(JavaNext<VariableID> f) {
        return new MNext(f.interval(), (f.arg()).accept(new Init0((f.arg()).freeVariablesInOrder())), true, new LinkedList<>());
    }

    public Mformula visit(JavaOr<VariableID> f) {
        //NOT SURE IF I HAD TO ADD THE ABOVE ELEMENTS
        return new MOr((f.arg1()).accept(new Init0((f.arg1()).freeVariablesInOrder())),
                (f.arg2()).accept(new Init0((f.arg2()).freeVariablesInOrder())));
    }

    public Mformula visit(JavaPrev<VariableID> f) {
        return new MPrev(f.interval(), (f.arg()).accept(new Init0((f.arg()).freeVariablesInOrder())), true, new LinkedList<>());

    }

    public Mformula visit(JavaSince<VariableID> f) {

        if(safe_formula(f.arg1())){
            return new MSince(true,
                    (f.arg1()).accept(new Init0((f.arg1()).freeVariablesInOrder())),
                    f.interval(),
                    (f.arg2()).accept(new Init0(f.arg2().freeVariablesInOrder())));
        }else{
            if((f.arg1()) instanceof JavaNot){
                return new MSince(false,
                        (f.arg1()).accept(new Init0((f.arg1()).freeVariablesInOrder())),
                        f.interval(),
                        (f.arg2()).accept(new Init0((f.arg2()).freeVariablesInOrder())));
            }else{
                return null;
            }
        }

    }

    public Mformula visit(JavaUntil<VariableID> f) {

        if(safe_formula(f.arg1())){
            return new MUntil(true,
                    (f.arg1()).accept(new Init0((f.arg1()).freeVariablesInOrder())),
                    f.interval(),
                    (f.arg2()).accept(new Init0((f.arg2()).freeVariablesInOrder())));
        }else{
            if((f.arg1()) instanceof JavaNot){
                return new MUntil(false,
                        (f.arg1()).accept(new Init0((f.arg1()).freeVariablesInOrder())),
                        f.interval(),
                        (f.arg2()).accept(new Init0((f.arg2()).freeVariablesInOrder())));
            }else{
                return null;
            }
        }

    }

    public static boolean safe_formula(JavaGenFormula<VariableID> form){
        if(form instanceof JavaPred){
            return true;
        }else if(form instanceof JavaNot && ((JavaNot<VariableID>)form).arg() instanceof JavaOr && ((JavaOr<VariableID>)((JavaNot<VariableID>)form).arg()).arg1() instanceof JavaNot){
            ArrayList<Object> freeVarsInOrder1 = new ArrayList<>(JavaConverters.seqAsJavaList(((JavaOr<VariableID>)((JavaNot<VariableID>)form).arg()).arg1().freeVariablesInOrder()));
            ArrayList<Object> freeVarsInOrder2 = new ArrayList<>(JavaConverters.seqAsJavaList(((JavaOr<VariableID>)((JavaNot<VariableID>)form).arg()).arg2().freeVariablesInOrder()));
            boolean isSubset = freeVarsInOrder1.containsAll(freeVarsInOrder2);
            boolean alternative = false;
            if(((JavaOr<VariableID>)((JavaNot<VariableID>)form).arg()).arg2() instanceof JavaNot){
                alternative = safe_formula(((JavaNot<VariableID>)((JavaOr<VariableID>)((JavaNot<VariableID>)form).arg()).arg2()).arg());
            }
            return ((safe_formula(((JavaOr<VariableID>)((JavaNot<VariableID>)form).arg()).arg1()) && safe_formula(((JavaOr<VariableID>)((JavaNot<VariableID>)form).arg()).arg2()) && isSubset) || alternative);
        }else if(form instanceof JavaOr){
            ArrayList<Object> freeVarsInOrder1 = new ArrayList<>(JavaConverters.seqAsJavaList(((JavaOr<VariableID>)form).arg1().freeVariablesInOrder()));
            ArrayList<Object> freeVarsInOrder2 = new ArrayList<>(JavaConverters.seqAsJavaList(((JavaOr<VariableID>)form).arg2().freeVariablesInOrder()));
            boolean isEqual = freeVarsInOrder1.containsAll(freeVarsInOrder2) && freeVarsInOrder2.containsAll(freeVarsInOrder1);
            return (safe_formula(((JavaOr<VariableID>)form).arg1()) && safe_formula(((JavaOr<VariableID>)form).arg2()) && isEqual);

        }else if(form instanceof JavaEx){
            return safe_formula(((JavaEx<VariableID>) form).arg());
        }else if(form instanceof JavaPrev){
            return safe_formula(((JavaPrev<VariableID>) form).arg());
        }else if(form instanceof JavaNext){
            return safe_formula(((JavaNext<VariableID>) form).arg());
        }else if(form instanceof JavaSince){
            ArrayList<Object> freeVarsInOrder1 = new ArrayList<>(JavaConverters.seqAsJavaList(((JavaSince<VariableID>) form).arg1().freeVariablesInOrder()));
            ArrayList<Object> freeVarsInOrder2 = new ArrayList<>(JavaConverters.seqAsJavaList(((JavaSince<VariableID>) form).arg2().freeVariablesInOrder()));
            boolean isSubset = freeVarsInOrder2.containsAll(freeVarsInOrder1);
            boolean sff = safe_formula(((JavaSince<VariableID>) form).arg1());
            boolean sfs = safe_formula(((JavaSince<VariableID>) form).arg2());
            boolean alt = false;
            if( ((JavaSince<VariableID>) form).arg1() instanceof JavaNot){
                alt = safe_formula(((JavaNot<VariableID>) ((JavaSince<VariableID>) form).arg1()).arg());
            }
            boolean sec = sff || alt && sfs;
            return isSubset && sec;
        }else if(form instanceof JavaUntil){
            ArrayList<Object> freeVarsInOrder1 = new ArrayList<>(JavaConverters.seqAsJavaList(((JavaUntil<VariableID>) form).arg1().freeVariablesInOrder()));
            ArrayList<Object> freeVarsInOrder2 = new ArrayList<>(JavaConverters.seqAsJavaList(((JavaUntil<VariableID>) form).arg2().freeVariablesInOrder()));
            boolean isSubset = freeVarsInOrder2.containsAll(freeVarsInOrder1);
            boolean sff = safe_formula(((JavaUntil<VariableID>) form).arg1());
            boolean sfs = safe_formula(((JavaUntil<VariableID>) form).arg2());
            boolean alt = false;
            if( ((JavaUntil<VariableID>) form).arg1() instanceof JavaNot){
                alt = safe_formula(((JavaNot<VariableID>) ((JavaUntil<VariableID>) form).arg1()).arg());
            }
            boolean sec = sff || alt && sfs;
            return isSubset && sec;
        }else{
            return false;
        }

    }

    /**
     * WARNING: this method doesn’t check the list size, so you better
     * be sure it contains at least one element.
     */
    public static List<VariableID> tail(List<VariableID> xs)
            throws IndexOutOfBoundsException, IllegalArgumentException {
        if(xs.size() < 1){
            return new LinkedList<>();
        }
        return new ArrayList<VariableID>(xs.subList(1, xs.size()));
        //sublist not serializable, so we have to use an arrayList
    }



}