package ch.ethz.infsec.src;
import ch.ethz.infsec.policy.*;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import java.util.*;


public class Init0 implements FormulaVisitor<ch.ethz.infsec.src.Mformula> {

    ArrayList<VariableID> freeVariablesInOrder;

    public Init0(Seq<VariableID> fvio){
        this.freeVariablesInOrder = new ArrayList<>(JavaConverters.seqAsJavaList(fvio));
        //If you would use List<VariableID> there, you could avoid the two conversions steps (one before the recursive call and the other in the Init0 constructor).

    }


    public ch.ethz.infsec.src.Mformula visit(JavaPred<VariableID> f){
        return new MPred(f.relation(), f.args(), this.freeVariablesInOrder);
    }


    public ch.ethz.infsec.src.Mformula visit(JavaNot<VariableID> f) {
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

    public ch.ethz.infsec.src.Mformula visit(JavaAnd<VariableID> f) {
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

    public ch.ethz.infsec.src.Mformula visit(JavaAll<VariableID> f) {
        return null;
    }

    public ch.ethz.infsec.src.Mformula visit(JavaEx<VariableID> f) {
        VariableID variable = f.variable();
        //VariableID varScala = new VariableID(variable.toString(), -1);
        //not sure if the above use of the constructor is correct!
        //List<VariableID> freeVarsInOrderSubformula = new ArrayList<>((Collection<VariableID>) JavaConverters.seqAsJavaListConverter(f.arg().freeVariablesInOrder()));

        this.freeVariablesInOrder.add(0,variable);
        Seq<VariableID> fvios = JavaConverters.asScalaBufferConverter(this.freeVariablesInOrder).asScala().toSeq();
        JavaGenFormula<VariableID> subformula = f.arg();
        return new MExists(subformula.accept(new Init0(fvios)));
    }

    public ch.ethz.infsec.src.Mformula visit(JavaFalse<VariableID> f) {
        HashSet<LinkedList<Optional<Object>>> table = new HashSet<>();
        LinkedList<Optional<Object>> el = new LinkedList<>();
        table.add(el);
        return new MRel(table); // aka empty table
    }

    public ch.ethz.infsec.src.Mformula visit(JavaTrue<VariableID> f) {
        int n = f.freeVariablesInOrder().size();
        HashSet<LinkedList<Optional<Object>>> table = new HashSet<>();
        LinkedList<Optional<Object>> el = new LinkedList<>();
        for(int i = 0; i < n; i++){
            //not sure if this is efficient
            el.add(Optional.empty());
        }
        table.add(el);
        return new MRel(table);
    }

    public ch.ethz.infsec.src.Mformula visit(JavaNext<VariableID> f) {
        return new MNext(f.interval(), f.accept(new Init0(f.freeVariablesInOrder())), true, new LinkedList<>());
    }

    public ch.ethz.infsec.src.Mformula visit(JavaOr<VariableID> f) {


        //NOT SURE IF I HAD TO ADD THE ABOVE ELEMENTS
        return new MOr((f.arg1()).accept(new Init0((f.arg1()).freeVariablesInOrder())),
                (f.arg2()).accept(new Init0((f.arg2()).freeVariablesInOrder())));
    }

    public ch.ethz.infsec.src.Mformula visit(JavaPrev<VariableID> f) {
        return new MPrev(f.interval(), f.accept(new Init0(f.freeVariablesInOrder())),
                true, new LinkedList<>());

    }

    public ch.ethz.infsec.src.Mformula visit(JavaSince<VariableID> f) {

        if(safe_formula(f.arg1())){
            return new MSince(true,
                    (f.arg1()).accept(new Init0((f.arg1()).freeVariablesInOrder())),
                    f.interval(),
                    (f.arg2()).accept(new Init0(f.arg2().freeVariablesInOrder())),
                    new LinkedList<>(),
                    new LinkedList<>());
        }else{
            if((f.arg1()) instanceof JavaNot){
                return new MSince(false,
                        (f.arg1()).accept(new Init0((f.arg1()).freeVariablesInOrder())),
                        f.interval(),
                        (f.arg2()).accept(new Init0((f.arg2()).freeVariablesInOrder())),
                        new LinkedList<>(),
                        new LinkedList<>());
            }else{
                return null;
            }
        }

    }

    public ch.ethz.infsec.src.Mformula visit(JavaUntil<VariableID> f) {

        if(safe_formula(f.arg1())){
            return new MUntil(true,
                    (f.arg1()).accept(new Init0((f.arg1()).freeVariablesInOrder())),
                    f.interval(),
                    (f.arg2()).accept(new Init0((f.arg2()).freeVariablesInOrder())),
                    new LinkedList<>(),
                    new LinkedList<>());
        }else{
            if((f.arg1()) instanceof JavaNot){
                return new MUntil(false,
                        (f.arg1()).accept(new Init0((f.arg1()).freeVariablesInOrder())),
                        f.interval(),
                        (f.arg2()).accept(new Init0((f.arg2()).freeVariablesInOrder())),
                        new LinkedList<>(),
                        new LinkedList<>());
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



}