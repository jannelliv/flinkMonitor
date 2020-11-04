package ch.ethz.infsec.src;
import ch.ethz.infsec.policy.*;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.immutable.List;
import scala.collection.immutable.Set;

import java.util.*;

//does this have to be abstract??
public class Init0<U> implements FormulaVisitor<Mformula<U>> {
    ArrayList<VariableID> freeVariablesInOrder; //shouldn't this be of type VariableID?

    public Init0(Seq<VariableID> fvio){
        this.freeVariablesInOrder = new ArrayList<VariableID>(JavaConverters.seqAsJavaList(fvio));
        //If you would use List<VariableID> there, you could avoid the two conversions steps (one before the recursive call and the other in the Init0 constructor).

    }


    public Mformula<U> visit(JavaPred f){
        return new MPred(f.relation(), f.args(), this.freeVariablesInOrder);
    }


    public Mformula<U> visit(JavaNot f) {
        if(f.arg() instanceof JavaOr){
            //don't know how to handle Eq
            if(((JavaOr) f.arg()).arg1() instanceof JavaNot){
                //check if arg2 is a safe_formula
                //make sure it's correct that you have
                ArrayList<Object> freeVarsInOrder1 = new ArrayList<Object>(JavaConverters.seqAsJavaList(((JavaOr) f.arg()).arg1().freeVariablesInOrder()));
                ArrayList<Object> freeVarsInOrder2 = new ArrayList<Object>(JavaConverters.seqAsJavaList(((JavaOr) f.arg()).arg2().freeVariablesInOrder()));
                boolean isSubset = freeVarsInOrder1.containsAll(freeVarsInOrder2);
                if(isSubset && safe_formula(((JavaOr) f.arg()).arg2())){
                    Optional<Object> el = Optional.empty();
                    LinkedList<Optional<Object>> listEl = new LinkedList<Optional<Object>>();
                    listEl.add(el);
                    HashSet<LinkedList<Optional<Object>>> setEl = new HashSet<LinkedList<Optional<Object>>>();
                    setEl.add(listEl);
                    LinkedList<HashSet<LinkedList<Optional<Object>>>> fst = new LinkedList<HashSet<LinkedList<Optional<Object>>>>();
                    LinkedList<HashSet<LinkedList<Optional<Object>>>> snd = new LinkedList<HashSet<LinkedList<Optional<Object>>>>();
                    fst.add(setEl);
                    snd.add(setEl);
                    //WHY IS THE BELOW CAST TO Mformula NOT NEGLIGIBLE, I.E. HAPPENS OBVIOUSLY??
                    return new MAnd((((JavaOr)convert(f.arg())).arg1()).accept(new Init0((((JavaOr) convert(f.arg())).arg1()).freeVariablesInOrder())),
                            false, (((JavaOr) convert(f.arg())).arg2()).accept(new Init0((((JavaOr) convert(f.arg())).arg2()).freeVariablesInOrder())),
                            new Tuple<LinkedList<HashSet<LinkedList<Optional<Object>>>>, LinkedList<HashSet<LinkedList<Optional<Object>>>>>(fst, snd));
                }else{
                    if(((JavaOr)convert(f.arg())).arg2() instanceof JavaNot){
                        Optional<Object> el = Optional.empty();
                        LinkedList<Optional<Object>> listEl = new LinkedList<>();
                        listEl.add(el);
                        HashSet<LinkedList<Optional<Object>>> setEl = new HashSet<>();
                        setEl.add(listEl);
                        LinkedList<HashSet<LinkedList<Optional<Object>>>> fst = new LinkedList<>();
                        LinkedList<HashSet<LinkedList<Optional<Object>>>> snd = new LinkedList<>();
                        fst.add(setEl);
                        snd.add(setEl);
                        return new MAnd((((JavaOr)convert(f.arg())).arg1()).accept(new Init0((((JavaOr) convert(f.arg())).arg1()).freeVariablesInOrder())),
                                false, (((JavaOr) convert(f.arg())).arg2()).accept(new Init0((((JavaOr) convert(f.arg())).arg2()).freeVariablesInOrder())),
                                new Tuple<LinkedList<HashSet<LinkedList<Optional<Object>>>>, LinkedList<HashSet<LinkedList<Optional<Object>>>>>(fst, snd));
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

    public Mformula<U> visit(JavaAnd f) {
        JavaGenFormula arg1 = convert(f.arg1());
        JavaGenFormula arg2 = convert(f.arg2());
        if(safe_formula(arg2)){
            Optional<Object> el = Optional.empty();
            LinkedList<Optional<Object>> listEl = new LinkedList<>();
            listEl.add(el);
            HashSet<LinkedList<Optional<Object>>> setEl = new HashSet<>();
            setEl.add(listEl);
            LinkedList<HashSet<LinkedList<Optional<Object>>>> fst = new LinkedList<>();
            LinkedList<HashSet<LinkedList<Optional<Object>>>> snd = new LinkedList<>();
            fst.add(setEl);
            snd.add(setEl);
            return new MAnd(arg1.accept(new Init0(arg1.freeVariablesInOrder())), true, arg2.accept(new Init0(arg2.freeVariablesInOrder())),
                    new Tuple<LinkedList<HashSet<LinkedList<Optional<Object>>>>, LinkedList<HashSet<LinkedList<Optional<Object>>>>>(fst, snd));
        }else{
            ArrayList<Object> freeVarsInOrder1 = new ArrayList<Object>(JavaConverters.seqAsJavaList(arg1.freeVariablesInOrder()));
            ArrayList<Object> freeVarsInOrder2 = new ArrayList<Object>(JavaConverters.seqAsJavaList(arg2.freeVariablesInOrder()));
            boolean isSubset = freeVarsInOrder1.containsAll(freeVarsInOrder2);
            if(arg2 instanceof JavaNot && isSubset){
                Optional<Object> el = Optional.empty();
                LinkedList<Optional<Object>> listEl = new LinkedList<>();
                listEl.add(el);
                HashSet<LinkedList<Optional<Object>>> setEl = new HashSet<>();
                setEl.add(listEl);
                LinkedList<HashSet<LinkedList<Optional<Object>>>> fst = new LinkedList<>();
                LinkedList<HashSet<LinkedList<Optional<Object>>>> snd = new LinkedList<>();
                fst.add(setEl);
                snd.add(setEl);
                return new MAnd(arg1.accept(new Init0(arg1.freeVariablesInOrder())), false, arg2.accept(new Init0(arg2.freeVariablesInOrder())),
                        new Tuple<LinkedList<HashSet<LinkedList<Optional<Object>>>>, LinkedList<HashSet<LinkedList<Optional<Object>>>>>(fst, snd));
            }else{
                return null;
            }
        }

    }

    public Mformula<U> visit(JavaAll f) {
        return null;
    }

    public Mformula<U> visit(JavaEx f) {
        VariableID variable = (VariableID) f.variable();
        VariableID varScala = new VariableID(variable.toString(), -1);
        //not sure if the above use of the constructor is correct!
        //List<VariableID> freeVarsInOrderSubformula = new ArrayList<>((Collection<VariableID>) JavaConverters.seqAsJavaListConverter(f.arg().freeVariablesInOrder()));

        this.freeVariablesInOrder.add(0,varScala);
        Seq<VariableID> fvios = JavaConverters.asScalaBufferConverter(this.freeVariablesInOrder).asScala().toSeq();
        JavaGenFormula subformula = convert(f.arg());
        return new MExists(subformula.accept(new Init0(fvios)));
    }

    public Mformula<U> visit(JavaFalse f) {
        int n = f.freeVariablesInOrder().size();
        HashSet<LinkedList<Optional<Object>>> table = new HashSet<>();
        LinkedList<Optional<Object>> el = new LinkedList<>();
        table.add(el);
        return new MRel(table); // aka empty table
    }

    public Mformula<U> visit(JavaTrue f) {
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

    public Mformula<U> visit(JavaNext f) {
        return new MNext(f.interval(), f.accept(new Init0(f.freeVariablesInOrder())), true, new LinkedList<Integer>());
    }

    public Mformula<U> visit(JavaOr f) {

        Optional<Object> el = Optional.empty();
        LinkedList<Optional<Object>> listEl = new LinkedList<>();
        listEl.add(el);
        HashSet<LinkedList<Optional<Object>>> setEl = new HashSet<>();
        setEl.add(listEl);
        LinkedList<HashSet<LinkedList<Optional<Object>>>> fst = new LinkedList<>();
        LinkedList<HashSet<LinkedList<Optional<Object>>>> snd = new LinkedList<>();
        fst.add(setEl);
        snd.add(setEl);
        //NOT SURE IF I HAD TO ADD THE ABOVE ELEMENTS
        return new MOr((convert(f.arg1())).accept(new Init0((convert(f.arg1())).freeVariablesInOrder())),
                (convert(f.arg2())).accept(new Init0((convert(f.arg2())).freeVariablesInOrder())),
                new Tuple<LinkedList<HashSet<LinkedList<Optional<Object>>>>, LinkedList<HashSet<LinkedList<Optional<Object>>>>>(fst, snd));
    }

    public Mformula<U> visit(JavaPrev f) {
        Optional<Object> el = Optional.empty();
        LinkedList<Optional<Object>> listEl = new LinkedList<>();
        listEl.add(el);
        LinkedList<LinkedList<Optional<Object>>> listEl2 = new LinkedList<>();
        listEl2.add(listEl);
        LinkedList<LinkedList<LinkedList<Optional<Object>>>> listEl3 = new LinkedList<>();
        listEl3.add(listEl2);
        return new MPrev(f.interval(), f.accept(new Init0(f.freeVariablesInOrder())),
                true, new LinkedList<Integer>(), listEl3);

    }

    public Mformula<U> visit(JavaSince f) {
        Optional<Object> el = Optional.empty();
        LinkedList<Optional<Object>> listEl = new LinkedList<>();
        listEl.add(el);
        HashSet<LinkedList<Optional<Object>>> setEl = new HashSet<>();
        setEl.add(listEl);
        LinkedList<HashSet<LinkedList<Optional<Object>>>> fst = new LinkedList<>();
        LinkedList<HashSet<LinkedList<Optional<Object>>>> snd = new LinkedList<>();
        fst.add(setEl);
        snd.add(setEl);
        if(safe_formula(convert(f.arg1()))){
            return new MSince(true,
                    convert(f.arg1()).accept(new Init0(convert(f.arg1()).freeVariablesInOrder())),
                    f.interval(),
                    convert(f.arg2()).accept(new Init0(convert(f.arg2()).freeVariablesInOrder())),
                    new Tuple<LinkedList<HashSet<LinkedList<Optional<Object>>>>, LinkedList<HashSet<LinkedList<Optional<Object>>>>>(fst, snd),
                    new LinkedList<Integer>(),
                    new LinkedList<Triple<Integer, HashSet<LinkedList<Optional<Object>>>, HashSet<LinkedList<Optional<Object>>>>>());
        }else{
            if((f.arg1()) instanceof JavaNot){
                return new MSince(false,
                        (convert(f.arg1())).accept(new Init0((convert(f.arg1())).freeVariablesInOrder()){}),
                        f.interval(),
                        convert(f.arg2()).accept(new Init0(convert(f.arg2()).freeVariablesInOrder())),
                        new Tuple(fst, snd),
                        new LinkedList<Integer>(),
                        new LinkedList<Triple<Integer, HashSet<LinkedList<Optional<Object>>>, HashSet<LinkedList<Optional<Object>>>>>());
            }else{
                return null;
            }
        }

    }

    public Mformula<U> visit(JavaUntil f) {
        Optional<Object> el = Optional.empty();
        LinkedList<Optional<Object>> listEl = new LinkedList<>();
        listEl.add(el);
        HashSet<LinkedList<Optional<Object>>> setEl = new HashSet<>();
        setEl.add(listEl);
        LinkedList<HashSet<LinkedList<Optional<Object>>>> fst = new LinkedList<>();
        LinkedList<HashSet<LinkedList<Optional<Object>>>> snd = new LinkedList<>();
        fst.add(setEl);
        snd.add(setEl);
        if(safe_formula(convert(f.arg1()))){
            return new MUntil(true,
                    convert(f.arg1()).accept(new Init0(convert(f.arg1()).freeVariablesInOrder())),
                    f.interval(),
                    convert(f.arg2()).accept(new Init0(convert(f.arg2()).freeVariablesInOrder())),
                    new Tuple<LinkedList<HashSet<LinkedList<Optional<Object>>>>, LinkedList<HashSet<LinkedList<Optional<Object>>>>>(fst, snd),
                    new LinkedList<Integer>(),
                    new LinkedList<Triple<Integer, HashSet<LinkedList<Optional<Object>>>, HashSet<LinkedList<Optional<Object>>>>>());
        }else{
            if(convert(f.arg1()) instanceof JavaNot){
                return new MUntil(false,
                        convert(f.arg1()).accept(new Init0(convert(f.arg1()).freeVariablesInOrder())),
                        f.interval(),
                        convert(f.arg2()).accept(new Init0(convert(f.arg2()).freeVariablesInOrder())),
                        new Tuple<LinkedList<HashSet<LinkedList<Optional<Object>>>>, LinkedList<HashSet<LinkedList<Optional<Object>>>>>(fst, snd),
                        new LinkedList<Integer>(),
                        new LinkedList<Triple<Integer, HashSet<LinkedList<Optional<Object>>>, HashSet<LinkedList<Optional<Object>>>>>());
            }else{
                return null;
            }
        }

    }

    public static boolean safe_formula(JavaGenFormula form){
        if(form instanceof JavaPred){
            return true;
        }else if(form instanceof JavaNot && ((JavaNot)form).arg() instanceof JavaOr && ((JavaOr)((JavaNot)form).arg()).arg1() instanceof JavaNot){
            ArrayList<Object> freeVarsInOrder1 = new ArrayList<Object>(JavaConverters.seqAsJavaList(((JavaOr)((JavaNot)form).arg()).arg1().freeVariablesInOrder()));
            ArrayList<Object> freeVarsInOrder2 = new ArrayList<Object>(JavaConverters.seqAsJavaList(((JavaOr)((JavaNot)form).arg()).arg2().freeVariablesInOrder()));
            boolean isSubset = freeVarsInOrder1.containsAll(freeVarsInOrder2);
            boolean alternative = false;
            if(((JavaOr)((JavaNot)form).arg()).arg2() instanceof JavaNot){
                alternative = safe_formula(((JavaNot)((JavaOr)((JavaNot)form).arg()).arg2()).arg());
            }
            return ((safe_formula(((JavaOr)((JavaNot)form).arg()).arg1()) && safe_formula(((JavaOr)((JavaNot)form).arg()).arg2()) && isSubset) || alternative);
        }else if(form instanceof JavaOr){
            ArrayList<Object> freeVarsInOrder1 = new ArrayList<>(JavaConverters.seqAsJavaList(((JavaOr)form).arg1().freeVariablesInOrder()));
            ArrayList<Object> freeVarsInOrder2 = new ArrayList<>(JavaConverters.seqAsJavaList(((JavaOr)form).arg2().freeVariablesInOrder()));
            boolean isEqual = freeVarsInOrder1.containsAll(freeVarsInOrder2) && freeVarsInOrder2.containsAll(freeVarsInOrder1);
            return (safe_formula(((JavaOr)form).arg1()) && safe_formula(((JavaOr)form).arg2()) && isEqual);

        }else if(form instanceof JavaEx){
            return safe_formula(((JavaEx) form).arg());
        }else if(form instanceof JavaPrev){
            return safe_formula(((JavaPrev) form).arg());
        }else if(form instanceof JavaNext){
            return safe_formula(((JavaNext) form).arg());
        }else if(form instanceof JavaSince){
            ArrayList<Object> freeVarsInOrder1 = new ArrayList<>(JavaConverters.seqAsJavaList(((JavaSince) form).arg1().freeVariablesInOrder()));
            ArrayList<Object> freeVarsInOrder2 = new ArrayList<>(JavaConverters.seqAsJavaList(((JavaSince) form).arg2().freeVariablesInOrder()));
            boolean isSubset = freeVarsInOrder2.containsAll(freeVarsInOrder1);
            boolean sff = safe_formula(((JavaSince) form).arg1());
            boolean sfs = safe_formula( convert(((JavaSince) form).arg2()));
            boolean alt = false;
            if( ((JavaSince) form).arg1() instanceof JavaNot){
                alt = safe_formula(((JavaNot) ((JavaSince) form).arg1()).arg());
            }
            boolean sec = sff || alt && sfs;
            return isSubset && sec;
        }else if(form instanceof JavaUntil){
            ArrayList<Object> freeVarsInOrder1 = new ArrayList<Object>(JavaConverters.seqAsJavaList(((JavaUntil) form).arg1().freeVariablesInOrder()));
            ArrayList<Object> freeVarsInOrder2 = new ArrayList<Object>(JavaConverters.seqAsJavaList(((JavaUntil) form).arg2().freeVariablesInOrder()));
            boolean isSubset = freeVarsInOrder2.containsAll(freeVarsInOrder1);
            boolean sff = safe_formula(((JavaUntil) form).arg1());
            boolean sfs = safe_formula(((JavaUntil) form).arg2());
            boolean alt = false;
            if( ((JavaUntil) form).arg1() instanceof JavaNot){
                alt = safe_formula(((JavaNot) ((JavaUntil) form).arg1()).arg());
            }
            boolean sec = sff || alt && sfs;
            return isSubset && sec;
        }else{
            return false;
        }

    }

    public static <T> JavaGenFormula<T> convert(GenFormula<T> gf){

        if(gf instanceof Ex){ //if I included the generic type in the if, I got an error message
            return new JavaEx<T>(((Ex<T>) gf).variable(), ((Ex<T>) gf).arg());
        }else if(gf instanceof All){
            return new JavaAll<T>(((All<T>) gf).variable(), ((All<T>) gf).arg());
        }else if(gf instanceof And){
            return new JavaAnd<T>(((And<T>) gf).arg1(), ((And<T>) gf).arg2());
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