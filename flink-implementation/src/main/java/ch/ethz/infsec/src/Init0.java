package ch.ethz.infsec.src;
import ch.ethz.infsec.policy.VariableID;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.*;

//does this have to be abstract??
public class Init0 implements FormulaVisitor<Mformula> {
    List<VariableID> freeVariablesInOrder; //shouldn't this be of type VariableID?

    public Init0(Seq<VariableID> fvio){
        this.freeVariablesInOrder = new ArrayList<>((Collection<VariableID>) JavaConverters.seqAsJavaListConverter(fvio));
    }


    public MPred visit(JavaPred f){
        return new MPred(f.relation(), f.args(), this.freeVariablesInOrder);
    }

    public Mformula visit(JavaNot f) {
        if(f.arg() instanceof JavaOr){
            //don't know how to handle Eq
            if(((JavaOr) f.arg()).arg1() instanceof JavaNot){
                //check if arg2 is a safe_formula
                List<Object> freeVarsInOrder1 = new ArrayList<>((Collection<? extends Object>) JavaConverters.seqAsJavaListConverter(((JavaOr) f.arg()).arg1().freeVariablesInOrder()));
                List<Object> freeVarsInOrder2 = new ArrayList<>((Collection<? extends Object>) JavaConverters.seqAsJavaListConverter(((JavaOr) f.arg()).arg2().freeVariablesInOrder()));
                boolean isSubset = freeVarsInOrder1.containsAll(freeVarsInOrder2);
                if(isSubset && safe_formula((JavaGenFormula) ((JavaOr) f.arg()).arg2())){
                    Optional<Object> el = Optional.empty();
                    List<Optional<Object>> listEl = new LinkedList<>();
                    listEl.add(el);
                    Set<List<Optional<Object>>> setEl = new HashSet<>();
                    setEl.add(listEl);
                    List<Set<List<Optional<Object>>>> fst = new LinkedList<>();
                    List<Set<List<Optional<Object>>>> snd = new LinkedList<>();
                    fst.add(setEl);
                    snd.add(setEl);
                    return new MAnd(((JavaGenFormula)  ((JavaOr) f.arg()).arg1()).accept(new Init0((((JavaOr) f.arg()).arg1()).freeVariablesInOrder())),
                            false, ((JavaGenFormula)  ((JavaOr) f.arg()).arg2()).accept(new Init0((((JavaOr) f.arg()).arg2()).freeVariablesInOrder())),
                            new Tuple<>(fst, snd));
                }else{
                    if(((JavaOr) f.arg()).arg2() instanceof JavaNot){
                        Optional<Object> el = Optional.empty();
                        List<Optional<Object>> listEl = new LinkedList<>();
                        listEl.add(el);
                        Set<List<Optional<Object>>> setEl = new HashSet<>();
                        setEl.add(listEl);
                        List<Set<List<Optional<Object>>>> fst = new LinkedList<>();
                        List<Set<List<Optional<Object>>>> snd = new LinkedList<>();
                        fst.add(setEl);
                        snd.add(setEl);
                        return new MAnd(((JavaGenFormula)  ((JavaOr) f.arg()).arg1()).accept(new Init0((((JavaOr) f.arg()).arg1()).freeVariablesInOrder()) {}),
                                true,((JavaGenFormula)  ((JavaOr) f.arg()).arg2()).accept(new Init0((((JavaOr) f.arg()).arg2()).freeVariablesInOrder()) {}),
                                new Tuple<>(fst, snd));
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

    public Mformula visit(JavaAnd f) {
        JavaGenFormula arg1 = (JavaGenFormula) f.arg1();
        JavaGenFormula arg2 = (JavaGenFormula) f.arg2();
        if(safe_formula(arg2)){
            Optional<Object> el = Optional.empty();
            List<Optional<Object>> listEl = new LinkedList<>();
            listEl.add(el);
            Set<List<Optional<Object>>> setEl = new HashSet<>();
            setEl.add(listEl);
            List<Set<List<Optional<Object>>>> fst = new LinkedList<>();
            List<Set<List<Optional<Object>>>> snd = new LinkedList<>();
            fst.add(setEl);
            snd.add(setEl);
            return new MAnd(arg1.accept(new Init0(arg1.freeVariablesInOrder()) {}), true, arg2.accept(new Init0(arg2.freeVariablesInOrder()) {}),
                    new Tuple<>(fst, snd));
        }else{
            List<Object> freeVarsInOrder1 = new ArrayList<>((Collection<? extends Object>) JavaConverters.seqAsJavaListConverter(arg1.freeVariablesInOrder()));
            List<Object> freeVarsInOrder2 = new ArrayList<>((Collection<? extends Object>) JavaConverters.seqAsJavaListConverter(arg2.freeVariablesInOrder()));
            boolean isSubset = freeVarsInOrder1.containsAll(freeVarsInOrder2);
            if(arg2 instanceof JavaNot && isSubset){
                Optional<Object> el = Optional.empty();
                List<Optional<Object>> listEl = new LinkedList<>();
                listEl.add(el);
                Set<List<Optional<Object>>> setEl = new HashSet<>();
                setEl.add(listEl);
                List<Set<List<Optional<Object>>>> fst = new LinkedList<>();
                List<Set<List<Optional<Object>>>> snd = new LinkedList<>();
                fst.add(setEl);
                snd.add(setEl);
                return new MAnd(arg1.accept(new Init0(arg1.freeVariablesInOrder()) {}), false, arg2.accept(new Init0(arg2.freeVariablesInOrder()) {}),
                        new Tuple<>(fst, snd));
            }else{
                return null;
            }
        }

    }

    public Mformula visit(JavaAll f) {
        return null;
    }

    public MExists visit(JavaEx f) {
        VariableID variable = (VariableID) f.variable();
        VariableID varScala = new VariableID(variable.toString(), -1);
        //not sure if the above use of the constructor is correct!
        //List<VariableID> freeVarsInOrderSubformula = new ArrayList<>((Collection<VariableID>) JavaConverters.seqAsJavaListConverter(f.arg().freeVariablesInOrder()));

        this.freeVariablesInOrder.add(0,varScala);
        Seq<VariableID> fvios = JavaConverters.asScalaBufferConverter(this.freeVariablesInOrder).asScala().toSeq();
        JavaGenFormula subformula = (JavaGenFormula) f.arg();
        return new MExists(subformula.accept(new Init0(fvios) {}));
    }

    public MRel visit(JavaFalse f) {
        int n = f.freeVariablesInOrder().size();
        Set<List<Optional<Object>>> table = new HashSet<>();
        List<Optional<Object>> el = new LinkedList<>();
        table.add(el);
        return new MRel(table); // aka empty table
    }

    public MRel visit(JavaTrue f) {
        int n = f.freeVariablesInOrder().size();
        Set<List<Optional<Object>>> table = new HashSet<>();
        List<Optional<Object>> el = new LinkedList<>();
        for(int i = 0; i < n; i++){
            //not sure if this is efficient
            el.add(Optional.empty());
        }
        table.add(el);
        return new MRel(table);
    }

    public MNext visit(JavaNext f) {
        return new MNext(f.interval(), f.accept((FormulaVisitor<Mformula>) new Init0(f.freeVariablesInOrder()) {}), true, new LinkedList<Integer>());
    }

    public MOr visit(JavaOr f) {

        Optional<Object> el = Optional.empty();
        List<Optional<Object>> listEl = new LinkedList<>();
        listEl.add(el);
        Set<List<Optional<Object>>> setEl = new HashSet<>();
        setEl.add(listEl);
        List<Set<List<Optional<Object>>>> fst = new LinkedList<>();
        List<Set<List<Optional<Object>>>> snd = new LinkedList<>();
        fst.add(setEl);
        snd.add(setEl);
        //NOT SURE IF I HAD TO ADD THE ABOVE ELEMENTS
        return new MOr(((JavaGenFormula)f.arg1()).accept((FormulaVisitor<Mformula>) new Init0(((JavaGenFormula)f.arg1()).freeVariablesInOrder()){}),
                ((JavaGenFormula)f.arg2()).accept((FormulaVisitor<Mformula>) new Init0(((JavaGenFormula)f.arg2()).freeVariablesInOrder()){}),
                new Tuple(fst, snd));
    }

    public MPrev visit(JavaPrev f) {
        Optional<Object> el = Optional.empty();
        List<Optional<Object>> listEl = new LinkedList<>();
        listEl.add(el);
        List<List<Optional<Object>>> listEl2 = new LinkedList<>();
        listEl2.add(listEl);
        List<List<List<Optional<Object>>>> listEl3 = new LinkedList<>();
        listEl3.add(listEl2);
        return new MPrev(f.interval(), f.accept((FormulaVisitor<Mformula>) new Init0(f.freeVariablesInOrder()) {}),
                true, new LinkedList<Integer>(), listEl3);

    }

    public MSince visit(JavaSince f) {
        Optional<Object> el = Optional.empty();
        List<Optional<Object>> listEl = new LinkedList<>();
        listEl.add(el);
        Set<List<Optional<Object>>> setEl = new HashSet<>();
        setEl.add(listEl);
        List<Set<List<Optional<Object>>>> fst = new LinkedList<>();
        List<Set<List<Optional<Object>>>> snd = new LinkedList<>();
        fst.add(setEl);
        snd.add(setEl);
        if(safe_formula((JavaGenFormula) f.arg1())){
            return new MSince(true,
                    ((JavaGenFormula)f.arg1()).accept((FormulaVisitor<Mformula>) new Init0(((JavaGenFormula)f.arg1()).freeVariablesInOrder()){}),
                    f.interval(),
                    ((JavaGenFormula)f.arg2()).accept((FormulaVisitor<Mformula>) new Init0(((JavaGenFormula)f.arg2()).freeVariablesInOrder()){}),
                    new Tuple(fst, snd),
                    new LinkedList<Integer>(),
                    new LinkedList<Triple<Integer, Set<List<Optional<Object>>>, Set<List<Optional<Object>>>>>());
        }else{
            if(((JavaGenFormula)f.arg1()) instanceof JavaNot){
                return new MSince(false,
                        ((JavaGenFormula)f.arg1()).accept((FormulaVisitor<Mformula>) new Init0(((JavaGenFormula)f.arg1()).freeVariablesInOrder()){}),
                        f.interval(),
                        ((JavaGenFormula)f.arg2()).accept((FormulaVisitor<Mformula>) new Init0(((JavaGenFormula)f.arg2()).freeVariablesInOrder()){}),
                        new Tuple(fst, snd),
                        new LinkedList<Integer>(),
                        new LinkedList<Triple<Integer, Set<List<Optional<Object>>>, Set<List<Optional<Object>>>>>());
            }else{
                return null;
            }
        }

    }

    public MUntil visit(JavaUntil f) {
        Optional<Object> el = Optional.empty();
        List<Optional<Object>> listEl = new LinkedList<>();
        listEl.add(el);
        Set<List<Optional<Object>>> setEl = new HashSet<>();
        setEl.add(listEl);
        List<Set<List<Optional<Object>>>> fst = new LinkedList<>();
        List<Set<List<Optional<Object>>>> snd = new LinkedList<>();
        fst.add(setEl);
        snd.add(setEl);
        if(safe_formula((JavaGenFormula) f.arg1())){
            return new MUntil(true,
                    ((JavaGenFormula)f.arg1()).accept((FormulaVisitor<Mformula>) new Init0(((JavaGenFormula)f.arg1()).freeVariablesInOrder()){}),
                    f.interval(),
                    ((JavaGenFormula)f.arg2()).accept((FormulaVisitor<Mformula>) new Init0(((JavaGenFormula)f.arg2()).freeVariablesInOrder()){}),
                    new Tuple(fst, snd),
                    new LinkedList<Integer>(),
                    new LinkedList<Triple<Integer, Set<List<Optional<Object>>>, Set<List<Optional<Object>>>>>());
        }else{
            if(((JavaGenFormula)f.arg1()) instanceof JavaNot){
                return new MUntil(false,
                        ((JavaGenFormula)f.arg1()).accept((FormulaVisitor<Mformula>) new Init0(((JavaGenFormula)f.arg1()).freeVariablesInOrder()){}),
                        f.interval(),
                        ((JavaGenFormula)f.arg2()).accept((FormulaVisitor<Mformula>) new Init0(((JavaGenFormula)f.arg2()).freeVariablesInOrder()){}),
                        new Tuple(fst, snd),
                        new LinkedList<Integer>(),
                        new LinkedList<Triple<Integer, Set<List<Optional<Object>>>, Set<List<Optional<Object>>>>>());
            }else{
                return null;
            }
        }

    }

    public static boolean safe_formula(JavaGenFormula form){
        if(form instanceof JavaPred){
            return true;
        }else if(form instanceof JavaNot && ((JavaNot)form).arg() instanceof JavaOr && ((JavaOr)((JavaNot)form).arg()).arg1() instanceof JavaNot){
            List<Object> freeVarsInOrder1 = new ArrayList<Object>((Collection<? extends Object>) JavaConverters.seqAsJavaListConverter(((JavaOr)((JavaNot)form).arg()).arg1().freeVariablesInOrder()));
            List<Object> freeVarsInOrder2 = new ArrayList<Object>((Collection<? extends Object>) JavaConverters.seqAsJavaListConverter(((JavaOr)((JavaNot)form).arg()).arg2().freeVariablesInOrder()));
            boolean isSubset = freeVarsInOrder1.containsAll(freeVarsInOrder2);
            boolean alternative = false;
            if(((JavaOr)((JavaNot)form).arg()).arg2() instanceof JavaNot){
                alternative = safe_formula((JavaGenFormula) ((JavaNot)((JavaOr)((JavaNot)form).arg()).arg2()).arg());
            }
            return ((safe_formula((JavaGenFormula) ((JavaOr)((JavaNot)form).arg()).arg1()) && safe_formula((JavaGenFormula) ((JavaOr)((JavaNot)form).arg()).arg2()) && isSubset) || alternative);
        }else if(form instanceof JavaOr){
            List<Object> freeVarsInOrder1 = new ArrayList<Object>((Collection<? extends Object>) JavaConverters.seqAsJavaListConverter(((JavaOr)form).arg1().freeVariablesInOrder()));
            List<Object> freeVarsInOrder2 = new ArrayList<Object>((Collection<? extends Object>) JavaConverters.seqAsJavaListConverter(((JavaOr)form).arg2().freeVariablesInOrder()));
            boolean isEqual = freeVarsInOrder1.containsAll(freeVarsInOrder2) && freeVarsInOrder2.containsAll(freeVarsInOrder1);
            return (safe_formula((JavaGenFormula) ((JavaOr)form).arg1()) && safe_formula((JavaGenFormula) ((JavaOr)form).arg2()) && isEqual);

        }else if(form instanceof JavaEx){
            return safe_formula((JavaGenFormula) ((JavaEx) form).arg());
        }else if(form instanceof JavaPrev){
            return safe_formula((JavaGenFormula) ((JavaPrev) form).arg());
        }else if(form instanceof JavaNext){
            return safe_formula((JavaGenFormula) ((JavaNext) form).arg());
        }else if(form instanceof JavaSince){
            List<Object> freeVarsInOrder1 = new ArrayList<Object>((Collection<? extends Object>) JavaConverters.seqAsJavaListConverter(((JavaSince) form).arg1().freeVariablesInOrder()));
            List<Object> freeVarsInOrder2 = new ArrayList<Object>((Collection<? extends Object>) JavaConverters.seqAsJavaListConverter(((JavaSince) form).arg2().freeVariablesInOrder()));
            boolean isSubset = freeVarsInOrder2.containsAll(freeVarsInOrder1);
            boolean sff = safe_formula((JavaGenFormula) ((JavaSince) form).arg1());
            boolean sfs = safe_formula((JavaGenFormula) ((JavaSince) form).arg2());
            boolean alt = false;
            if( ((JavaSince) form).arg1() instanceof JavaNot){
                alt = safe_formula((JavaGenFormula) ((JavaNot) ((JavaSince) form).arg1()).arg());
            }
            boolean sec = sff || alt && sfs;
            return isSubset && sec;
        }else if(form instanceof JavaUntil){
            List<Object> freeVarsInOrder1 = new ArrayList<Object>((Collection<? extends Object>) JavaConverters.seqAsJavaListConverter(((JavaUntil) form).arg1().freeVariablesInOrder()));
            List<Object> freeVarsInOrder2 = new ArrayList<Object>((Collection<? extends Object>) JavaConverters.seqAsJavaListConverter(((JavaUntil) form).arg2().freeVariablesInOrder()));
            boolean isSubset = freeVarsInOrder2.containsAll(freeVarsInOrder1);
            boolean sff = safe_formula((JavaGenFormula) ((JavaUntil) form).arg1());
            boolean sfs = safe_formula((JavaGenFormula) ((JavaUntil) form).arg2());
            boolean alt = false;
            if( ((JavaUntil) form).arg1() instanceof JavaNot){
                alt = safe_formula((JavaGenFormula) ((JavaNot) ((JavaUntil) form).arg1()).arg());
            }
            boolean sec = sff || alt && sfs;
            return isSubset && sec;
        }else{
            return false;
        }

    }

}