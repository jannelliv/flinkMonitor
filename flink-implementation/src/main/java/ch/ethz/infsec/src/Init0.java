package ch.ethz.infsec.src;
import ch.ethz.infsec.policy.GenFormula;
import ch.ethz.infsec.policy.Pred;
import ch.ethz.infsec.policy.VariableID;
import ch.ethz.infsec.policy.VariableMapper;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.immutable.List;
import scala.collection.immutable.Set;

import java.util.*;

//does this have to be abstract??
public class Init0 implements FormulaVisitor<Mformula> {
    ArrayList<VariableID> freeVariablesInOrder; //shouldn't this be of type VariableID?

    public Init0(Seq<VariableID> fvio){
        this.freeVariablesInOrder = new ArrayList<VariableID>(JavaConverters.seqAsJavaList(fvio));
        //If you would use List<VariableID> there, you could avoid the two conversions steps (one before the recursive call and the other in the Init0 constructor).

    }


    public MPred visit(JavaPred f){
        return new MPred(f.relation(), f.args(), this.freeVariablesInOrder);
    }

    public Mformula visit(JavaNot f) {
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
                    if(((JavaOr) f.arg()).arg2() instanceof JavaNot){
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

    public Mformula visit(JavaAnd f) {
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
        JavaGenFormula subformula = convert(f.arg());
        return new MExists(subformula.accept(new Init0(fvios)));
    }

    public MRel visit(JavaFalse f) {
        int n = f.freeVariablesInOrder().size();
        HashSet<LinkedList<Optional<Object>>> table = new HashSet<>();
        LinkedList<Optional<Object>> el = new LinkedList<>();
        table.add(el);
        return new MRel(table); // aka empty table
    }

    public MRel visit(JavaTrue f) {
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

    public MNext visit(JavaNext f) {
        return new MNext(f.interval(), f.accept(new Init0(f.freeVariablesInOrder())), true, new LinkedList<Integer>());
    }

    public MOr visit(JavaOr f) {

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

    public MPrev visit(JavaPrev f) {
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

    public MSince visit(JavaSince f) {
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

    public MUntil visit(JavaUntil f) {
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
                    new Tuple(fst, snd),
                    new LinkedList<Integer>(),
                    new LinkedList<Triple<Integer, HashSet<LinkedList<Optional<Object>>>, HashSet<LinkedList<Optional<Object>>>>>());
        }else{
            if(convert(f.arg1()) instanceof JavaNot){
                return new MUntil(false,
                        convert(f.arg1()).accept(new Init0(convert(f.arg1()).freeVariablesInOrder())),
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

    public static boolean safe_formula(JavaGenFormula form){
        if(form instanceof JavaPred){
            return true;
        }else if(form instanceof JavaNot && ((JavaNot)form).arg() instanceof JavaOr && ((JavaOr)((JavaNot)form).arg()).arg1() instanceof JavaNot){
            ArrayList<Object> freeVarsInOrder1 = new ArrayList<Object>(JavaConverters.seqAsJavaList(((JavaOr)((JavaNot)form).arg()).arg1().freeVariablesInOrder()));
            ArrayList<Object> freeVarsInOrder2 = new ArrayList<Object>(JavaConverters.seqAsJavaList(((JavaOr)((JavaNot)form).arg()).arg2().freeVariablesInOrder()));
            boolean isSubset = freeVarsInOrder1.containsAll(freeVarsInOrder2);
            boolean alternative = false;
            if(((JavaOr)((JavaNot)form).arg()).arg2() instanceof JavaNot){
                alternative = safe_formula((JavaGenFormula) ((JavaNot)((JavaOr)((JavaNot)form).arg()).arg2()).arg());
            }
            return ((safe_formula((JavaGenFormula) ((JavaOr)((JavaNot)form).arg()).arg1()) && safe_formula((JavaGenFormula) ((JavaOr)((JavaNot)form).arg()).arg2()) && isSubset) || alternative);
        }else if(form instanceof JavaOr){
            ArrayList<Object> freeVarsInOrder1 = new ArrayList<>(JavaConverters.seqAsJavaList(((JavaOr)form).arg1().freeVariablesInOrder()));
            ArrayList<Object> freeVarsInOrder2 = new ArrayList<>(JavaConverters.seqAsJavaList(((JavaOr)form).arg2().freeVariablesInOrder()));
            boolean isEqual = freeVarsInOrder1.containsAll(freeVarsInOrder2) && freeVarsInOrder2.containsAll(freeVarsInOrder1);
            return (safe_formula((JavaGenFormula) ((JavaOr)form).arg1()) && safe_formula((JavaGenFormula) ((JavaOr)form).arg2()) && isEqual);

        }else if(form instanceof JavaEx){
            return safe_formula((JavaGenFormula) ((JavaEx) form).arg());
        }else if(form instanceof JavaPrev){
            return safe_formula((JavaGenFormula) ((JavaPrev) form).arg());
        }else if(form instanceof JavaNext){
            return safe_formula((JavaGenFormula) ((JavaNext) form).arg());
        }else if(form instanceof JavaSince){
            ArrayList<Object> freeVarsInOrder1 = new ArrayList<>(JavaConverters.seqAsJavaList(((JavaSince) form).arg1().freeVariablesInOrder()));
            ArrayList<Object> freeVarsInOrder2 = new ArrayList<>(JavaConverters.seqAsJavaList(((JavaSince) form).arg2().freeVariablesInOrder()));
            boolean isSubset = freeVarsInOrder2.containsAll(freeVarsInOrder1);
            boolean sff = safe_formula((JavaGenFormula) ((JavaSince) form).arg1());
            boolean sfs = safe_formula( convert(((JavaSince) form).arg2()));
            boolean alt = false;
            if( ((JavaSince) form).arg1() instanceof JavaNot){
                alt = safe_formula((JavaGenFormula) ((JavaNot) ((JavaSince) form).arg1()).arg());
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
        JavaGenFormula<T> jgf = new JavaGenFormula<T>() {
            @Override
            public <Mformula> Mformula accept(FormulaVisitor<Mformula> v) {
                return null;
            }

            @Override
            public Set<Pred<T>> atoms() {
                return null;
            }

            @Override
            public Set<T> freeVariables() {
                return null;
            }

            @Override
            public List<String> check() {
                return null;
            }

            @Override
            public Seq<Pred<T>> atomsInOrder() {
                return null;
            }

            @Override
            public Seq<T> freeVariablesInOrder() {
                return null;
            }

            @Override
            public String toQTL() {
                return null;
            }

            @Override
            public <W> GenFormula<W> map(VariableMapper<T, W> mapper) {
                return null;
            }

            @Override
            public GenFormula<T> close(boolean neg) {
                return JavaGenFormula.super.close(neg);
            }

            @Override
            public String toQTLString(boolean neg) {
                return JavaGenFormula.super.toQTLString(neg);
            }
        };
        return jgf;
    }

}