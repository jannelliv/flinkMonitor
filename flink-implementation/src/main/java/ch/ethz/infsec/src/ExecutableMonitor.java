package ch.ethz.infsec.src;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;


public class ExecutableMonitor {
    /*Set<List<Optional<Object>>> eq_rel(int n, MfotlTerm x, MfotlTerm y, Set<List<Optional<Object>>> t){

        if(x instanceof MfotlConst && y instanceof MfotlConst) {
            if(x==y) {
                return t.unit_table(n);
            }else {
                Set<List<Optional<Object>>> result = new HashSet<>();
                return result;
            }
        }else if(x instanceof MfotlVar && y instanceof MfotlConst) {
            return (Set<List<Optional<Object>>>) t.singleton_table(n,((MfotlVar) x).name,((MfotlConst) y).value);
        }else if(y instanceof MfotlVar && x instanceof MfotlConst) {
            return (Set<List<Optional<Object>>>) t.singleton_table(n,((MfotlVar) y).name,((MfotlConst) x).value);
            //PROBLEM W/ ARGUMENT TYPES!
        }else {
            return null; //"undefined" in Isabelle
        }

    }*/

    /*Set<List<Optional<Object>>> neq_rel(int n, MfotlTerm x, MfotlTerm y, Table t){

        if(x instanceof MfotlConst && y instanceof MfotlConst) {
            if(x==y) {
                Set<List<Optional<Object>>> result = new HashSet<List<Optional<Object>>>();
                return result;
            }else {
                return t.unit_table(n);
            }
        }else if(x instanceof MfotlVar && y instanceof MfotlVar) {
            if(x==y) {
                Set<List<Optional<Object>>> result = new HashSet<List<Optional<Object>>>();
                return result;
            }else {
                return null;
            }
        }else {
            return null; //"undefined" in Isabelle
        }
    }*/



    Tuple<List<Set<Optional<List<Optional<Object>>>>>, List<Set<Optional<List<Optional<Object>>>>>> mbuf2_add(List<Set<Optional<List<Optional<Object>>>>> xsp,
                                                                                List<Set<Optional<List<Optional<Object>>>>> ysp, Tuple<List<Set<Optional<List<Optional<Object>>>>>,
                                                                                List<Set<Optional<List<Optional<Object>>>>>> mbuf2){
        List<Set<Optional<List<Optional<Object>>>>> xs = mbuf2.fst();
        List<Set<Optional<List<Optional<Object>>>>> ys = mbuf2.snd();
        List<Set<Optional<List<Optional<Object>>>>> result1 = new ArrayList<>();
        result1.addAll(xs);
        result1.addAll(xsp);
        List<Set<Optional<List<Optional<Object>>>>> result2 = new ArrayList<>();
        result2.addAll(ys);
        result2.addAll(ysp);

        Tuple<List<Set<Optional<List<Optional<Object>>>>>, List<Set<Optional<List<Optional<Object>>>>>> result = new Tuple(result1, result2);
        return result;

    }

    <U> Tuple<List<U>, Tuple<List<Set<Optional<List<Optional<Object>>>>>, List<Set<Optional<List<Optional<Object>>>>>> > mbuf2_take(BiFunction<Set<Optional<List<Optional<Object>>>>, Set<Optional<List<Optional<Object>>>>, U> f,
                                                                                                      Tuple<List<Set<Optional<List<Optional<Object>>>>>, List<Set<Optional<List<Optional<Object>>>>>> mbuf2){

        List<Set<Optional<List<Optional<Object>>>>> xs = mbuf2.fst();
        List<Set<Optional<List<Optional<Object>>>>> ys = mbuf2.snd();
        if(xs.size() >= 1 && ys.size() >= 1) {
            Set<Optional<List<Optional<Object>>>> x = xs.remove(0);
            Set<Optional<List<Optional<Object>>>> y = ys.remove(0);
            Tuple<List<Set<Optional<List<Optional<Object>>>>>, List<Set<Optional<List<Optional<Object>>>>>> mbuf2p = new Tuple<>(xs, ys);
            Tuple<List<U>, Tuple<List<Set<Optional<List<Optional<Object>>>>>, List<Set<Optional<List<Optional<Object>>>>>> > zsbuf = mbuf2_take(f, mbuf2p);

            Tuple<List<Set<Optional<List<Optional<Object>>>>>, List<Set<Optional<List<Optional<Object>>>>>> buf = zsbuf.snd();
            List<U> zs = zsbuf.fst();
            U elem = f.apply(x,y);
            zs.add(0,elem);
            Tuple<List<U>, Tuple<List<Set<Optional<List<Optional<Object>>>>>, List<Set<Optional<List<Optional<Object>>>>>> > result = new Tuple(zs, buf);
            return result;
        }else {
            List<U> res1 = new ArrayList<>();
            Tuple<List<U>, Tuple<List<Set<Optional<List<Optional<Object>>>>>, List<Set<Optional<List<Optional<Object>>>>>> > result = new Tuple(res1, mbuf2);
            return result;
        }
    }

    Optional<Function<Integer, Object>> match(List<MfotlTerm> ts, List<Object> ys){
        if(ts.size() == 0 && ys.size() == 0) {
            Function<Integer, Object> emptyMap = new Function<Integer, Object>() {
                @Override
                public Object apply(Integer integer) {
                    return null;
                }
            };
            Optional<Function<Integer, Object>> result = Optional.of(emptyMap);
            return result;
        }else {
            if(ts.size() > 0 && ys.size() > 0 && (ts.get(0) instanceof MfotlConst)) {
                if(ts.get(0) == ys.get(0)) {
                    ts.remove(0);
                    ys.remove(0);
                    return match(ts, ys);
                }else {
                    return Optional.empty();
                }
            }else if(ts.size() > 0 && ys.size() > 0 && (ts.get(0) instanceof MfotlVar)) {
                Optional<Function<Integer, Object>> result = match(ts, ys);
                if(!result.isPresent()){
                    return Optional.empty();
                }else{
                    Function<Integer, Object> f = result.get();
                    //if(f.apply())
                }
            }else{
                return Optional.empty();
            }
        }
        return null;

    }




}
