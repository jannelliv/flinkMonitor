package ch.ethz.infsec.src;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;


public class ExecutableMonitor<T> {

    //Integer ts;
    //Tuple<List<Table<T>>, List<Table<T>>> mbuf2;
    //List<Tuple<Integer, Table<T>>> msaux; //clarify what msaux is!!
    //List<Triple<Integer, Table<T>, Table<T>>> muaux;


    Set<List<Optional<T>>> eq_rel(int n, MfotlTerm<T> x, MfotlTerm<T> y, Table<T> t){

        if(x instanceof MfotlConst && y instanceof MfotlConst) {
            if(x==y) {
                return t.unit_table(n);
            }else {
                Set<List<Optional<T>>> result = new HashSet<List<Optional<T>>>();
                return result;
            }
        }else if(x instanceof MfotlVar && y instanceof MfotlConst) {
            return (Set<List<Optional<T>>>) t.singleton_table(n,((MfotlVar) x).name,((MfotlConst<T>) y).value);
        }else if(y instanceof MfotlVar && x instanceof MfotlConst) {
            return (Set<List<Optional<T>>>) t.singleton_table(n,((MfotlVar) y).name,((MfotlConst<T>) x).value);
            //PROBLEM W/ ARGUMENT TYPES!
        }else {
            return null; //"undefined" in Isabelle
        }

    }

    Set<List<Optional<T>>> neq_rel(int n, MfotlTerm<T> x, MfotlTerm<T> y, Table<T> t){

        if(x instanceof MfotlConst && y instanceof MfotlConst) {
            if(x==y) {
                Set<List<Optional<T>>> result = new HashSet<List<Optional<T>>>();
                return result;
            }else {
                return t.unit_table(n);
            }
        }else if(x instanceof MfotlVar && y instanceof MfotlVar) {
            if(x==y) {
                Set<List<Optional<T>>> result = new HashSet<List<Optional<T>>>();
                return result;
            }else {
                return null;
            }
        }else {
            return null; //"undefined" in Isabelle
        }
    }

    Triple<List<Set<List<Optional<T>>>>, List<Set<List<Optional<T>>>>, List<Integer>> mprev_next(Interval i,
                                                                                                 List<Set<List<Optional<T>>>> xs, List<Integer> ts){
        if(xs.size() == 0) {
            List<Set<List<Optional<T>>>> fstResult = new ArrayList<Set<List<Optional<T>>>>();
            List<Set<List<Optional<T>>>> sndResult = new ArrayList<Set<List<Optional<T>>>>();
            return new Triple(fstResult,sndResult, ts);
        }else if(ts.size() == 0) {
            List<Set<List<Optional<T>>>> fstResult = new ArrayList<Set<List<Optional<T>>>>();
            List<Integer> thrdResult = new ArrayList<Integer>();
            return new Triple(fstResult,xs, thrdResult);
        }else if(ts.size() == 1) {
            List<Set<List<Optional<T>>>> fstResult = new ArrayList<Set<List<Optional<T>>>>();
            List<Integer> thrdResult = new ArrayList<Integer>();
            thrdResult.add(ts.get(0));
            return new Triple(fstResult,xs, thrdResult);
        }else if(xs.size() >= 1 && ts.size() >= 2) {
            Integer t = ts.get(0);
            Integer tp = ts.get(1);
            if(i.mem(tp - t)) {
                //???
            }
        }
        return null;

    }

    Tuple<List<Set<List<Optional<T>>>>, List<Set<List<Optional<T>>>>> mbuf2_add(List<Set<List<Optional<T>>>> xsp,
                                                                                List<Set<List<Optional<T>>>> ysp, Tuple<List<Set<List<Optional<T>>>>, List<Set<List<Optional<T>>>>> mbuf2){
        List<Set<List<Optional<T>>>> xs = mbuf2.fst();
        List<Set<List<Optional<T>>>> ys = mbuf2.snd();
        List<Set<List<Optional<T>>>> result1 = new ArrayList<Set<List<Optional<T>>>>();
        result1.addAll(xs);
        result1.addAll(xsp);
        List<Set<List<Optional<T>>>> result2 = new ArrayList<Set<List<Optional<T>>>>();
        result2.addAll(ys);
        result2.addAll(ysp);

        Tuple<List<Set<List<Optional<T>>>>, List<Set<List<Optional<T>>>>> result = new Tuple(result1, result2);
        return result;

    }

    <U> Tuple<List<U>, Tuple<List<Set<List<Optional<T>>>>, List<Set<List<Optional<T>>>>> > mbuf2_take(BiFunction<Set<List<Optional<T>>>, Set<List<Optional<T>>>, U> f,
                                                                                                      Tuple<List<Set<List<Optional<T>>>>, List<Set<List<Optional<T>>>>> mbuf2){

        List<Set<List<Optional<T>>>> xs = mbuf2.fst();
        List<Set<List<Optional<T>>>> ys = mbuf2.snd();
        if(xs.size() >= 1 && ys.size() >= 1) {
            Set<List<Optional<T>>> x = xs.remove(0);
            Set<List<Optional<T>>> y = ys.remove(0);
            Tuple<List<Set<List<Optional<T>>>>, List<Set<List<Optional<T>>>>> mbuf2p = new Tuple(xs, ys);
            Tuple<List<U>, Tuple<List<Set<List<Optional<T>>>>, List<Set<List<Optional<T>>>>> > zsbuf = mbuf2_take(f, mbuf2p);

            Tuple<List<Set<List<Optional<T>>>>, List<Set<List<Optional<T>>>>> buf = zsbuf.snd();
            List<U> zs = zsbuf.fst();
            U elem = f.apply(x,y);
            zs.add(0,elem);
            Tuple<List<U>, Tuple<List<Set<List<Optional<T>>>>, List<Set<List<Optional<T>>>>> > result = new Tuple(zs, buf);
            return result;
        }else {
            List<U> res1 = new ArrayList<U>();
            Tuple<List<U>, Tuple<List<Set<List<Optional<T>>>>, List<Set<List<Optional<T>>>>> > result = new Tuple(res1, mbuf2);
            return result;
        }
    }

    Optional<Function<Integer, T>> match(List<MfotlTerm<T>> ts, List<T> ys){
        if(ts.size() == 0 && ys.size() == 0) {
            Function<Integer, T> emptyMap = new Function<Integer, T>() {
                @Override
                public T apply(Integer integer) {
                    return null;
                }
            };
            Optional<Function<Integer, T>> result = Optional.of(emptyMap);
            return result;
        }else {
            if(ts.size() > 0 && ys.size() > 0 && (ts.get(0) instanceof MfotlConst<?>)) {
                if(ts.get(0) == ys.get(0)) {
                    ts.remove(0);
                    ys.remove(0);
                    return match(ts, ys);
                }else {
                    return Optional.empty();
                }
            }else if(ts.size() > 0 && ys.size() > 0 && (ts.get(0) instanceof MfotlVar)) {
                Optional<Function<Integer, T>> result = match(ts, ys);
                if(!result.isPresent()){
                    return Optional.empty();
                }else{
                    Function<Integer, T> f = result.get();
                    //if(f.apply())
                }
            }else{
                return Optional.empty();
            }
        }
        return null;

    }




}
