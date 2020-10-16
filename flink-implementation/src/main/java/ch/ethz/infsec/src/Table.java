package ch.ethz.infsec.src;



import java.util.List;



import java.util.ArrayList;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.Iterator;

//@FunctionalInterface
//interface MyUnaryFunction<T>{
//	Optional<T> ufunction(int a);
//}
//You don't need this since you are using the Function.

public class Table<T> {


    Set<List<Optional<T>>> table;

    public Table() {
        table = new HashSet<List<Optional<T>>>();
    }

    public List<Optional<T>> tabulate(Function<Integer, Optional<T>> muf, int x, int n){
        if(n == 0) {
            List<Optional<T>> result = new ArrayList<Optional<T>>();
            return result;
        }else {
            List<Optional<T>> temporary = tabulate(muf, x + 1, n - 1);
            temporary.add(0,  muf.apply(x));
            return temporary;
        }
    }

    public Set<List<Optional<T>>> singleton_table(int n, int i, T x){//not sure about the type of x
        Function<Integer, Optional<T>> muf = (j) -> {if (i == j) {return Optional.of(x);}else{return Optional.empty();}};
        Set<List<Optional<T>>> result = new HashSet<List<Optional<T>>>();
        result.add(tabulate(muf, 0, n));
        return result;
    }

    public Set<List<Optional<T>>> unit_table(int n){
        List<Optional<T>> list = new ArrayList<Optional<T>>();
        for(int i = 0; i < n; i++) {
            list.add(Optional.empty());
        }
        Set<List<Optional<T>>> result = new HashSet<List<Optional<T>>>();
        result.add(list);
        return result;
    }

    public Set<List<Optional<T>>> join(Set<List<Optional<T>>> table, boolean pos, Set<List<Optional<T>>> table2){

        Set<List<Optional<T>>> result = new HashSet<List<Optional<T>>>();
        Iterator<List<Optional<T>>> it = table.iterator();

        while(it.hasNext()) {
            Iterator<List<Optional<T>>> it2 = table2.iterator();
            while(it2.hasNext()) {
                Optional<List<Optional<T>>> tupleRes = join1(it.next(), it2.next());
                if(tupleRes.isPresent()) {
                    List<Optional<T>> tuple = tupleRes.get();
                    result.add(tuple);
                }
            }
        }
        if(pos) {
            return result;
        }else {
            //we don't have to do anything here right?
            table.removeAll(result);
            return table;

        }

    }

    public Optional<List<Optional<T>>> join1(List<Optional<T>> a, List<Optional<T>> b){
        if(a.size() == 0 && b.size() == 0) {
            List<Optional<T>> emptyList = new ArrayList<Optional<T>>();
            Optional<List<Optional<T>>> result = Optional.of(emptyList);
            return result;
        }else {
            Optional<T> x = a.remove(0);
            Optional<T> y = b.remove(0);
            Optional<List<Optional<T>>> subResult = join1(a, b);
            if(!x.isPresent() && !y.isPresent()) {

                if(!subResult.isPresent()) {
                    Optional<List<Optional<T>>> result = Optional.empty();
                    return result;
                }else {
                    List<Optional<T>> consList = new ArrayList<Optional<T>>();
                    consList.add(Optional.empty());
                    consList.addAll(subResult.get());
                    //Problem: get() can only return a value if the wrapped object is not null;
                    //otherwise, it throws a no such element exception
                    Optional<List<Optional<T>>> result = Optional.of(consList);
                    return result;
                }
            }else if(x.isPresent() && !y.isPresent()) {


                if(!subResult.isPresent()) {
                    Optional<List<Optional<T>>> result = Optional.empty();
                    return result;
                }else {
                    List<Optional<T>> consList = new ArrayList<Optional<T>>();
                    consList.add(x);
                    consList.addAll(subResult.get());
                    Optional<List<Optional<T>>> result = Optional.of(consList);
                    return result;
                }
            }else if(!x.isPresent() && y.isPresent()) {

                if(!subResult.isPresent()) {
                    Optional<List<Optional<T>>> result = Optional.empty();
                    return result;
                }else {
                    List<Optional<T>> consList = new ArrayList<Optional<T>>();
                    consList.add(y);
                    consList.addAll(subResult.get());
                    Optional<List<Optional<T>>> result = Optional.of(consList);
                    return result;
                }
            }else if(x.isPresent() && y.isPresent() || x!=y) {
                if(!subResult.isPresent()) {
                    Optional<List<Optional<T>>> result = Optional.empty();
                    return result;
                }else {
                    if(x==y) { //should enter this clause automatically
                        List<Optional<T>> consList = new ArrayList<Optional<T>>();
                        consList.add(x);
                        consList.addAll(subResult.get());
                        Optional<List<Optional<T>>> result = Optional.of(consList);
                        return result;
                    }
                }
            }else {
                Optional<List<Optional<T>>> result = Optional.empty();
                return result;
            }
        }
        return null; //not sure why this is necessary

    }



}
