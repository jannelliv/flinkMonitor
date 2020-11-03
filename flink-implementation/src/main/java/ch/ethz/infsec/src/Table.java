package ch.ethz.infsec.src;
import java.util.*;
import java.util.function.Function;

public class Table {


    Set<List<Optional<Object>>> table;

    public Table() {
        table = new HashSet<List<Optional<Object>>>();
    }

    public List<Optional<Object>> tabulate(Function<Integer, Optional<Object>> muf, int x, int n){
        if(n == 0) {
            List<Optional<Object>> result = new ArrayList<Optional<Object>>();
            return result;
        }else {
            List<Optional<Object>> temporary = tabulate(muf, x + 1, n - 1);
            temporary.add(0,  muf.apply(x));
            return temporary;
        }
    }

    public Set<List<Optional<Object>>> singleton_table(int n, int i, Object x){//not sure about the type of x
        Function<Integer, Optional<Object>> muf = (j) -> {if (i == j) {return Optional.of(x);}else{return Optional.empty();}};
        Set<List<Optional<Object>>> result = new HashSet<List<Optional<Object>>>();
        result.add(tabulate(muf, 0, n));
        return result;
    }

    public Set<List<Optional<Object>>> unit_table(int n){
        List<Optional<Object>> list = new LinkedList<Optional<Object>>();
        for(int i = 0; i < n; i++) {
            list.add(Optional.empty());
        }
        Set<List<Optional<Object>>> result = new HashSet<List<Optional<Object>>>();
        result.add(list);
        return result;
    }

    public Set<List<Optional<Object>>> join(Set<List<Optional<Object>>> table, boolean pos, Set<List<Optional<Object>>> table2){

        Set<List<Optional<Object>>> result = new HashSet<List<Optional<Object>>>();
        Iterator<List<Optional<Object>>> it = table.iterator();

        while(it.hasNext()) {
            Iterator<List<Optional<Object>>> it2 = table2.iterator();
            while(it2.hasNext()) {
                Optional<List<Optional<Object>>> tupleRes = join1(it.next(), it2.next());
                if(tupleRes.isPresent()) {
                    List<Optional<Object>> tuple = tupleRes.get();
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

    public Optional<List<Optional<Object>>> join1(List<Optional<Object>> a, List<Optional<Object>> b){
        if(a.size() == 0 && b.size() == 0) {
            List<Optional<Object>> emptyList = new ArrayList<Optional<Object>>();
            Optional<List<Optional<Object>>> result = Optional.of(emptyList);
            return result;
        }else {
            Optional<Object> x = a.remove(0);
            Optional<Object> y = b.remove(0);
            Optional<List<Optional<Object>>> subResult = join1(a, b);
            if(!x.isPresent() && !y.isPresent()) {

                if(!subResult.isPresent()) {
                    Optional<List<Optional<Object>>> result = Optional.empty();
                    return result;
                }else {
                    List<Optional<Object>> consList = new ArrayList<Optional<Object>>();
                    consList.add(Optional.empty());
                    consList.addAll(subResult.get());
                    //Problem: get() can only return a value if the wrapped object is not null;
                    //otherwise, it throws a no such element exception
                    Optional<List<Optional<Object>>> result = Optional.of(consList);
                    return result;
                }
            }else if(x.isPresent() && !y.isPresent()) {


                if(!subResult.isPresent()) {
                    Optional<List<Optional<Object>>> result = Optional.empty();
                    return result;
                }else {
                    List<Optional<Object>> consList = new ArrayList<Optional<Object>>();
                    consList.add(x);
                    consList.addAll(subResult.get());
                    Optional<List<Optional<Object>>> result = Optional.of(consList);
                    return result;
                }
            }else if(!x.isPresent() && y.isPresent()) {

                if(!subResult.isPresent()) {
                    Optional<List<Optional<Object>>> result = Optional.empty();
                    return result;
                }else {
                    List<Optional<Object>> consList = new ArrayList<Optional<Object>>();
                    consList.add(y);
                    consList.addAll(subResult.get());
                    Optional<List<Optional<Object>>> result = Optional.of(consList);
                    return result;
                }
            }else if(x.isPresent() && y.isPresent() || x!=y) {
                if(!subResult.isPresent()) {
                    Optional<List<Optional<Object>>> result = Optional.empty();
                    return result;
                }else {
                    if(x==y) { //should enter this clause automatically
                        List<Optional<Object>> consList = new ArrayList<Optional<Object>>();
                        consList.add(x);
                        consList.addAll(subResult.get());
                        Optional<List<Optional<Object>>> result = Optional.of(consList);
                        return result;
                    }
                }
            }else {
                Optional<List<Optional<Object>>> result = Optional.empty();
                return result;
            }
        }
        return null; //not sure why this is necessary

    }



}
