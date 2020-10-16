package ch.ethz.infsec.src;


import java.util.List;
import java.util.Optional;

public class MPrev<E> implements Mformula<E> {
    Interval interval;
    Mformula<E> formula;
    boolean bool;
    List<List<List<Optional<E>>>> tableList;
    List<Integer> tsList;
}