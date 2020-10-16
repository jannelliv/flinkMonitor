package ch.ethz.infsec.src;

import java.util.List;

import java.util.Optional;

public class MSince<E> implements Mformula<E> {

    boolean bool; //"We ignore this and assume it's true"
    Mformula<E> formula1;
    Interval interval;
    Mformula<E> formula2;
    Tuple<List<Table<E>>, List<Table<E>>> mbuf2;
    List<Integer> tsList;
    List<Triple<Integer, Table<E>, Table<E>>> muaux;


}
