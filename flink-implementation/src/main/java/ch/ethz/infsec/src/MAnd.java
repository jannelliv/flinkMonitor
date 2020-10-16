package ch.ethz.infsec.src;

import java.util.List;

public class MAnd<E> implements Mformula<E> {
    boolean bool;
    Mformula<E> op1;
    Mformula<E> op2;
    Tuple<List<Table<E>>, List<Table<E>>> mbuf2;
}
