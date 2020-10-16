package ch.ethz.infsec.src;


import java.util.List;

public class MOr<E> implements Mformula<E> {

    Mformula<E> op1;
    Mformula<E> op2;
    Tuple<List<Table<E>>, List<Table<E>>> mbuf2;
}

