package ch.ethz.infsec.src;

import java.util.List;
import java.util.Optional;

public class MNext<E> implements Mformula<E> {
    Interval interval;
    Mformula<E> formula;
    boolean bool;
    List<Integer> tsList;
}
