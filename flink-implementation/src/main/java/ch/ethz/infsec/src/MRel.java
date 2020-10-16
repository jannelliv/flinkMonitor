package ch.ethz.infsec.src;

import java.util.List;
import java.util.Optional;

public class MRel<E> implements Mformula<E> {
    List<List<Optional<E>>> table;
}