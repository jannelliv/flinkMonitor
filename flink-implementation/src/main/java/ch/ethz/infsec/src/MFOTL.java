package ch.ethz.infsec.src;

import java.util.List;
import java.util.Set;

public class MFOTL<T> {
    String name;
    Tuple<String, List<T>> event;
    Set<Tuple<String, List<T>>> database;

}
