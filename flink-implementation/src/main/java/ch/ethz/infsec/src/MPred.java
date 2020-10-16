package ch.ethz.infsec.src;


import java.util.List;

public class MPred<E> implements Mformula<E> {
    String Mfotlname; //not sure if it's ok to express MFOTL.trm as a String
    List<MfotlTerm<E>> args;
}
