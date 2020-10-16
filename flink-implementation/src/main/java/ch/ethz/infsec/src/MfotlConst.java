package ch.ethz.infsec.src;

public class MfotlConst<T> extends MfotlTerm {
    public final T value;

    public MfotlConst(T value) {
        this.value = value;
    }
}