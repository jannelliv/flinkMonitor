package ch.ethz.infsec.src;

public class Triple<T, U, V> {
    public T fst;
    public U snd;
    public V thrd;

    Triple(T fst, U snd, V thrd){
        this.fst = fst;
        this.snd = snd;
        this.thrd = thrd;
    }

}
