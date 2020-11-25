package ch.ethz.infsec.src;


import java.io.Serializable;

public class Tuple<T, U> implements Serializable {
    public T fst;
    public U snd;

    Tuple(T fst, U snd){
        this.fst = fst;
        this.snd = snd;
    }

    public T fst() {
        return this.fst;
    }

    public U snd() {
        return this.snd;
    }

}

