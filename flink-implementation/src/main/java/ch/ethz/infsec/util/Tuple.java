package ch.ethz.infsec.util;


import java.io.Serializable;
//I used to have just my own Tuple objects, but that didn't work because they were not serializable
public class Tuple<T, U> implements Serializable {
    public T fst;
    public U snd;

    public Tuple(T fst, U snd){
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

