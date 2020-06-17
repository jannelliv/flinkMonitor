package ch.ethz.infsec.generator;


import java.util.List;


interface Signature {
    List<String> getEvents();
    int getArity(String e);

}