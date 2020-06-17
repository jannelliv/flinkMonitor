package ch.ethz.infsec.generator;

import java.util.Arrays;
import java.util.List;

interface EventPattern extends Signature {

    @Override
    default List<String> getEvents(){
        return Arrays.asList(getBaseEvent(), getPositiveEvent(), getNegativeEvent());
    }

    @Override
    default int getArity(String e) {
        return getArguments(e).size();
    }

    List<String> getVariables();

    String getBaseEvent();

    String getPositiveEvent();

    String getNegativeEvent();

    List<String> getArguments(String event);
}
