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

    @Override
    default String getString() {
        StringBuilder signature = new StringBuilder();
        for (String event : new String[] {getBaseEvent(), getPositiveEvent(), getNegativeEvent()}) {
            signature.append(event).append('(');
            final int arity = getArguments(event).size();
            for (int i = 0; i < arity; ++i) {
                if (i > 0) {
                    signature.append(',');
                }
                signature.append("int");
            }
            signature.append(")\n");
        }
        return signature.toString();
    }

    List<String> getVariables();

    String getBaseEvent();

    String getPositiveEvent();

    String getNegativeEvent();

    List<String> getArguments(String event);
}
