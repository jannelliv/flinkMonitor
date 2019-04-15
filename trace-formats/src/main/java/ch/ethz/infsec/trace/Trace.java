package ch.ethz.infsec.trace;

import ch.ethz.infsec.monitor.Fact;

public class Trace {
    public static final String EVENT_FACT = "";

    public static boolean isEventFact(Fact fact) {
        return EVENT_FACT.equals(fact.getName());
    }

    private Trace() { }
}
