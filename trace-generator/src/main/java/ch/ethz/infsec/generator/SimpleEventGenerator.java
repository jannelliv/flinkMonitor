package ch.ethz.infsec.generator;


import org.apache.commons.math3.distribution.IntegerDistribution;
import org.apache.commons.math3.distribution.UniformIntegerDistribution;
import org.apache.commons.math3.random.RandomGenerator;

import java.util.*;

final class SimpleEventGenerator extends AbstractEventGenerator {

    private SimpleSignature sig;
    private final IntegerDistribution eventDistribution;
    private final IntegerDistribution valueDistribution;
    private final Queue<Integer> values;
    private final int domainSize;
    private final float newFraction;

    SimpleEventGenerator(RandomGenerator random, int eventRate, int indexRate, long firstTimestamp, SimpleSignature s, int ds, float nf) {
        super(random, eventRate, indexRate, firstTimestamp);
        this.sig = s;
        this.domainSize = ds;
        this.newFraction = nf;
        values = new ArrayDeque<Integer>(domainSize);
        eventDistribution = new UniformIntegerDistribution(random, 0, sig.getEvents().size()-1);
        valueDistribution = new UniformIntegerDistribution(random, 0, 999_999_999);

    }

    @Override
    void appendNextEvent(StringBuilder builder, long timestamp) {
        //Sample event name
        List<String> events = sig.getEvents();

        String sampledEvent = events.get(eventDistribution.sample());

        int[] attrs = new int[sig.getArity(sampledEvent)];

        for (int i = 0; i<attrs.length; i++){
            attrs[i] = sampleValue();
        }
        appendEvent(builder, sampledEvent, attrs);
    }

    private int sampleValue(){

        if (values.size()>domainSize/2){

            int dom = Math.max(values.size(),1);
            int dist= Math.round(dom/(1-newFraction));

            int sampleInd = valueDistribution.sample()%dist;

            if (sampleInd<values.size()) {
                return (Integer)values.toArray()[sampleInd];
            }
        }
        Integer value = valueDistribution.sample();
        if(!values.contains(value)){
            if(values.size()==domainSize){
                values.remove();
            }
            values.add(value);
        }
        return value;

    }

    private void appendEvent(StringBuilder builder, String relation, int[] attributes) {
        appendEventStart(builder, relation);
        for (int i = 0; i < attributes.length; ++i) {
            appendAttribute(builder, "x" + i, attributes[i]);
        }
    }

    @Override
    void initialize() {

    }

    public static AbstractEventGenerator getInstance(RandomGenerator random,
                                                     int eventRate,
                                                     int indexRate,
                                                     long firstTimestamp,
                                                     SimpleSignature s,
                                                     int ds,
                                                     float nf){
        return new SimpleEventGenerator(random, eventRate, indexRate, firstTimestamp, s, ds, nf);
    }

}
