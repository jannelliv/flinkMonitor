package ch.ethz.infsec.src;


import java.util.*;

import ch.ethz.infsec.monitor.Fact;
import ch.ethz.infsec.trace.parser.TraceParser;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.util.Collector;

class ParsingFunction implements FlatMapFunction<String, Fact>, ListCheckpointed<TraceParser> {
    //not sure what to do about the "deprecated" above
        TraceParser parser;
    public ParsingFunction(TraceParser tp){
        this.parser = tp;
    }

    @Override
    public void flatMap(String line, Collector<Fact> collector) throws Exception {
            //init parser!
        this.parser.parseLine( x -> {collector.collect(x);}, line);
    }

    @Override
    public List<TraceParser> snapshotState(long checkpointId, long timestamp) throws Exception {

        return new ArrayList<>(Collections.singleton(parser));
    }

    @Override
    public void restoreState(List<TraceParser> state) throws Exception {
        assert(state.size() == 1);
        parser = state.get(0);
    }
}
