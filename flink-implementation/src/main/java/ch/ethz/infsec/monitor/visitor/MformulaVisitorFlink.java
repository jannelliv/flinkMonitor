package ch.ethz.infsec.monitor.visitor;
import ch.ethz.infsec.monitor.Fact;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;
import java.util.HashMap;

import ch.ethz.infsec.monitor.*;
import ch.ethz.infsec.util.*;
import static ch.ethz.infsec.formula.JavaGenFormula.convert;

public class MformulaVisitorFlink implements MformulaVisitor<DataStream<PipelineEvent>> {

    HashMap<String, OutputTag<Fact>> hmap;
    SingleOutputStreamOperator<Fact> mainDataStream;

    public MformulaVisitorFlink(HashMap<String, OutputTag<Fact>> hmap, SingleOutputStreamOperator<Fact> mainDataStream){
        this.hmap = hmap;
        this.mainDataStream = mainDataStream;
    }

    public DataStream<PipelineEvent> visit(MPred state) {
        OutputTag<Fact> factStream = this.hmap.get(state.getPredName());
        return this.mainDataStream.getSideOutput(factStream).flatMap(state);
    }

    public DataStream<PipelineEvent> visit(MAnd f) {
        DataStream<PipelineEvent> input1 = f.op1.accept(this);
        DataStream<PipelineEvent> input2 = f.op2.accept(this);
        ConnectedStreams<PipelineEvent, PipelineEvent> connectedStreams = input1.connect(input2);
        return connectedStreams.flatMap(f);
    }

    public DataStream<PipelineEvent> visit(MExists f) {
        DataStream<PipelineEvent> input = f.subFormula.accept(this);
        return input.flatMap(f);
    }

    public DataStream<PipelineEvent> visit(MNext f) {
        DataStream<PipelineEvent> input = f.formula.accept(this);
        return input.flatMap(f);
    }

    public DataStream<PipelineEvent> visit(MOr f) {
        DataStream<PipelineEvent> input1 = f.op1.accept(this);
        DataStream<PipelineEvent> input2 = f.op2.accept(this);
        ConnectedStreams<PipelineEvent, PipelineEvent> connectedStreams = input1.connect(input2);
        return connectedStreams.flatMap(f);
        //flatMap here will be interpreted as a coflatmap because the argumetn it receives is an Or,
        //which is a binary operator so it receives a coflatmap. This will apply flatMap1 or flatMap2 depending
        //on the input stream
    }

    public DataStream<PipelineEvent> visit(MPrev f) {
        DataStream<PipelineEvent> input = f.formula.accept(this);
        return input.flatMap(f);
    }

    public DataStream<PipelineEvent> visit(MSince f) {
        DataStream<PipelineEvent> input1 = f.formula1.accept(this);
        DataStream<PipelineEvent> input2 = f.formula2.accept(this);
        ConnectedStreams<PipelineEvent, PipelineEvent> connectedStreams = input1.connect(input2);
        return connectedStreams.flatMap(f);
    }

    public DataStream<PipelineEvent> visit(MUntil f) {
        DataStream<PipelineEvent> input1 = f.formula1.accept(this);
        DataStream<PipelineEvent> input2 = f.formula2.accept(this);
        ConnectedStreams<PipelineEvent, PipelineEvent> connectedStreams = input1.connect(input2);
        return connectedStreams.flatMap(f);
    }

    @Override
    public DataStream<PipelineEvent> visit(MOnce f) {
        DataStream<PipelineEvent> input = f.formula.accept(this);
        return input.flatMap(f);
    }

    @Override
    public DataStream<PipelineEvent> visit(MEventually f) {
        DataStream<PipelineEvent> input = f.formula.accept(this);
        return input.flatMap(f);
    }

    public DataStream<PipelineEvent> visit(MRel f) {
        OutputTag<Fact> factStream = this.hmap.get("");
        return this.mainDataStream.getSideOutput(factStream).flatMap(f);
    }

}
