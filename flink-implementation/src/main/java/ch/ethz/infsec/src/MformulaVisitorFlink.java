package ch.ethz.infsec.src;

import ch.ethz.infsec.monitor.Fact;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.List;
import java.util.Optional;

public class MformulaVisitorFlink implements MformulaVisitor<DataStream<List<Optional<Object>>>> {

    Mformula formula;
    Mformula subformula;
    HashMap<String, OutputTag<Fact>> hmap;
    SingleOutputStreamOperator<Fact> mainDataStream;

    public MformulaVisitorFlink(Mformula formula, Mformula subformula, HashMap<String, OutputTag<Fact>> hmap, SingleOutputStreamOperator<Fact> mainDataStream){
        this.formula = formula;
        this.subformula = subformula;
        this.hmap = hmap;
        this.mainDataStream = mainDataStream;
    }

    public DataStream<List<Optional<Object>>> visit(MPred event) {
        OutputTag<Fact> factStream = this.hmap.get(event.toString());
        return this.mainDataStream.getSideOutput(factStream).flatMap(event);
    }

    public DataStream<List<Optional<Object>>> visit(MAnd f) {
        DataStream<List<Optional<Object>>> input1 = f.accept(new MformulaVisitorFlink(f, f.op1, hmap, mainDataStream));
        DataStream<List<Optional<Object>>> input2 = f.accept(new MformulaVisitorFlink(f, f.op2, hmap, mainDataStream));
        ConnectedStreams<List<Optional<Object>>, List<Optional<Object>>> connectedStreams = input1.connect(input2);
        return connectedStreams.flatMap(f);
    }

    public DataStream<List<Optional<Object>>> visit(MExists f) {
        DataStream<List<Optional<Object>>> input = f.accept(new MformulaVisitorFlink(f, f.subFormula, hmap, mainDataStream));
        return input.flatMap(f);
    }

    public DataStream<List<Optional<Object>>> visit(MNext f) {
        DataStream<List<Optional<Object>>> input = f.accept(new MformulaVisitorFlink(f, f.formula, hmap, mainDataStream));
        return input.flatMap(f);
    }

    public DataStream<List<Optional<Object>>> visit(MOr f) {
        DataStream<List<Optional<Object>>> input1 = f.accept(new MformulaVisitorFlink(f, f.op1, hmap, mainDataStream));
        DataStream<List<Optional<Object>>> input2 = f.accept(new MformulaVisitorFlink(f, f.op2, hmap, mainDataStream));
        ConnectedStreams<List<Optional<Object>>, List<Optional<Object>>> connectedStreams = input1.connect(input2);
        return connectedStreams.flatMap(f);
    }

    public DataStream<List<Optional<Object>>> visit(MPrev f) {
        DataStream<List<Optional<Object>>> input = f.accept(new MformulaVisitorFlink(f, f.formula, hmap, mainDataStream));
        return input.flatMap(f);
    }

    public DataStream<List<Optional<Object>>> visit(MSince f) {
        DataStream<List<Optional<Object>>> input1 = f.accept(new MformulaVisitorFlink(f, f.formula1, hmap, mainDataStream));
        DataStream<List<Optional<Object>>> input2 = f.accept(new MformulaVisitorFlink(f, f.formula2, hmap, mainDataStream));
        ConnectedStreams<List<Optional<Object>>, List<Optional<Object>>> connectedStreams = input1.connect(input2);
        return connectedStreams.flatMap(f);
    }

    public DataStream<List<Optional<Object>>> visit(MUntil f) {
        DataStream<List<Optional<Object>>> input1 = f.accept(new MformulaVisitorFlink(f, f.formula1, hmap, mainDataStream));
        DataStream<List<Optional<Object>>> input2 = f.accept(new MformulaVisitorFlink(f, f.formula2, hmap, mainDataStream));
        ConnectedStreams<List<Optional<Object>>, List<Optional<Object>>> connectedStreams = input1.connect(input2);
        return connectedStreams.flatMap(f);
    }

    public DataStream<List<Optional<Object>>> visit(MRel f) {
        return null;
    }

    //public abstract DataStream<Fact> visit(JavaFalse f);
    //public abstract DataStream<Fact> visit(JavaTrue f);
    //public abstract DataStream<Fact> visit(JavaAll f);
    //public abstract DataStream<Fact> visit(MNot f);
}
