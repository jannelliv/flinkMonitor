package ch.ethz.infsec.src;

import ch.ethz.infsec.monitor.Fact;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;
import java.util.HashMap;
import java.util.Optional;

public class MformulaVisitorFlink implements MformulaVisitor<DataStream<Optional<Assignment>>> {

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

    public DataStream<Optional<Assignment>> visit(MPred state) {
        OutputTag<Fact> factStream = this.hmap.get(state.getPredName());
        return this.mainDataStream.getSideOutput(factStream).flatMap(state );
    }

    public DataStream<Optional<Assignment>> visit(MAnd f) {
        //when do I call flatMap1 and flatMap2?
        DataStream<Optional<Assignment>> input1 = f.accept(new MformulaVisitorFlink(f, f.op1, hmap, mainDataStream));
        DataStream<Optional<Assignment>> input2 = f.accept(new MformulaVisitorFlink(f, f.op2, hmap, mainDataStream));
        ConnectedStreams<Optional<Assignment>, Optional<Assignment>> connectedStreams = input1.connect(input2);
        return connectedStreams.flatMap(f);
    }

    public DataStream<Optional<Assignment>> visit(MExists f) {
        DataStream<Optional<Assignment>> input = f.accept(new MformulaVisitorFlink(f, f.subFormula, hmap, mainDataStream));
        return input.flatMap(f);
    }

    public DataStream<Optional<Assignment>> visit(MNext f) {
        DataStream<Optional<Assignment>> input = f.accept(new MformulaVisitorFlink(f, f.formula, hmap, mainDataStream));
        return input.flatMap(f);
    }

    public DataStream<Optional<Assignment>> visit(MOr f) {
        DataStream<Optional<Assignment>> input1 = f.accept(new MformulaVisitorFlink(f, f.op1, hmap, mainDataStream));
        DataStream<Optional<Assignment>> input2 = f.accept(new MformulaVisitorFlink(f, f.op2, hmap, mainDataStream));
        ConnectedStreams<Optional<Assignment>, Optional<Assignment>> connectedStreams = input1.connect(input2);
        //coflatmap goes from connected streams to data streams --> see below
        //this flat map below is actually a coflatmap

        return connectedStreams.flatMap(f);
        //flatMap here will be interpreted as a coflatmap because the argumetn it receives is an Or,
        //which is a binary operator so it receives a coflatmap. This will apply flatMap1 or flatMap2 depending
        //on the input stream
    }

    public DataStream<Optional<Assignment>> visit(MPrev f) {
        DataStream<Optional<Assignment>> input = f.accept(new MformulaVisitorFlink(f, f.formula, hmap, mainDataStream));
        return input.flatMap(f);
    }

    public DataStream<Optional<Assignment>> visit(MSince f) {
        DataStream<Optional<Assignment>> input1 = f.accept(new MformulaVisitorFlink(f, f.formula1, hmap, mainDataStream));
        DataStream<Optional<Assignment>> input2 = f.accept(new MformulaVisitorFlink(f, f.formula2, hmap, mainDataStream));
        ConnectedStreams<Optional<Assignment>, Optional<Assignment>> connectedStreams = input1.connect(input2);
        return connectedStreams.flatMap(f);
    }

    public DataStream<Optional<Assignment>> visit(MUntil f) {
        DataStream<Optional<Assignment>> input1 = f.accept(new MformulaVisitorFlink(f, f.formula1, hmap, mainDataStream));
        DataStream<Optional<Assignment>> input2 = f.accept(new MformulaVisitorFlink(f, f.formula2, hmap, mainDataStream));
        ConnectedStreams<Optional<Assignment>, Optional<Assignment>> connectedStreams = input1.connect(input2);
        return connectedStreams.flatMap(f);
    }

    public DataStream<Optional<Assignment>> visit(MRel f) {
        OutputTag<Fact> factStream = this.hmap.get("");
        return this.mainDataStream.getSideOutput(factStream).flatMap(f);
    }

    //public abstract DataStream<Fact> visit(JavaFalse f);
    //public abstract DataStream<Fact> visit(JavaTrue f);
    //public abstract DataStream<Fact> visit(JavaAll f);
    //public abstract DataStream<Fact> visit(MNot f);
}
