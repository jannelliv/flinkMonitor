package ch.ethz.infsec.src;
import ch.ethz.infsec.monitor.Fact;
import org.apache.flink.streaming.api.datastream.*;

//does this have to be abstract??
public abstract class MevalFormulaVisitor implements FormulaVisitor {
    public abstract DataStream<Fact> visit(JavaPred f);
    public abstract DataStream<Fact> visit(JavaNot f);
    public abstract DataStream<Fact> visit(JavaAnd f);
    public abstract DataStream<Fact> visit(JavaAll f);
    public abstract DataStream<Fact> visit(JavaEx f);
    public abstract DataStream<Fact> visit(JavaFalse f);
    public abstract DataStream<Fact> visit(JavaTrue f);
    public abstract DataStream<Fact> visit(JavaNext f);
    public abstract DataStream<Fact> visit(JavaOr f);
    public abstract DataStream<Fact> visit(JavaPrev f);
    public abstract DataStream<Fact> visit(JavaSince f);
    public abstract DataStream<Fact> visit(JavaUntil f);

}