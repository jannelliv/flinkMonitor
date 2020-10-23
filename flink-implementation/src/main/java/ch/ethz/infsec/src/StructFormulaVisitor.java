package ch.ethz.infsec.src;
import ch.ethz.infsec.monitor.Fact;
import org.apache.flink.streaming.api.datastream.*;

import java.util.List;
import java.util.Optional;

//does this have to be abstract??
public abstract class StructFormulaVisitor implements FormulaVisitor {
    public abstract DataStream<List<Optional<Object>>> visit(JavaPred f);
    public abstract DataStream<List<Optional<Object>>> visit(JavaNot f);
    public abstract DataStream<List<Optional<Object>>> visit(JavaAnd f);
    public abstract DataStream<List<Optional<Object>>> visit(JavaAll f);
    public abstract DataStream<List<Optional<Object>>> visit(JavaEx f);
    public abstract DataStream<List<Optional<Object>>> visit(JavaFalse f);
    public abstract DataStream<List<Optional<Object>>> visit(JavaTrue f);
    public abstract DataStream<List<Optional<Object>>> visit(JavaNext f);
    public abstract DataStream<List<Optional<Object>>> visit(JavaOr f);
    public abstract DataStream<List<Optional<Object>>> visit(JavaPrev f);
    public abstract DataStream<List<Optional<Object>>> visit(JavaSince f);
    public abstract DataStream<List<Optional<Object>>> visit(JavaUntil f);

}