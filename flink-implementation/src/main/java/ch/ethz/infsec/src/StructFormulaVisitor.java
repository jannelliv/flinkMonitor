package ch.ethz.infsec.src;

import org.apache.flink.streaming.api.datastream.*;

public abstract class StructFormulaVisitor implements FormulaVisitor {
    public abstract DataStream<Assignment> visit(JavaPred f);
    public abstract DataStream<Assignment> visit(JavaNot f);
    public abstract DataStream<Assignment> visit(JavaAnd f);
    public abstract DataStream<Assignment> visit(JavaAll f);
    public abstract DataStream<Assignment> visit(JavaEx f);
    public abstract DataStream<Assignment> visit(JavaFalse f);
    public abstract DataStream<Assignment> visit(JavaTrue f);
    public abstract DataStream<Assignment> visit(JavaNext f);
    public abstract DataStream<Assignment> visit(JavaOr f);
    public abstract DataStream<Assignment> visit(JavaPrev f);
    public abstract DataStream<Assignment> visit(JavaSince f);
    public abstract DataStream<Assignment> visit(JavaUntil f);

}