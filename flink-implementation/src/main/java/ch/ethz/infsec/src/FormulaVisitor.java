package ch.ethz.infsec.src;
import ch.ethz.infsec.monitor.Fact;
import org.apache.flink.streaming.api.datastream.*;

public interface FormulaVisitor<T> {
    public T visit(JavaPred f);
    public T visit(JavaNot f);
    public T visit(JavaAnd f);
    public T visit(JavaAll f);
    public T visit(JavaEx f);
    public T visit(JavaFalse f);
    public T visit(JavaTrue f);
    public T visit(JavaNext f);
    public T visit(JavaOr f);
    public T visit(JavaPrev f);
    public T visit(JavaSince f);
    public T visit(JavaUntil f);

}
