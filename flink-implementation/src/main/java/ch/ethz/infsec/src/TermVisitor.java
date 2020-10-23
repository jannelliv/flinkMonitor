package ch.ethz.infsec.src;
import ch.ethz.infsec.monitor.Fact;
import org.apache.flink.streaming.api.datastream.*;

public interface TermVisitor<T> {
    public T visit(JavaVar f);
    public T visit(JavaConst f);


}