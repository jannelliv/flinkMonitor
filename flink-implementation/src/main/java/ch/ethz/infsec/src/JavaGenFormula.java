package ch.ethz.infsec.src;
import org.apache.flink.streaming.api.datastream.*;
import ch.ethz.infsec.policy.JavaGenFormulaUnsealed;
import ch.ethz.infsec.policy.True;
import org.apache.kafka.common.utils.Java;

public interface JavaGenFormula extends JavaGenFormulaUnsealed {
    public <T> T accept(FormulaVisitor<T> v);
}




