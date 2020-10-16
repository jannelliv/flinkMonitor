package ch.ethz.infsec.src;
import org.apache.flink.streaming.api.datastream.*;
import ch.ethz.infsec.policy.GenFormula;
import ch.ethz.infsec.policy.True;

public interface JavaGenFormula extends GenFormula {
    public <T> T accept(FormulaVisitor<T> v);
}


