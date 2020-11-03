package ch.ethz.infsec.src;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import java.util.List;
import java.util.Optional;


public interface Mformula<X> extends FlatMapFunction<X, List<Optional<Object>>>,
        CoFlatMapFunction<List<Optional<Object>>, List<Optional<Object>>, List<Optional<Object>>>{
    <T> DataStream<List<Optional<Object>>> accept(MformulaVisitor<T> v);
    List<String> freeVariablesInOrder = null;


}
