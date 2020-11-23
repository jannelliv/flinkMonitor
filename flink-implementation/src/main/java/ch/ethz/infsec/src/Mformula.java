package ch.ethz.infsec.src;
import org.apache.flink.streaming.api.datastream.DataStream;
import java.util.Optional;


public interface Mformula {
    <T> DataStream<PipelineEvent> accept(MformulaVisitor<T> v);



}
