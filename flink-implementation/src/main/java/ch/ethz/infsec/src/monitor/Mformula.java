package ch.ethz.infsec.src.monitor;
import org.apache.flink.streaming.api.datastream.DataStream;
import java.util.Optional;

import ch.ethz.infsec.src.util.*;
import ch.ethz.infsec.src.monitor.visitor.*;


public interface Mformula {
    <T> DataStream<PipelineEvent> accept(MformulaVisitor<T> v);



}
