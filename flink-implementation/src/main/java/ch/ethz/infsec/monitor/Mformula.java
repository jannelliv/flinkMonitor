package ch.ethz.infsec.monitor;
import org.apache.flink.streaming.api.datastream.DataStream;
import java.util.Optional;

import ch.ethz.infsec.util.*;
import ch.ethz.infsec.monitor.visitor.*;


public interface Mformula {
    <T> DataStream<PipelineEvent> accept(MformulaVisitor<T> v);



}
