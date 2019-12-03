package ch.ethz.infsec.tools;

import ch.ethz.infsec.kafka.MonitorKafkaConfig;
import ch.ethz.infsec.monitor.Fact;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ReferenceMap;
import it.unimi.dsi.fastutil.longs.Long2ReferenceOpenHashMap;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.util.Collector;
import scala.Int;
import scala.Tuple2;
import java.util.Arrays;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;


class ReorderFunctionStateInstance {

}

@ForwardedFields({"1"})
public abstract class ReorderFunction extends RichFlatMapFunction<Tuple2<Int, Fact>, Fact> {
    protected int numSources = MonitorKafkaConfig.getNumPartitions();
    private Long2ReferenceMap<ReferenceArrayList<Fact>> idx2Facts = new Long2ReferenceOpenHashMap<>();
    private long[] maxOrderElem = new long[numSources];
    private long currentIdx = -2;
    private long maxIdx = -1;
    private int numEOF = 0;

    private transient ListState<Tuple2<Long, ReferenceArrayList<Fact>>> idx2facts_state = null;
    private transient ListState<Long> maxorderelem_state = null;
    private transient ListState<Long> indices_state = null;

    abstract protected boolean isOrderElement(Fact fact);
    abstract protected long indexExtractor(Fact fact);
    abstract protected Fact makeTerminator(long idx);

    ReorderFunction() {
        Arrays.fill(maxOrderElem, -1);
    }

    private void insertElement(Fact fact, int idx, long timeStamp, Collector<Fact> out) {
        ReferenceArrayList<Fact> buf = idx2Facts.get(timeStamp);
        if (buf == null) {
            ReferenceArrayList<Fact> nl = new ReferenceArrayList<>();
            nl.add(fact);
            idx2Facts.put(timeStamp, nl);
        } else {
            buf.add(fact);
        }
    }

    private void forceFlush(Collector<Fact> out) {
        if (maxIdx != -1) {
            for (long idx = currentIdx + 1; idx <= maxIdx; ++idx) {
                ReferenceArrayList<Fact> arr;
                if((arr = idx2Facts.get(idx)) != null) {
                    for (Fact fact: arr)
                        out.collect(fact);
                    if (idx > maxIdx) {
                        maxIdx = idx;
                        out.collect(makeTerminator(idx));
                    }
                }
            }
        }
    }

    private long minMaxOrderElem() {
        long min = Long.MAX_VALUE;
        for (long l: maxOrderElem) {
            if (l < min)
                min = l;
        }
        return min;
    }

    private void flushReady(Collector<Fact> out) {
        long maxAgreedIdx = minMaxOrderElem();
        if (maxAgreedIdx < currentIdx)
            return;

        for (long idx = currentIdx + 1; idx <= maxAgreedIdx; ++idx) {
            ReferenceArrayList<Fact> arr;
            if ((arr = idx2Facts.get(idx)) != null) {
                idx2Facts.remove(idx);
                for(Fact fact: arr)
                    out.collect(fact);
            }
            out.collect(makeTerminator(idx));
        }

        currentIdx = maxAgreedIdx;

    }

    @Override public void flatMap(Tuple2<Int, Fact> value, Collector<Fact> out) {
        int subtaskidx = (Integer)((Object) value._1);
        Fact fact = value._2;
        if (currentIdx == -2) {
            assert fact.isMeta() && fact.getName().equals("START");
            long first_idx = Long.parseLong((String) fact.getArgument(0));
            assert first_idx >= 0;
            currentIdx = first_idx - 1;
            return;
        }

        if (isOrderElement(fact)) {
            long idx = indexExtractor(fact);
            if (idx > maxOrderElem[subtaskidx])
                maxOrderElem[subtaskidx] = idx;
            flushReady(out);
            return;
        }

        if (fact.isMeta()) {
            if (fact.getName().equals("EOF")) {
                if((++numEOF) == numSources)
                    forceFlush(out);
                return;
            } else if (fact.getName().equals("START"))
                return;
            else
                throw new RuntimeException("should not happen");
        }
        long idx = indexExtractor(fact);
        maxIdx = Math.max(idx, maxIdx);

        if (idx < currentIdx)
            throw new RuntimeException("FATAL ERROR: Got a timestamp that should already be flushed");
        insertElement(fact, subtaskidx, idx, out);
    }
}

class ReorderTotalOrderFunction extends ReorderFunction {
    private Long2LongMap tpTotsMap = new Long2LongOpenHashMap();

    @Override
    protected boolean isOrderElement(Fact fact) {
        boolean ret = fact.isTerminator();
        if (ret) {
            long tp = fact.getTimepoint();
            long ts = fact.getTimestamp();
            tpTotsMap.put(tp, ts);
        }
        return ret;
    }

    @Override
    protected long indexExtractor(Fact fact) {
        return fact.getTimepoint();
    }

    @Override
    protected Fact makeTerminator(long idx) {
        long ts = tpTotsMap.get(idx);
        tpTotsMap.remove(idx);
        return Fact.terminator(ts);
    }
}

class ReorderCollapsedWithWatermarksFunction extends ReorderFunction {

    @Override
    protected boolean isOrderElement(Fact fact) {
        return fact.isMeta() && fact.getName().equals("WATERMARK");
    }

    @Override
    protected long indexExtractor(Fact fact) {
        if (isOrderElement(fact)) {
            return Long.parseLong((String) fact.getArgument(0));
        } else {
            return fact.getTimestamp();
        }
    }

    @Override
    protected Fact makeTerminator(long idx) {
        return Fact.terminator(idx);
    }
}

class ReorderCollapsedPerPartitionFunction extends ReorderFunction {

    @Override
    protected boolean isOrderElement(Fact fact) {
        return fact.isTerminator();
    }

    @Override
    protected long indexExtractor(Fact fact) {
        return fact.getTimestamp();
    }

    @Override
    protected Fact makeTerminator(long idx) {
        return Fact.terminator(idx);
    }
}
