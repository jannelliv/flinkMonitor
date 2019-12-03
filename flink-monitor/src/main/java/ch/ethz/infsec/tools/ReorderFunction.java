package ch.ethz.infsec.tools;

import ch.ethz.infsec.StreamMonitoring;
import ch.ethz.infsec.kafka.MonitorKafkaConfig;
import ch.ethz.infsec.monitor.Fact;
import ch.ethz.infsec.slicer.HypercubeSlicer;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ReferenceMap;
import it.unimi.dsi.fastutil.longs.Long2ReferenceOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;
import scala.Int;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;


@ForwardedFields({"1"})
public abstract class ReorderFunction extends RichFlatMapFunction<Tuple2<Int, Fact>, Fact> implements CheckpointedFunction {
    protected int numSources = MonitorKafkaConfig.getNumPartitions();
    private Long2ReferenceMap<ReferenceArrayList<Fact>> idx2Facts = new Long2ReferenceOpenHashMap<>();
    private long[] maxOrderElem = new long[numSources];
    private long currentIdx = -2;
    private long maxIdx = -1;
    private int numEOF = 0;
    private Integer ownSubtaskIdx = null;
    private Integer numMonitors = null;

    private transient ListState<Tuple2<Long, ReferenceArrayList<Fact>>> idx2facts_state = null;
    private transient ListState<Long> maxorderelem_state = null;
    private transient ListState<Long> indices_state = null;
    private String newStategy = null;

    abstract protected boolean isOrderElement(Fact fact);

    abstract protected long indexExtractor(Fact fact);

    abstract protected Fact makeTerminator(long idx);

    ReorderFunction() {
        Arrays.fill(maxOrderElem, -1);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        idx2facts_state.clear();
        maxorderelem_state.clear();
        indices_state.clear();
        ownSubtaskIdx = getRuntimeContext().getIndexOfThisSubtask();
        numMonitors = getRuntimeContext().getNumberOfParallelSubtasks();
        if (ownSubtaskIdx == 0) {
            for (long l: maxOrderElem)
                maxorderelem_state.add(l);
            indices_state.add(currentIdx);
            indices_state.add(maxIdx);
            indices_state.add((long) numEOF);
        }
        if (newStategy == null) {
            for (Long2ReferenceMap.Entry<ReferenceArrayList<Fact>> k: idx2Facts.long2ReferenceEntrySet()) {
                idx2facts_state.add(new Tuple2<>(k.getLongKey(), k.getValue()));
            }
        } else {
            HypercubeSlicer slicer = null;
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext ctxt) throws Exception {
        ListStateDescriptor<Tuple2<Long, ReferenceArrayList<Fact>>> idx2facts_state_desc =
                new ListStateDescriptor<>(
                        "idx2facts_state",
                        TypeInformation.of(new TypeHint<Tuple2<Long, ReferenceArrayList<Fact>>>() {
                        })
                );

        ListStateDescriptor<Long> maxorderelem_state_desc =
                new ListStateDescriptor<>(
                        "maxorderelem_state",
                        TypeInformation.of(new TypeHint<Long>() {
                        })
                );
        ListStateDescriptor<Long> indices_state_desc =
                new ListStateDescriptor<>(
                        "indices_state",
                        TypeInformation.of(new TypeHint<Long>() {
                        })
                );
        idx2facts_state = ctxt.getOperatorStateStore().getListState(idx2facts_state_desc);
        maxorderelem_state = ctxt.getOperatorStateStore().getUnionListState(maxorderelem_state_desc);
        indices_state = ctxt.getOperatorStateStore().getUnionListState(indices_state_desc);
        if (ctxt.isRestored()) {
            for (Tuple2<Long, ReferenceArrayList<Fact>> k: idx2facts_state.get())
                idx2Facts.put((long) k._1, k._2);
            ArrayList<Long> tmp = new ArrayList<>();
            for (Long l: maxorderelem_state.get())
                tmp.add(l);
            maxOrderElem = new long[tmp.size()];
            for (int i = 0; i < tmp.size(); ++i)
                maxOrderElem[i] = tmp.get(i);
            ArrayList<Long> indices = new ArrayList<>();
            for (Long l: indices_state.get())
                indices.add(l);
            /*if (indices.size() != 3)
                throw new Exception("invariant: 3 saved indices");*/
            currentIdx = indices.get(0);
            maxIdx = indices.get(1);
            numEOF = Math.toIntExact(indices.get(2));
            newStategy = null;
            numSources = MonitorKafkaConfig.getNumPartitions();
        }
    }

    private void insertElement(Fact fact, long idx) {
        ReferenceArrayList<Fact> buf = idx2Facts.get(idx);
        if (buf == null) {
            ReferenceArrayList<Fact> nl = new ReferenceArrayList<>();
            nl.add(fact);
            idx2Facts.put(idx, nl);
        } else {
            buf.add(fact);
        }
    }

    private void forceFlush(Collector<Fact> out) {
        if (maxIdx != -1) {
            for (long idx = currentIdx + 1; idx <= maxIdx; ++idx) {
                ReferenceArrayList<Fact> arr;
                if ((arr = idx2Facts.get(idx)) != null) {
                    for (Fact fact : arr)
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
        for (long l : maxOrderElem) {
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
                for (Fact fact : arr)
                    out.collect(fact);
            }
            out.collect(makeTerminator(idx));
        }

        currentIdx = maxAgreedIdx;

    }

    @Override
    public void flatMap(Tuple2<Int, Fact> value, Collector<Fact> out) {
        int subtaskidx = (Integer) ((Object) value._1);
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
                if ((++numEOF) == numSources)
                    forceFlush(out);
                return;
            } else if (fact.getName().equals("START"))
                return;
            else
                throw new RuntimeException("should not happen");
        }
        long idx = indexExtractor(fact);
        if (idx > maxIdx) maxIdx = idx;
        /*if (idx < currentIdx)
            throw new RuntimeException("FATAL ERROR: Got a timestamp that should already be flushed");*/
        insertElement(fact, idx);
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
