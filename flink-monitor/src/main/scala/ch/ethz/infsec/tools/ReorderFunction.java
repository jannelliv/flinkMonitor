package ch.ethz.infsec.tools;

import ch.ethz.infsec.monitor.Fact;
import ch.ethz.infsec.slicer.HypercubeSlicer;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ReferenceMap;
import it.unimi.dsi.fastutil.longs.Long2ReferenceOpenHashMap;
import it.unimi.dsi.fastutil.objects.ReferenceArrayList;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.util.ListCollector;
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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

public abstract class ReorderFunction extends RichFlatMapFunction<Tuple2<Int, Fact>, Fact> implements CheckpointedFunction, Serializable {
    private Long2ReferenceMap<ReferenceArrayList<Fact>> idx2Facts;
    private Long2ReferenceMap<ReferenceArrayList<Fact>> terminatorLatencyMarkers;
    protected int numSources;
    private long[] maxOrderElem;
    private long currentIdx;
    private int numEOF;
    private int numNewStrategy;
    private long maxLatencyIdx;

    private transient ListState<Long2ReferenceMap<ReferenceArrayList<Fact>>> idx2facts_state = null;
    private transient ListState<Long> maxorderelem_state = null;
    private transient ListState<Long> indices_state = null;
    private HypercubeSlicer slicer;

    abstract protected boolean isOrderElement(Fact fact);

    abstract protected long indexExtractor(Fact fact);

    abstract protected Fact makeTerminator(long idx);

    ReorderFunction(int numSources, HypercubeSlicer slicer) {
        //FIXME: should be the first ts
        numNewStrategy = 0;
        maxLatencyIdx = 0;
        currentIdx = -2;
        numEOF = 0;
        this.slicer = slicer;
        this.numSources = numSources;
        terminatorLatencyMarkers = new Long2ReferenceOpenHashMap<>();
        idx2Facts = new Long2ReferenceOpenHashMap<>();
        maxOrderElem = new long[numSources];
        Arrays.fill(maxOrderElem, -1);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        idx2facts_state.clear();
        maxorderelem_state.clear();
        indices_state.clear();
        int ownSubtaskIdx = getRuntimeContext().getIndexOfThisSubtask();
        int numMonitors = getRuntimeContext().getNumberOfParallelSubtasks();
        if (ownSubtaskIdx == 0) {
            for (long l: maxOrderElem)
                maxorderelem_state.add(l);
        }
        indices_state.add(currentIdx);
        indices_state.add((long) numEOF);
        /*if (newStategy == null) {
            for (int i = 0; i < numMonitors; ++i) {
                if (i == ownSubtaskIdx)
                    idx2facts_state.add(idx2Facts);
                else
                    idx2facts_state.add(new Long2ReferenceOpenHashMap<>());
            }
        } else {*/
            ArrayList<Long2ReferenceOpenHashMap<ReferenceArrayList<Fact>>> state = new ArrayList<>();
            ReferenceArrayList<Tuple2<Object, Fact>> list = new ReferenceArrayList<>();
            ListCollector<Tuple2<Object, Fact>> collector = new ListCollector<>(list);
            int degree = slicer.degree();
            for (int i = 0; i < degree; ++i)
                state.add(new Long2ReferenceOpenHashMap<>());
            for (Long2ReferenceMap.Entry<ReferenceArrayList<Fact>> e: idx2Facts.long2ReferenceEntrySet()) {
                for (Fact f: e.getValue()) {
                    list.clear();
                    slicer.processEvent(f, collector);
                    for (Tuple2<Object, Fact> tup: list) {
                        Long2ReferenceMap<ReferenceArrayList<Fact>> map = state.get((Integer) tup._1);
                        ReferenceArrayList<Fact> sublist;
                        if ((sublist = map.get(e.getLongKey())) == null) {
                            sublist = new ReferenceArrayList<>();
                            sublist.add(tup._2);
                            map.put(e.getLongKey(), sublist);
                        } else {
                            sublist.add(tup._2);
                        }
                    }
                }
            }
            for (Long2ReferenceOpenHashMap<ReferenceArrayList<Fact>> m: state)
                idx2facts_state.add(m);
        //}
        /*System.out.println("LOL snapshot: subtask " + ownSubtaskIdx +
                "\nmaxOrderElem: " + Arrays.toString(maxOrderElem) +
                "\ncurr_idx = " + currentIdx + ", numEOF = " + numEOF +
                "\nno elem in idx2Facts: " + noElems(idx2Facts));*/
    }

    @Override
    public void initializeState(FunctionInitializationContext ctxt) throws Exception {
        ListStateDescriptor<Long2ReferenceMap<ReferenceArrayList<Fact>>> idx2facts_state_desc =
                new ListStateDescriptor<>(
                        "idx2facts_state",
                        TypeInformation.of(new TypeHint<Long2ReferenceMap<ReferenceArrayList<Fact>>>() {
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
        indices_state = ctxt.getOperatorStateStore().getListState(indices_state_desc);
        if (ctxt.isRestored()) {
            for (Long2ReferenceMap<ReferenceArrayList<Fact>> m: idx2facts_state.get()) {
                for(Long2ReferenceMap.Entry<ReferenceArrayList<Fact>> k: m.long2ReferenceEntrySet()) {
                    long key = k.getLongKey();
                    ReferenceArrayList<Fact> l;
                    if ((l = idx2Facts.get(key)) == null)
                        idx2Facts.put(key, k.getValue());
                    else
                        l.addAll(k.getValue());
                }
            }
            ArrayList<Long> tmp = new ArrayList<>();
            for (Long l: maxorderelem_state.get())
                tmp.add(l);
            maxOrderElem = new long[tmp.size()];
            for (int i = 0; i < tmp.size(); ++i)
                maxOrderElem[i] = tmp.get(i);
            ArrayList<Long> indices = new ArrayList<>();
            for (Long l: indices_state.get())
                indices.add(l);
            if (indices.size() != 2)
                throw new Exception(String.format("INVARIANT: expected 2 saved indices, got %d", indices.size()));
            currentIdx = indices.get(0);
            numEOF = Math.toIntExact(indices.get(1));
            //newStategy = null;
            /*System.out.println("LOL restore: subtask " + getRuntimeContext().getIndexOfThisSubtask() +
                    "\nmaxOrderElem: " + Arrays.toString(maxOrderElem) +
                    "\ncurr_idx = " + currentIdx + ", numEOF = " + numEOF +
                    "\nno elem in idx2Facts: " + noElems(idx2Facts));*/
        }
    }

    private int noElems(Long2ReferenceMap<ReferenceArrayList<Fact>> dings) {
        int sum = 0;
        for (ReferenceArrayList<Fact> bla : dings.values()) {
            sum += bla.size();
        }
        return sum;
    }

    private void insertElement(Fact fact, long idx) {
        if (idx < currentIdx)
            throw new RuntimeException("INVARIANT: idx >= currentIdx");
        ReferenceArrayList<Fact> buf;
        if ((buf = idx2Facts.get(idx)) == null) {
            buf = new ReferenceArrayList<>(Collections.singletonList(fact));
            idx2Facts.put(idx, buf);
        } else {
            buf.add(fact);
        }
    }

    private long getMaxAgreedIdx() {
        long min = Long.MAX_VALUE;
        for (long l : maxOrderElem) {
            if (l < min)
                min = l;
        }
        return min;
    }

    private long getMaxOrderElem() {
        long max = -1;
        for (long l : maxOrderElem) {
            if (l > max)
                max = l;
        }
        return max;
    }

    private void flushReady(Collector<Fact> out) {
        long maxAgreedIdx = getMaxAgreedIdx();
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
            if((arr = terminatorLatencyMarkers.get(idx)) != null) {
                terminatorLatencyMarkers.remove(idx);
                for (Fact fact : arr)
                    out.collect(fact);
            }
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
            //System.out.println("LOL: got order elem " + idx);
            if (idx > maxOrderElem[subtaskidx])
                maxOrderElem[subtaskidx] = idx;
            flushReady(out);
            return;
        }

        if (fact.isMeta()) {
            switch (fact.getName()) {
                case "EOF":
                    if ((++numEOF) == numSources) {
                        if (!(terminatorLatencyMarkers.isEmpty() && idx2Facts.isEmpty())) {
                            System.out.println("terminatorLatencyMarkers is " + terminatorLatencyMarkers);
                            System.out.println("idx2Facts is " +  idx2Facts);
                            throw new RuntimeException("INVARIANT: terminatorLatencyMarkers and idx2Facts should be empty after receiving all EOFS");
                        }
                    }
                    out.collect(fact);
                    return;
                case "set_slicer":
                    if (++numNewStrategy == numSources) {
                        numNewStrategy = 0;
                        slicer.unstringify((String) fact.getArgument(0));
                        out.collect(fact);
                    }
                    if (numNewStrategy > numSources)
                        throw new RuntimeException("INVARIANT: numNewStrategy <= numSources");
                    return;
                case "LATENCY":
                    long maxAgreed = getMaxAgreedIdx();
                    if (maxAgreed >= maxLatencyIdx || maxLatencyIdx == -1) {
                        if (!idx2Facts.isEmpty())
                            throw new RuntimeException("INVARIANT: idx2Facts.isEmpty()");
                        out.collect(fact);
                    } else {
                        if (idx2Facts.get(maxLatencyIdx) == null)
                            throw new RuntimeException("INVARIANT: idx2Facts.get(maxLatencyIdx) != null");
                        insertElement(fact, maxLatencyIdx);
                        //System.out.println(String.format("lol: for subtask %d got latency marker from subtask %d, inserting it at index %d, currentidx: %d", getRuntimeContext().getIndexOfThisSubtask(), subtaskidx, maxLatencyIdx, currentIdx));
                    }
                    return;
                case "START":
                    return;
                default:
                    out.collect(fact);
                    return;
            }
        }
        long idx = indexExtractor(fact);
        if (idx > maxLatencyIdx)
            maxLatencyIdx = idx;
        insertElement(fact, idx);
    }
}

class ReorderTotalOrderFunction extends ReorderFunction {
    private Long2LongMap tpTotsMap = new Long2LongOpenHashMap();

    ReorderTotalOrderFunction(int numSources, HypercubeSlicer slicer) {
        super(numSources, slicer);
    }

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

    ReorderCollapsedWithWatermarksFunction(int numSources, HypercubeSlicer slicer) {
        super(numSources, slicer);
    }

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
    ReorderCollapsedPerPartitionFunction(int numSources, HypercubeSlicer slicer) {
        super(numSources, slicer);
    }

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