package org.apache.flink.cep.standalone;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.EventComparator;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.NFAState;
import org.apache.flink.cep.nfa.NFAStateSerializer;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBuffer;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBufferAccessor;
import org.apache.flink.cep.time.TimerService;
import org.apache.flink.runtime.state.BackendBuildingException;
import org.apache.flink.runtime.state.DefaultKeyedStateStore;
import org.apache.flink.runtime.state.VoidNamespace;

import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;

public class StandaloneRunner<IN> implements WatermarkListener {
    private static final String NFA_STATE_NAME = "nfaStateName";
    private static final String EVENT_QUEUE_STATE_NAME = "eventQueuesStateName";
    private final String runnerId;
    private final String taskId;
    private final KeyedStateStore keyedStateStore;
    private final TypeSerializer<IN> inputSerializer;
    private final EventTimeService timerService = new EventTimeService();
    private final TimerService cepTimerService = new TimerServiceImpl();
    private final EventComparator<IN> comparator = null;
    private ValueState<NFAState> computationStates;
    private MapState<Long, List<IN>> elementQueueState;
    private SharedBuffer<IN> partialMatches;
    private long lastWatermark;
    private NFA<IN> nfa;

    public StandaloneRunner(TypeSerializer<IN> inputSerializer) throws BackendBuildingException {
        this.runnerId = JobID.generate().toHexString();
        this.taskId = JobID.generate().toHexString();
        this.inputSerializer = inputSerializer;
        this.keyedStateStore = new DefaultKeyedStateStore(StandaloneStateBackendUtil.createStateBackend(runnerId, taskId), new ExecutionConfig());
    }

    public void init() throws Exception {
        computationStates =
                keyedStateStore
                        .getState(
                                new ValueStateDescriptor<>(
                                        NFA_STATE_NAME, new NFAStateSerializer()));

        partialMatches = new SharedBuffer<>(keyedStateStore, inputSerializer);

        elementQueueState =
                keyedStateStore.getMapState(
                                new MapStateDescriptor<>(
                                        EVENT_QUEUE_STATE_NAME,
                                        LongSerializer.INSTANCE,
                                        new ListSerializer<>(inputSerializer)));
    }

    public void processElement(TimestampedElement<IN> element) throws Exception {


            long timestamp = element.getTimestamp();
            IN value = element.getElement();

            // In event-time processing we assume correctness of the watermark.
            // Events with timestamp smaller than or equal with the last seen watermark are
            // considered late.
            // Late events are put in a dedicated side output, if the user has specified one.

            if (timestamp > timerService.currentWatermark) {
                //saveRegisterWatermarkTimer();
                bufferEvent(value, timestamp);
                timerService.tryAdvanceWatermark(element);
            }

    }

    private void bufferEvent(IN event, long currentTime) throws Exception {
        List<IN> elementsForTimestamp = elementQueueState.get(currentTime);
        if (elementsForTimestamp == null) {
            elementsForTimestamp = new ArrayList<>();
        }

        elementsForTimestamp.add(event);
        elementQueueState.put(currentTime, elementsForTimestamp);
    }

//    private void saveRegisterWatermarkTimer() {
//        long currentWatermark = timerService.currentWatermark();
//        // protect against overflow
//        if (currentWatermark + 1 > currentWatermark) {
//            timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, currentWatermark + 1);
//        }
//    }

    @Override
    public void onWatermark(long watermark) throws Exception {

        PriorityQueue<Long> sortedTimestamps = getSortedTimestamps();
        NFAState nfaState = getNFAState();

        // STEP 2
        while (!sortedTimestamps.isEmpty()
                && sortedTimestamps.peek() <= timerService.currentWatermark()) {
            long timestamp = sortedTimestamps.poll();
            advanceTime(nfaState, timestamp);
            try (Stream<IN> elements = sort(elementQueueState.get(timestamp))) {
                elements.forEachOrdered(
                        event -> {
                            try {
                                processEvent(nfaState, event, timestamp);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
            }
            elementQueueState.remove(timestamp);
        }

        // STEP 3
        advanceTime(nfaState, timerService.currentWatermark());

        // STEP 4
        updateNFA(nfaState);

//        if (!sortedTimestamps.isEmpty() || !partialMatches.isEmpty()) {
//            saveRegisterWatermarkTimer();
//        }

        // STEP 5
        updateLastSeenWatermark(timerService.currentWatermark());
    }

    private void updateLastSeenWatermark(long timestamp) {
        this.lastWatermark = timestamp;
    }

    private Stream<IN> sort(Collection<IN> elements) {
        Stream<IN> stream = elements.stream();
        return (comparator == null) ? stream : stream.sorted(comparator);
    }

    private void processEvent(NFAState nfaState, IN event, long timestamp) throws Exception {
        try (SharedBufferAccessor<IN> sharedBufferAccessor = partialMatches.getAccessor()) {
            Collection<Map<String, List<IN>>> patterns =
                    nfa.process(
                            sharedBufferAccessor,
                            nfaState,
                            event,
                            timestamp,
                            AfterMatchSkipStrategy.noSkip(),
                            cepTimerService);
            processMatchedSequences(patterns, timestamp);
        }
    }

    private void advanceTime(NFAState nfaState, long timestamp) throws Exception {
        try (SharedBufferAccessor<IN> sharedBufferAccessor = partialMatches.getAccessor()) {
            Collection<Tuple2<Map<String, List<IN>>, Long>> timedOut =
                    nfa.advanceTime(sharedBufferAccessor, nfaState, timestamp);
            if (!timedOut.isEmpty()) {
                //processTimedOutSequences(timedOut);
            }
        }
    }

    private void processMatchedSequences(
            Iterable<Map<String, List<IN>>> matchingSequences, long timestamp) throws Exception {
//        PatternProcessFunction<IN, OUT> function = getUserFunction();
//        setTimestamp(timestamp);
//        for (Map<String, List<IN>> matchingSequence : matchingSequences) {
//            function.processMatch(matchingSequence, context, collector);
//        }
    }

    private NFAState getNFAState() throws IOException {
        NFAState nfaState = computationStates.value();
        return nfaState != null ? nfaState : nfa.createInitialNFAState();
    }

    private void updateNFA(NFAState nfaState) throws IOException {
        if (nfaState.isStateChanged()) {
            nfaState.resetStateChanged();
            computationStates.update(nfaState);
        }
    }
    private PriorityQueue<Long> getSortedTimestamps() throws Exception {
        PriorityQueue<Long> sortedTimestamps = new PriorityQueue<>();
        for (Long timestamp : elementQueueState.keys()) {
            sortedTimestamps.offer(timestamp);
        }
        return sortedTimestamps;
    }

    private class TimerServiceImpl implements TimerService {

        @Override
        public long currentProcessingTime() {
            return System.currentTimeMillis();
        }
    }

}
