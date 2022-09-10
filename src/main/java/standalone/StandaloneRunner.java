package standalone;

import cep.EventComparator;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import cep.nfa.NFA;
import cep.nfa.NFAState;
import cep.nfa.NFAStateSerializer;
import cep.nfa.aftermatch.AfterMatchSkipStrategy;
import cep.nfa.compiler.NFACompiler;
import cep.nfa.sharedbuffer.SharedBuffer;
import cep.nfa.sharedbuffer.SharedBufferAccessor;
import cep.pattern.Pattern;
import cep.time.TimerService;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.state.DefaultKeyedStateStore;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.util.IOUtils;
import standalone.source.Source;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Stream;

public class StandaloneRunner<OUT> extends Thread implements WatermarkListener {
    private static final String NFA_STATE_NAME = "nfaStateName";
    private static final String EVENT_QUEUE_STATE_NAME = "eventQueuesStateName";
    private final String runnerId;
    private final String taskId;
    private final KeyedStateBackend<String> stateBackend;
    private final KeyedStateStore keyedStateStore;
    private final TypeSerializer<JSONObject> inputSerializer;
    private final EventTimeService timerService;
    private final TimerService cepTimerService;
    private final EventComparator<JSONObject> comparator = null;
    private final MatchFunction<JSONObject, OUT> function;
    private ValueState<NFAState> computationStates;
    private MapState<Long, List<JSONObject>> elementQueueState;
    private SharedBuffer<JSONObject> partialMatches;
    private final NFA<JSONObject> nfa;
    private final BlockingQueue<TimestampedElement<JSONObject>> receiveBuffer;
    private final List<Source> sources = new ArrayList<>();
    private Thread processingThread;
    private final CloseableRegistry closeableRegistry;
    private final AfterMatchSkipStrategy afterMatchSkipStrategy;
    private long lastWatermark;

    public StandaloneRunner(Configuration configuration,
                            NFA<JSONObject>nfa,
                            AfterMatchSkipStrategy afterMatchSkipStrategy,
                            MatchFunction<JSONObject, OUT> function,
                            ExecutionConfig executionConfig,
                            Source ...sources) throws Exception {
        this.runnerId = JobID.generate().toHexString();
        this.taskId = JobID.generate().toHexString();
        this.nfa = nfa;
        this.afterMatchSkipStrategy = afterMatchSkipStrategy;
        this.function = function;
        this.timerService = new EventTimeService();
        this.cepTimerService = System::currentTimeMillis;
        this.closeableRegistry = new CloseableRegistry();
        this.inputSerializer = TypeInformation.of(JSONObject.class).createSerializer(executionConfig);
        this.receiveBuffer = new ArrayBlockingQueue<>(10000);
        this.stateBackend = StandaloneStateBackendUtil.createStateBackend(configuration, executionConfig, runnerId, taskId, closeableRegistry);
        this.keyedStateStore = new DefaultKeyedStateStore(
                stateBackend,
                new ExecutionConfig());
        this.sources.addAll(Arrays.asList(sources));

    }



    public static <O> StandaloneRunner<O> create(Pattern<JSONObject, JSONObject> pattern,
                                                 MatchFunction<JSONObject, O> function,
                                                 Source ...sources) throws Exception {
        NFACompiler.NFAFactory<JSONObject> nfaFactory = NFACompiler.compileFactory(pattern, false);
        return new StandaloneRunner<>(
                Configuration.load(),
                nfaFactory.createNFA(),
                pattern.getAfterMatchSkipStrategy(),
                function,
                new ExecutionConfig(),
                sources);
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
        timerService.addListener(this);
        for (Source source : sources) {
            source.run(this);
        }
    }

    @Override
    public void run() {
        this.processingThread = Thread.currentThread();
        try {
            init();
            while (!Thread.interrupted()) {
                final TimestampedElement<JSONObject> element = receiveBuffer.take();
                processElement(element);
            }
        } catch (Throwable t) {
            if (!(t instanceof InterruptedException)) {
                t.printStackTrace();
            }
            shutdown();
        }
    }

    public void shutdown() {
        sources.forEach(Source::shutdown);
        if (processingThread != null) {
            processingThread.interrupt();
        }
        IOUtils.closeAllQuietly(closeableRegistry);

    }

    public void sendElement(TimestampedElement<JSONObject> element) throws InterruptedException {
        receiveBuffer.put(element);
    }

    public void processElement(TimestampedElement<JSONObject> element) throws Exception {
            stateBackend.setCurrentKey("default");
            long timestamp = element.getTimestamp();
            JSONObject value = element.getElement();

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

    private void bufferEvent(JSONObject event, long currentTime) throws Exception {
        List<JSONObject> elementsForTimestamp = elementQueueState.get(currentTime);
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
            try (Stream<JSONObject> elements = sort(elementQueueState.get(timestamp))) {
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

    private Stream<JSONObject> sort(Collection<JSONObject> elements) {
        Stream<JSONObject> stream = elements.stream();
        return (comparator == null) ? stream : stream.sorted(comparator);
    }

    private void processEvent(NFAState nfaState, JSONObject event, long timestamp) throws Exception {
        try (SharedBufferAccessor<JSONObject> sharedBufferAccessor = partialMatches.getAccessor()) {
            Collection<Map<String, List<JSONObject>>> patterns =
                    nfa.process(
                            sharedBufferAccessor,
                            nfaState,
                            event,
                            timestamp,
                            afterMatchSkipStrategy,
                            cepTimerService);
            processMatchedSequences(patterns, timestamp);
        }
    }

    private void advanceTime(NFAState nfaState, long timestamp) throws Exception {
        try (SharedBufferAccessor<JSONObject> sharedBufferAccessor = partialMatches.getAccessor()) {
            Collection<Tuple2<Map<String, List<JSONObject>>, Long>> timedOut =
                    nfa.advanceTime(sharedBufferAccessor, nfaState, timestamp);
            if (!timedOut.isEmpty()) {
                //processTimedOutSequences(timedOut);
            }
        }
    }

    private void processMatchedSequences(
            Iterable<Map<String, List<JSONObject>>> matchingSequences, long timestamp) throws Exception {
        //PatternProcessFunction<IN, OUT> function = f();
        //setTimestamp(timestamp);
        for (Map<String, List<JSONObject>> matchingSequence : matchingSequences) {
            OUT out = function.processMatch(matchingSequence);
            System.out.println(out);
        }
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

}
