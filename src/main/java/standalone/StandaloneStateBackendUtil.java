package standalone;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackendBuilder;
import org.apache.flink.contrib.streaming.state.RocksDBResourceContainer;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackend;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackendBuilder;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.taskexecutor.KvStateService;

import javax.annotation.Nonnull;
import java.io.File;
import java.util.Collections;

public class StandaloneStateBackendUtil {
    private static final KvStateService KV_STATE_SERVICE = new KvStateService(new KvStateRegistry(),
            null, null);

    public static KeyedStateBackend<String> createStateBackend(@Nonnull Configuration configuration,
                                                               ExecutionConfig executionConfig,
                                                               String runnerId,
                                                               String taskId,
                                                               CloseableRegistry closeableRegistry) throws Exception {
        final CloseableRegistry backendCloseRegistry = new CloseableRegistry();
        closeableRegistry.registerCloseable(backendCloseRegistry);
        if (LocalStateStorage.ROCKSDB == getLocalStateStorage(configuration)) {
            return createRocksdbStateBackend(configuration.get(Options.TMP_DIR), executionConfig,
                    runnerId, taskId, 1, KeyGroupRange.of(0, 0), backendCloseRegistry);
        } else {
            return createHeapStateBackend(configuration.get(Options.TMP_DIR), executionConfig,
                    runnerId, taskId, 1, KeyGroupRange.of(0, 0), backendCloseRegistry);
        }


    }

    private static LocalStateStorage getLocalStateStorage(Configuration configuration) {
        return configuration.get(Options.LOCAL_STATE_STORAGE);
    }

    private static RocksDBKeyedStateBackend<String> createRocksdbStateBackend(String tmpdir,
                                                                      ExecutionConfig executionConfig,
                                                                      String runnerId,
                                                                      String taskId,
                                                                      int keyGroupNumbers,
                                                                      KeyGroupRange keyGroupRange,
                                                                      CloseableRegistry closeableRegistry) throws Exception {
        final RocksDBResourceContainer optionsContainer = new RocksDBResourceContainer();

        return new RocksDBKeyedStateBackendBuilder<>(
                runnerId,
                ClassLoader.getSystemClassLoader(),
                new File(tmpdir + File.separator + taskId),
                optionsContainer,
                stateName -> optionsContainer.getColumnOptions(),
                KV_STATE_SERVICE.createKvStateTaskRegistry(JobID.fromHexString(runnerId), JobVertexID.fromHexString(taskId)),
                StringSerializer.INSTANCE,
                keyGroupNumbers,
                keyGroupRange,
                executionConfig,
                new LocalRecoveryConfig(false,
                        new LocalRecoveryDirectoryProviderImpl(
                                new File(tmpdir + "/localState/" + taskId),
                                JobID.fromHexString(runnerId),
                                JobVertexID.fromHexString(taskId),
                                0)),
                RocksDBStateBackend.PriorityQueueStateType.ROCKSDB,
                TtlTimeProvider.DEFAULT,
                new UnregisteredMetricsGroup(),
                Collections.emptyList(),
                UncompressedStreamCompressionDecorator.INSTANCE,
                closeableRegistry
        ).build();
    }

    private static HeapKeyedStateBackend<String> createHeapStateBackend(
            String tmpDir,
            ExecutionConfig executionConfig,
            String runnerId,
            String taskId,
            int keyGroupNumbers,
            KeyGroupRange keyGroupRange,
            CloseableRegistry closeableRegistry) throws Exception {

        HeapPriorityQueueSetFactory priorityQueueSetFactory =
                new HeapPriorityQueueSetFactory(keyGroupRange, 1, 128);
        return new HeapKeyedStateBackendBuilder<>(
                KV_STATE_SERVICE.createKvStateTaskRegistry(JobID.fromHexString(runnerId), JobVertexID.fromHexString(taskId)),
                StringSerializer.INSTANCE,
                ClassLoader.getSystemClassLoader(),
                keyGroupNumbers,
                keyGroupRange,
                executionConfig,
                TtlTimeProvider.DEFAULT,
                Collections.emptyList(),
                UncompressedStreamCompressionDecorator.INSTANCE,
                new LocalRecoveryConfig(false,
                        new LocalRecoveryDirectoryProviderImpl(
                                new File(tmpDir + "/localState/" + taskId),
                                JobID.fromHexString(runnerId),
                                JobVertexID.fromHexString(taskId),
                                0)),
                priorityQueueSetFactory,
                true,
                closeableRegistry)
                .build();

    }

}
