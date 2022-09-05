package standalone;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.contrib.streaming.state.*;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.LocalRecoveryDirectoryProviderImpl;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.taskexecutor.KvStateService;
import org.rocksdb.DBOptions;

import java.io.File;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;

public class StandaloneStateBackendUtil {
    private static final KvStateService KV_STATE_SERVICE = new KvStateService(new KvStateRegistry(), null, null);

    public static RocksDBKeyedStateBackend<String> createStateBackend(Configuration configuration,
                                                                      ExecutionConfig executionConfig,
                                                                      String runnerId,
                                                                      String taskId,
                                                                      CloseableRegistry closeableRegistry) throws Exception {
        final RocksDBResourceContainer optionsContainer = new RocksDBResourceContainer();
        final CloseableRegistry backendCloseRegistry = new CloseableRegistry();
        final String tmpdir = configuration.get("tmp.dir", String.class);
        final Method ensureRocksDBIsLoaded = RocksDBStateBackend.class.getDeclaredMethod("ensureRocksDBIsLoaded", String.class);
        ensureRocksDBIsLoaded.setAccessible(true);
        ensureRocksDBIsLoaded.invoke(null, tmpdir);
        closeableRegistry.registerCloseable(backendCloseRegistry);
        RocksDBKeyedStateBackendBuilder<String> stringRocksDBKeyedStateBackendBuilder = new RocksDBKeyedStateBackendBuilder<>(
                runnerId,
                ClassLoader.getSystemClassLoader(),
                new File(tmpdir + File.separator + taskId),
                optionsContainer,
                stateName -> optionsContainer.getColumnOptions(),
                KV_STATE_SERVICE.createKvStateTaskRegistry(JobID.fromHexString(runnerId), JobVertexID.fromHexString(taskId)),
                StringSerializer.INSTANCE,
                1,
                new KeyGroupRange(0, 0),
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
                backendCloseRegistry
        );
        return stringRocksDBKeyedStateBackendBuilder.build();

    }

}
