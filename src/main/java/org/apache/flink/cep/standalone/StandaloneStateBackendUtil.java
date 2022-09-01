package org.apache.flink.cep.standalone;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeinfo.Types;
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
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.taskexecutor.KvStateService;

import java.io.File;
import java.util.Collections;

public class StandaloneStateBackendUtil {
    private static final KvStateService KV_STATE_SERVICE = new KvStateService(new KvStateRegistry(), null, null);

    public static RocksDBKeyedStateBackend<String> createStateBackend(String runnerId, String taskId) throws BackendBuildingException {
        final RocksDBResourceContainer optionsContainer = new RocksDBResourceContainer();
        return new RocksDBKeyedStateBackendBuilder<String>(
                runnerId,
                ClassLoader.getSystemClassLoader(),
                new File("/tmp"),
                optionsContainer,
                stateName -> optionsContainer.getColumnOptions(),
                KV_STATE_SERVICE.createKvStateTaskRegistry(JobID.fromHexString(runnerId), JobVertexID.fromHexString(taskId)),
                StringSerializer.INSTANCE,
                1,
                new KeyGroupRange(0,0),
                new ExecutionConfig(),
                new LocalRecoveryConfig(false,
                        new LocalRecoveryDirectoryProviderImpl(
                                new File("/tmp/localState"),
                                JobID.fromHexString(runnerId),
                                JobVertexID.fromHexString(taskId),
                                0)),
                RocksDBStateBackend.PriorityQueueStateType.ROCKSDB,
                TtlTimeProvider.DEFAULT,
                new UnregisteredMetricsGroup(),
                Collections.emptyList(),
                UncompressedStreamCompressionDecorator.INSTANCE,
                new CloseableRegistry()
        ).build();

    }

}
