package ai.pipestream.proto.mesh.cluster;

import ai.pipestream.proto.mesh.cluster.v1.ClusterDescriptor;
import ai.pipestream.proto.mesh.cluster.v1.ClusterEvent;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Thread-safe process-local cluster event repository for tests and ephemeral deployments. */
public final class InMemoryClusterEventRepository implements ClusterEventRepository {

    private final Map<String, List<ClusterEvent>> logs = new HashMap<>();

    @Override
    public synchronized Optional<List<ClusterEvent>> load(ClusterDescriptor cluster) {
        ClusterValidation.validate(cluster);
        return Optional.ofNullable(logs.get(cluster.getFingerprint()));
    }

    @Override
    public synchronized void save(ClusterDescriptor cluster, List<ClusterEvent> events) {
        ClusterValidation.validate(cluster);
        ClusterValidation.validateEventLog(events);
        logs.put(cluster.getFingerprint(), List.copyOf(events));
    }
}
