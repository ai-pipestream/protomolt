package ai.pipestream.proto.mesh.cluster;

import ai.pipestream.proto.mesh.cluster.v1.ClusterDescriptor;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Thread-safe process-local cluster event repository for tests and ephemeral deployments. */
public final class InMemoryClusterEventRepository implements ClusterEventRepository {

    private final Map<String, StoredDirectory> logs = new HashMap<>();

    @Override
    public synchronized Optional<StoredDirectory> load(ClusterDescriptor cluster) {
        ClusterValidation.validate(cluster);
        return Optional.ofNullable(logs.get(cluster.getFingerprint()));
    }

    @Override
    public synchronized void save(ClusterDescriptor cluster, StoredDirectory directory) {
        ClusterValidation.validate(cluster);
        ClusterValidation.validateEventLog(directory.events(), directory.firstSeq());
        logs.put(cluster.getFingerprint(), directory);
    }
}
