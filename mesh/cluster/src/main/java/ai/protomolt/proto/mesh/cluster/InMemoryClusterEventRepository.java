package ai.protomolt.proto.mesh.cluster;

import ai.protomolt.proto.mesh.cluster.v1.ClusterDescriptor;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;

/** Thread-safe process-local cluster event repository for tests and ephemeral deployments. */
public final class InMemoryClusterEventRepository implements ClusterEventRepository {

    private final ReentrantLock lock = new ReentrantLock();
    private final Map<String, StoredDirectory> logs = new HashMap<>();

    @Override
    public Optional<StoredDirectory> load(ClusterDescriptor cluster) {
        lock.lock();
        try {
            ClusterValidation.validate(cluster);
            return Optional.ofNullable(logs.get(cluster.getFingerprint()));
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void save(ClusterDescriptor cluster, StoredDirectory directory) {
        lock.lock();
        try {
            ClusterValidation.validate(cluster);
            ClusterValidation.validateEventLog(directory.events(), directory.firstSeq());
            logs.put(cluster.getFingerprint(), directory);
        } finally {
            lock.unlock();
        }
    }
}
