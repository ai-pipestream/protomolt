package ai.pipestream.proto.mesh.cluster;

import ai.pipestream.proto.mesh.cluster.v1.ClusterDescriptor;
import ai.pipestream.proto.mesh.cluster.v1.ClusterEvent;

import java.util.List;
import java.util.Optional;

/** Stores complete, validated event logs for restartable cluster directories. */
public interface ClusterEventRepository {

    /** Loads the event log for the exact cluster identity, when one exists. */
    Optional<List<ClusterEvent>> load(ClusterDescriptor cluster);

    /** Atomically replaces the event log for the exact cluster identity. */
    void save(ClusterDescriptor cluster, List<ClusterEvent> events);
}
