package ai.protomolt.proto.serve;

import ai.protomolt.proto.delegation.EnvRepositoryStateKeyResolver;
import ai.protomolt.proto.mesh.cluster.ClusterEventRepository;
import ai.protomolt.proto.mesh.cluster.ClusterValidation;
import ai.protomolt.proto.mesh.cluster.InMemoryClusterEventRepository;
import ai.protomolt.proto.mesh.cluster.PersistentClusterDirectory;
import ai.protomolt.proto.mesh.cluster.RepositoryServiceClusterEventRepository;
import ai.protomolt.proto.mesh.cluster.v1.ClusterDescriptor;
import ai.protomolt.proto.repo.v1.DocumentServiceGrpc;
import com.google.protobuf.Timestamp;
import io.grpc.ManagedChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Serve-owned mesh directory runtime. A configured delegation repository supplies the same
 * encrypted repository boundary for cluster events. Without that repository the directory is
 * intentionally memory-only, which is useful for tests but requires nodes to re-register after
 * restart.
 */
final class MeshClusterRuntime implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(MeshClusterRuntime.class);
    private static final long SWEEP_INTERVAL_SECONDS = 30;

    private final PersistentClusterDirectory directory;
    private final ManagedChannel repoChannel;
    private final ScheduledExecutorService sweeper;

    private MeshClusterRuntime(PersistentClusterDirectory directory,
                               ManagedChannel repoChannel,
                               ScheduledExecutorService sweeper) {
        this.directory = directory;
        this.repoChannel = repoChannel;
        this.sweeper = sweeper;
    }

    /** Opens the configured directory, or returns null when the mesh surface is disabled. */
    static MeshClusterRuntime open(ProtoMoltServe.MeshClusterOptions mesh,
                                   ProtoMoltServe.DelegationOptions repository) {
        if (mesh == null) {
            return null;
        }
        ClusterDescriptor unsigned = ClusterDescriptor.newBuilder()
                .setClusterId(mesh.clusterId())
                .setDisplayName(mesh.displayName())
                .setTrustDomain(mesh.trustDomain())
                .addProtocolRevisions(1)
                .setCreatedAt(Timestamp.newBuilder()
                        .setSeconds(mesh.createdAt().getEpochSecond())
                        .setNanos(mesh.createdAt().getNano()))
                .build();
        ClusterDescriptor cluster = unsigned.toBuilder()
                .setFingerprint(ClusterValidation.descriptorFingerprint(unsigned))
                .build();

        if (repository == null) {
            return runtime(new PersistentClusterDirectory(cluster, Clock.systemUTC(),
                    new InMemoryClusterEventRepository()), null);
        }

        ManagedChannel channel = DelegationRuntime.channel(repository);
        try {
            ClusterEventRepository events = new RepositoryServiceClusterEventRepository(
                    DocumentServiceGrpc.newBlockingStub(channel), repository.drive(),
                    "mesh/" + mesh.clusterId() + "/events.pb.enc",
                    repository.keyReference(), new EnvRepositoryStateKeyResolver());
            return runtime(new PersistentClusterDirectory(cluster, Clock.systemUTC(), events),
                    channel);
        } catch (RuntimeException | Error e) {
            channel.shutdownNow();
            throw e;
        }
    }

    private static MeshClusterRuntime runtime(PersistentClusterDirectory directory,
                                              ManagedChannel channel) {
        ScheduledExecutorService sweeper = Executors.newSingleThreadScheduledExecutor(
                Thread.ofVirtual().name("protomolt-mesh-sweep-", 0).factory());
        sweeper.scheduleWithFixedDelay(() -> {
            try {
                directory.sweep();
            } catch (RuntimeException e) {
                LOG.warn("Mesh expiry sweep failed; existing directory state remains visible", e);
            }
        }, SWEEP_INTERVAL_SECONDS, SWEEP_INTERVAL_SECONDS, TimeUnit.SECONDS);
        return new MeshClusterRuntime(directory, channel, sweeper);
    }

    /** Directory shared by every catalog action in this process. */
    PersistentClusterDirectory directory() {
        return directory;
    }

    @Override
    public void close() {
        sweeper.shutdownNow();
        if (repoChannel != null) {
            repoChannel.shutdownNow();
        }
    }
}
