package ai.protomolt.proto.serve;

import ai.protomolt.proto.delegation.EnvRepositoryStateKeyResolver;
import ai.protomolt.proto.descriptors.DescriptorRegistry;
import ai.protomolt.proto.mesh.cluster.ClusterDirectoryGrpcService;
import ai.protomolt.proto.mesh.cluster.ClusterEventRepository;
import ai.protomolt.proto.mesh.cluster.ClusterValidation;
import ai.protomolt.proto.mesh.cluster.InMemoryClusterEventRepository;
import ai.protomolt.proto.mesh.cluster.PersistentClusterDirectory;
import ai.protomolt.proto.mesh.cluster.RepositoryServiceClusterEventRepository;
import ai.protomolt.proto.mesh.cluster.v1.ClusterDescriptor;
import ai.protomolt.proto.mesh.runtime.ChannelResourceCatalog;
import ai.protomolt.proto.mesh.runtime.DemandProcessorCoordinator;
import ai.protomolt.proto.mesh.runtime.DirectoryProcessorResolver;
import ai.protomolt.proto.mesh.runtime.DirectoryWorkerAdmission;
import ai.protomolt.proto.mesh.runtime.DurableFlowCoordinator;
import ai.protomolt.proto.mesh.runtime.DurableProcessorChannel;
import ai.protomolt.proto.mesh.runtime.FileDurableProcessorChannel;
import ai.protomolt.proto.mesh.runtime.FileFlowLifecycleStore;
import ai.protomolt.proto.mesh.runtime.FlowLifecycleGrpcService;
import ai.protomolt.proto.mesh.runtime.PayloadLifecycle;
import ai.protomolt.proto.mesh.runtime.MeshRuntimeHealthGrpcService;
import ai.protomolt.proto.mesh.runtime.PayloadRecoveryReconciler;
import ai.protomolt.proto.mesh.runtime.PayloadResolver;
import ai.protomolt.proto.mesh.runtime.PayloadStore;
import ai.protomolt.proto.mesh.runtime.PayloadStoreGrpcService;
import ai.protomolt.proto.mesh.runtime.PayloadStoreResolver;
import ai.protomolt.proto.mesh.runtime.PolicyRoutedProcessorChannel;
import ai.protomolt.proto.mesh.runtime.PersistentDirectoryWorkerControl;
import ai.protomolt.proto.mesh.runtime.ProcessorDirectoryClient;
import ai.protomolt.proto.mesh.runtime.ProcessorRegistry;
import ai.protomolt.proto.mesh.runtime.RecoveryGrpcService;
import ai.protomolt.proto.mesh.runtime.RemoteProcessorInvoker;
import ai.protomolt.proto.mesh.runtime.RepositoryPayloadStore;
import ai.protomolt.proto.mesh.runtime.WorkerCapacityController;
import ai.protomolt.proto.mesh.runtime.WorkerSessionRegistry;
import ai.protomolt.proto.repo.v1.DocumentServiceGrpc;
import com.google.protobuf.Timestamp;
import io.grpc.BindableService;
import io.grpc.ManagedChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** Serve-owned durable mesh directory, workflow, channel, payload, and recovery runtime. */
final class MeshClusterRuntime implements AutoCloseable {

    static final String PAYLOAD_PROFILE = "repository-payload-v1";

    private static final Logger LOG = LoggerFactory.getLogger(MeshClusterRuntime.class);

    private final PersistentClusterDirectory directory;
    private final DurableProcessorChannel channel;
    private final FileFlowLifecycleStore lifecycleStore;
    private final DemandProcessorCoordinator workers;
    private final PayloadStore payloadStore;
    private final ManagedChannel repoChannel;
    private final ScheduledExecutorService sweeper;
    private final List<BindableService> services;

    private MeshClusterRuntime(
            PersistentClusterDirectory directory,
            DurableProcessorChannel channel,
            FileFlowLifecycleStore lifecycleStore,
            DemandProcessorCoordinator workers,
            PayloadStore payloadStore,
            ManagedChannel repoChannel,
            ScheduledExecutorService sweeper,
            List<BindableService> services) {
        this.directory = directory;
        this.channel = channel;
        this.lifecycleStore = lifecycleStore;
        this.workers = workers;
        this.payloadStore = payloadStore;
        this.repoChannel = repoChannel;
        this.sweeper = sweeper;
        this.services = List.copyOf(services);
    }

    /** Opens the configured runtime, or returns null when the mesh surface is disabled. */
    static MeshClusterRuntime open(
            ProtoMoltServe.MeshClusterOptions mesh,
            ProtoMoltServe.DelegationOptions repository,
            DescriptorRegistry descriptors) {
        if (mesh == null) {
            return null;
        }
        Clock clock = Clock.systemUTC();
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

        ManagedChannel repoChannel = null;
        PayloadStore payloadStore = null;
        DurableProcessorChannel channel = null;
        FileFlowLifecycleStore lifecycleStore = null;
        DemandProcessorCoordinator workers = null;
        ScheduledExecutorService sweeper = null;
        try {
            Files.createDirectories(mesh.stateDirectory());
            ClusterEventRepository events;
            DocumentServiceGrpc.DocumentServiceBlockingStub repositoryStub = null;
            if (repository == null) {
                events = new InMemoryClusterEventRepository();
            } else {
                repoChannel = DelegationRuntime.channel(repository);
                repositoryStub = DocumentServiceGrpc.newBlockingStub(repoChannel);
                events = new RepositoryServiceClusterEventRepository(
                        repositoryStub, repository.drive(),
                        "mesh/" + mesh.clusterId() + "/events.pb.enc",
                        repository.keyReference(), new EnvRepositoryStateKeyResolver());
            }
            PersistentClusterDirectory directory = new PersistentClusterDirectory(
                    cluster, clock, events);
            FileDurableProcessorChannel localWal = new FileDurableProcessorChannel(
                    mesh.stateDirectory().resolve("processor-channel.wal"), descriptors, clock);
            channel = new PolicyRoutedProcessorChannel(
                    descriptors, localWal, java.util.Map.of(), clock);
            lifecycleStore = new FileFlowLifecycleStore(
                    mesh.stateDirectory().resolve("flow-lifecycle.wal"));

            if (repositoryStub != null) {
                payloadStore = new RepositoryPayloadStore(
                        repositoryStub, repository.drive(),
                        "mesh/" + mesh.clusterId() + "/payloads",
                        mesh.stateDirectory().resolve("payload-ledger.wal"), clock);
            }
            PayloadResolver payloadResolver = payloadStore == null
                    ? PayloadResolver.inlineOnly(descriptors)
                    : new PayloadStoreResolver(descriptors, payloadStore,
                            envelope -> envelope.getHeader().getScopeId(), PAYLOAD_PROFILE);
            PayloadLifecycle payloadLifecycle = payloadStore == null
                    ? PayloadLifecycle.inlineOnly()
                    : PayloadLifecycle.stored(payloadStore, clock);
            ChannelResourceCatalog resources = new ChannelResourceCatalog(
                    payloadStore == null ? Set.of() : Set.of(PAYLOAD_PROFILE),
                    Set.of(), Set.of("default-retry"), Set.of("default-dead-letter"),
                    Set.of(), Set.of());

            ProcessorRegistry processors = new ProcessorRegistry(descriptors);
            AtomicReference<DemandProcessorCoordinator> workerReference =
                    new AtomicReference<>();
            DurableProcessorChannel sharedChannel = channel;
            var view = (java.util.function.Supplier<ProcessorDirectoryClient.View>) () ->
                    ProcessorDirectoryClient.View.from(
                            directory.snapshot(), directory.generation());
            DirectoryProcessorResolver resolver = new DirectoryProcessorResolver(view, clock);
            DirectoryWorkerAdmission admission = new DirectoryWorkerAdmission(
                    view, clock, contract -> processors.registerOrVerify(
                            new RemoteProcessorInvoker(contract, sharedChannel,
                                    () -> workerReference.get().workAvailable(),
                                    clock, mesh.maxAttempts())));
            workers = new DemandProcessorCoordinator(
                    descriptors, channel, admission, resolver,
                    new PersistentDirectoryWorkerControl(directory, clock),
                    new WorkerSessionRegistry(),
                    new WorkerCapacityController(mesh.maxDemand()),
                    clock, mesh.workerLeaseDuration());
            workerReference.set(workers);

            DurableFlowCoordinator flows = new DurableFlowCoordinator(
                    descriptors, processors, lifecycleStore, payloadResolver,
                    payloadLifecycle, resources, clock);
            List<BindableService> services = new ArrayList<>();
            services.add(new ClusterDirectoryGrpcService(directory));
            services.add(workers);
            services.add(new FlowLifecycleGrpcService(flows));
            RecoveryGrpcService.RecoveryReconciler reconciler;
            RecoveryGrpcService.DeadLetterPayloadControl payloadControl;
            if (payloadStore == null) {
                reconciler = RecoveryGrpcService.RecoveryReconciler.reportOnly();
                payloadControl = RecoveryGrpcService.DeadLetterPayloadControl.none();
            } else {
                PayloadRecoveryReconciler payloadRecovery = new PayloadRecoveryReconciler(
                        channel, java.util.Map.of(PAYLOAD_PROFILE, payloadStore));
                reconciler = payloadRecovery;
                payloadControl = payloadRecovery;
            }
            services.add(new RecoveryGrpcService(
                    channel, flows, reconciler, payloadControl, clock));
            services.add(new MeshRuntimeHealthGrpcService(
                    directory, workers, channel,
                    MeshRuntimeHealthGrpcService.WatchHealth::localView, clock));
            if (payloadStore != null) {
                services.add(new PayloadStoreGrpcService(payloadStore, clock));
            }

            sweeper = Executors.newSingleThreadScheduledExecutor(
                    Thread.ofVirtual().name("protomolt-mesh-sweep-", 0).factory());
            long sweepMillis = mesh.sweepInterval().toMillis();
            sweeper.scheduleWithFixedDelay(() -> {
                try {
                    directory.sweep();
                } catch (RuntimeException e) {
                    LOG.warn("Mesh expiry sweep failed; existing directory state remains visible",
                            e);
                }
            }, sweepMillis, sweepMillis, TimeUnit.MILLISECONDS);
            return new MeshClusterRuntime(directory, channel, lifecycleStore, workers,
                    payloadStore, repoChannel, sweeper, services);
        } catch (IOException e) {
            closeFailed(sweeper, workers, lifecycleStore, channel, payloadStore, repoChannel);
            throw new UncheckedIOException("cannot open mesh state at "
                    + mesh.stateDirectory(), e);
        } catch (RuntimeException | Error e) {
            closeFailed(sweeper, workers, lifecycleStore, channel, payloadStore, repoChannel);
            throw e;
        }
    }

    private static void closeFailed(
            ScheduledExecutorService sweeper,
            DemandProcessorCoordinator workers,
            FileFlowLifecycleStore lifecycleStore,
            DurableProcessorChannel channel,
            PayloadStore payloadStore,
            ManagedChannel repoChannel) {
        if (sweeper != null) {
            sweeper.shutdownNow();
        }
        closeQuietly(workers);
        closeQuietly(lifecycleStore);
        closeQuietly(channel);
        closeQuietly(payloadStore);
        if (repoChannel != null) {
            repoChannel.shutdownNow();
        }
    }

    private static void closeQuietly(Object resource) {
        if (resource instanceof AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception failure) {
                LOG.warn("Failed to close partial mesh runtime", failure);
            }
        }
    }

    /** Directory shared by every catalog action in this process. */
    PersistentClusterDirectory directory() {
        return directory;
    }

    /** Services mounted by the selected mesh server role. */
    List<BindableService> grpcServices() {
        return services;
    }

    @Override
    public void close() {
        closeFailed(sweeper, workers, lifecycleStore, channel, payloadStore, repoChannel);
    }
}
