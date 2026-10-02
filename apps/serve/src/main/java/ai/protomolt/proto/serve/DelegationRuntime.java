package ai.protomolt.proto.serve;

import ai.protomolt.proto.delegation.AdmissionPolicy;
import ai.protomolt.proto.delegation.CandidateReviewer;
import ai.protomolt.proto.delegation.DelegationBridge;
import ai.protomolt.proto.delegation.repository.EnvRepositoryStateKeyResolver;
import ai.protomolt.proto.delegation.InProcessDelegationCoordinator;
import ai.protomolt.proto.delegation.lifecycle.InMemoryTranscriptRepository;
import ai.protomolt.proto.delegation.repository.RepositoryServiceTranscriptRepository;
import ai.protomolt.proto.delegation.repository.RepositoryStateKeyResolver;
import ai.protomolt.proto.delegation.lifecycle.TranscriptRepository;
import ai.protomolt.proto.repo.v1.DocumentServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.inprocess.InProcessChannelBuilder;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Function;

/**
 * The serve-process delegation runtime: one bridge over one coordinator. With no
 * delegation repository configured the coordinator keeps its transcript in memory
 * (development mode: a restart loses it and workers re-register as new). With a
 * repository-service endpoint configured the transcript persists through
 * {@link RepositoryServiceTranscriptRepository}, so a restart restores every task,
 * cursor, and sequence scope and a re-registering worker resumes where the record
 * left off.
 */
final class DelegationRuntime implements AutoCloseable {

    /** Endpoint prefix selecting an in-process repository service (tests, all-in-one). */
    static final String IN_PROCESS_PREFIX = "in-process:";

    private final DelegationBridge bridge;
    private final ManagedChannel repoChannel;
    private final TranscriptRepository transcripts;

    private DelegationRuntime(DelegationBridge bridge, ManagedChannel repoChannel,
            TranscriptRepository transcripts) {
        this.bridge = bridge;
        this.repoChannel = repoChannel;
        this.transcripts = transcripts;
    }

    /** Opens the runtime with encryption keys resolved from the process environment. */
    static DelegationRuntime open(ProtoMoltServe.DelegationOptions options) {
        return open(options, new EnvRepositoryStateKeyResolver());
    }

    /** Opens with a host-selected reviewer; ordinary serve keeps manual review. */
    static DelegationRuntime open(ProtoMoltServe.DelegationOptions options,
            CandidateReviewer reviewer) {
        return open(options, new EnvRepositoryStateKeyResolver(),
                RepositoryServiceTranscriptRepository.DEFAULT_RPC_TIMEOUT,
                DelegationRuntime::channel, reviewer);
    }

    /**
     * Opens the runtime from the launcher options. Null options select the in-memory
     * store. A configured store loads any recorded transcript before the first frame,
     * so a repository service that is down or a key that does not resolve fails
     * startup instead of silently running memory-only.
     */
    static DelegationRuntime open(ProtoMoltServe.DelegationOptions options,
                                  RepositoryStateKeyResolver keys) {
        return open(options, keys,
                RepositoryServiceTranscriptRepository.DEFAULT_RPC_TIMEOUT,
                DelegationRuntime::channel, CandidateReviewer.manual());
    }

    /** Package-private seam for bounded-deadline and channel-cleanup tests. */
    static DelegationRuntime open(ProtoMoltServe.DelegationOptions options,
                                  RepositoryStateKeyResolver keys, Duration rpcTimeout,
                                  Function<ProtoMoltServe.DelegationOptions,
                                          ManagedChannel> channels) {
        return open(options, keys, rpcTimeout, channels, CandidateReviewer.manual());
    }

    static DelegationRuntime open(ProtoMoltServe.DelegationOptions options,
                                  RepositoryStateKeyResolver keys, Duration rpcTimeout,
                                  Function<ProtoMoltServe.DelegationOptions,
                                          ManagedChannel> channels, CandidateReviewer reviewer) {
        Objects.requireNonNull(reviewer, "reviewer");
        if (options == null) {
            TranscriptRepository transcripts = new InMemoryTranscriptRepository();
            var coordinator = new InProcessDelegationCoordinator(
                    AdmissionPolicy.allowAll(), reviewer, Clock.systemUTC(), transcripts);
            return new DelegationRuntime(new DelegationBridge(coordinator), null, transcripts);
        }
        Objects.requireNonNull(keys, "keys");
        Objects.requireNonNull(rpcTimeout, "rpcTimeout");
        Objects.requireNonNull(channels, "channels");
        ManagedChannel channel = channels.apply(options);
        try {
            TranscriptRepository transcripts = new RepositoryServiceTranscriptRepository(
                    DocumentServiceGrpc.newBlockingStub(channel), options.drive(),
                    options.objectKey(), options.keyReference(), keys, rpcTimeout);
            InProcessDelegationCoordinator coordinator = new InProcessDelegationCoordinator(
                    AdmissionPolicy.allowAll(), reviewer, Clock.systemUTC(),
                    transcripts);
            return new DelegationRuntime(new DelegationBridge(coordinator), channel, transcripts);
        } catch (RuntimeException | Error e) {
            channel.shutdownNow();
            throw e;
        }
    }

    /** The live bridge the catalog verbs and MCP resources mount. */
    DelegationBridge bridge() {
        return bridge;
    }

    /** The coordinator's own repository, shared with the accepted-workflow launcher. */
    TranscriptRepository transcripts() {
        return transcripts;
    }

    static ManagedChannel channel(ProtoMoltServe.DelegationOptions options) {
        String endpoint = options.repoEndpoint();
        if (endpoint.startsWith(IN_PROCESS_PREFIX)) {
            String name = endpoint.substring(IN_PROCESS_PREFIX.length());
            if (name.isBlank()) {
                throw new IllegalArgumentException(
                        "the delegation repo endpoint names no in-process server");
            }
            return InProcessChannelBuilder.forName(name).build();
        }
        ManagedChannelBuilder<?> builder = ManagedChannelBuilder.forTarget(endpoint);
        if (options.repoTls()) {
            builder.useTransportSecurity();
        } else {
            builder.usePlaintext();
        }
        return builder.build();
    }

    /** Worker streams close first, then the coordinator, then the repository channel. */
    @Override
    public void close() {
        bridge.close();
        bridge.coordinator().close();
        if (repoChannel != null) {
            repoChannel.shutdownNow();
        }
    }
}
