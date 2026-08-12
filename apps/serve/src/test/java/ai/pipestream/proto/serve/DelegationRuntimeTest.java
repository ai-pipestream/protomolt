package ai.pipestream.proto.serve;

import ai.pipestream.proto.delegation.DelegationBridge;
import ai.pipestream.proto.delegation.DelegationReducer;
import ai.pipestream.proto.delegation.RepositoryServiceTranscriptRepository;
import ai.pipestream.proto.delegation.RepositoryStateKeyResolver;
import ai.pipestream.proto.delegation.v1.AcceptanceCheck;
import ai.pipestream.proto.delegation.v1.TaskSpec;
import ai.pipestream.proto.delegation.v1.WorkerCapability;
import ai.pipestream.proto.delegation.v1.WorkerHello;
import ai.pipestream.proto.repo.v1.DocumentServiceGrpc;
import ai.pipestream.proto.repo.v1.FileStorageReference;
import ai.pipestream.proto.repo.v1.GetBlobRequest;
import ai.pipestream.proto.repo.v1.GetBlobResponse;
import ai.pipestream.proto.repo.v1.PutBlobRequest;
import ai.pipestream.proto.repo.v1.PutBlobResponse;
import com.google.protobuf.ByteString;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The serve delegation wiring: in-memory by default, durable through the repository
 * service when an endpoint is configured. The durable path is exercised end to end
 * against an in-process fake repository service, including a full "restart" (close
 * the runtime, open it again over the same stored blob).
 */
class DelegationRuntimeTest {

    private static final String WORKER = "serve-worker";
    private static final byte[] KEY = "0123456789abcdef0123456789abcdef"
            .getBytes(StandardCharsets.US_ASCII);
    private static final RepositoryStateKeyResolver KEYS =
            ignored -> new SecretKeySpec(KEY, "AES");

    private FakeDocumentService documents;
    private Server repoServer;
    private String endpoint;

    @BeforeEach
    void startRepoService() throws Exception {
        documents = new FakeDocumentService();
        String name = InProcessServerBuilder.generateName();
        repoServer = InProcessServerBuilder.forName(name).directExecutor()
                .addService(documents).build().start();
        endpoint = DelegationRuntime.IN_PROCESS_PREFIX + name;
    }

    @AfterEach
    void stopRepoService() {
        repoServer.shutdownNow();
    }

    @Test
    void defaultWiringIsInMemoryAndKeepsNothingAcrossARestart() {
        String taskId = UUID.randomUUID().toString();
        try (DelegationRuntime runtime = DelegationRuntime.open(null, KEYS)) {
            DelegationBridge bridge = runtime.bridge();
            assertThat(bridge.registerWorker(hello()).admitted()).isTrue();
            bridge.offer(WORKER, taskId, spec(), Duration.ofMinutes(5), null);
            bridge.accept(WORKER, taskId, 1);
            assertThat(bridge.coordinator().state().tasks().get(taskId).phase())
                    .isEqualTo(DelegationReducer.Phase.LEASED);
        }
        assertThat(documents.lastPut).isNull();

        try (DelegationRuntime reopened = DelegationRuntime.open(null, KEYS)) {
            assertThat(reopened.bridge().coordinator().workers()).isEmpty();
            assertThat(reopened.bridge().coordinator().transcript().getEntriesCount()).isZero();
            assertThat(reopened.bridge().coordinator().state().tasks()).isEmpty();
        }
    }

    @Test
    void durableWiringRestoresTasksCursorsAndWorkerScopesAcrossARestart() {
        ProtoMoltServe.DelegationOptions options = options();
        String taskId = UUID.randomUUID().toString();
        long cursorBeforeRestart;
        int entriesBeforeRestart;
        try (DelegationRuntime runtime = DelegationRuntime.open(options, KEYS)) {
            DelegationBridge bridge = runtime.bridge();
            assertThat(bridge.registerWorker(hello()).admitted()).isTrue();
            bridge.offer(WORKER, taskId, spec(), Duration.ofMinutes(5), null);
            bridge.accept(WORKER, taskId, 1);
            assertThat(bridge.progress(WORKER, taskId, 1, "mapped the envelope")).isEqualTo(1);
            cursorBeforeRestart = bridge.coordinator().eventsAfter("", 0).getLast().cursor();
            entriesBeforeRestart = bridge.coordinator().transcript().getEntriesCount();
        }

        // The repository service saw only the encrypted envelope.
        assertThat(documents.lastPut).isNotNull();
        assertThat(documents.lastPut.getDriveName())
                .isEqualTo(ProtoMoltServe.DelegationOptions.DEFAULT_DRIVE);
        assertThat(documents.lastPut.getData().toStringUtf8())
                .doesNotContain("prove the bounded change");

        try (DelegationRuntime restored = DelegationRuntime.open(options, KEYS)) {
            DelegationBridge bridge = restored.bridge();
            assertThat(bridge.coordinator().transcript().getEntriesCount())
                    .isEqualTo(entriesBeforeRestart);
            assertThat(bridge.coordinator().state().tasks().get(taskId).phase())
                    .isEqualTo(DelegationReducer.Phase.LEASED);

            // The same worker re-registers and resumes its sequence scopes.
            assertThat(bridge.registerWorker(hello()).admitted()).isTrue();
            assertThat(bridge.progress(WORKER, taskId, 1, "wired the stream")).isEqualTo(2);

            // A watcher resuming from the pre-restart cursor sees exactly the tail.
            var tail = bridge.coordinator().eventsAfter("", cursorBeforeRestart);
            assertThat(tail).hasSize(3);
            assertThat(tail.get(0).cursor()).isEqualTo(cursorBeforeRestart + 1);
            assertThat(tail.get(0).entry().getWorkerFrame().hasHello()).isTrue();
            assertThat(tail.get(0).entry().getWorkerFrame().getSeq()).isEqualTo(2);

            assertThat(bridge.coordinator().state().clean())
                    .as(bridge.coordinator().state().findings().toString()).isTrue();
        }
    }

    @Test
    void parseLeavesDelegationInMemoryByDefault() {
        ProtoMoltServe.Options options = ProtoMoltServe.Options.parse(new String[]{});
        assertThat(options.delegation()).isNull();
    }

    @Test
    void parseBuildsDurableDelegationWithDefaultsFromTheEndpointAlone() {
        ProtoMoltServe.Options options = ProtoMoltServe.Options.parse(new String[]{
                "--delegation-repo-endpoint", "repo.example.test:9443"});
        assertThat(options.delegation()).isNotNull();
        assertThat(options.delegation().repoEndpoint()).isEqualTo("repo.example.test:9443");
        assertThat(options.delegation().repoTls()).isFalse();
        assertThat(options.delegation().drive())
                .isEqualTo(ProtoMoltServe.DelegationOptions.DEFAULT_DRIVE);
        assertThat(options.delegation().objectKey())
                .isEqualTo(ProtoMoltServe.DelegationOptions.DEFAULT_OBJECT_KEY);
        assertThat(options.delegation().keyReference())
                .isEqualTo(ProtoMoltServe.DelegationOptions.DEFAULT_KEY_REFERENCE);
    }

    @Test
    void parseReadsEveryDelegationFlag() {
        ProtoMoltServe.Options options = ProtoMoltServe.Options.parse(new String[]{
                "--delegation-repo-endpoint", "repo.example.test:9443",
                "--delegation-repo-tls", "true",
                "--delegation-repo-drive", "state",
                "--delegation-transcript-object", "delegation/a/transcript.pb.enc",
                "--delegation-state-key-ref", "env:ALT_KEY"});
        assertThat(options.delegation().repoTls()).isTrue();
        assertThat(options.delegation().drive()).isEqualTo("state");
        assertThat(options.delegation().objectKey())
                .isEqualTo("delegation/a/transcript.pb.enc");
        assertThat(options.delegation().keyReference()).isEqualTo("env:ALT_KEY");
    }

    @Test
    void delegationOptionsRejectBlankCoordinates() {
        assertThatThrownBy(() -> new ProtoMoltServe.DelegationOptions(" ", false,
                "protomolt", "k", "env:X"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("endpoint");
        assertThatThrownBy(() -> new ProtoMoltServe.DelegationOptions("repo:443", false,
                "protomolt", " ", "env:X"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("object key");
    }

    @Test
    void unreachableRepositoryFailsWithinTheConfiguredDeadline() {
        documents.hangReads = true;

        long started = System.nanoTime();
        assertThatThrownBy(() -> DelegationRuntime.open(options(), KEYS,
                Duration.ofMillis(50), DelegationRuntime::channel))
                .isInstanceOf(StatusRuntimeException.class)
                .extracting(error -> ((StatusRuntimeException) error).getStatus().getCode())
                .isEqualTo(Status.Code.DEADLINE_EXCEEDED);
        assertThat(Duration.ofNanos(System.nanoTime() - started))
                .isLessThan(Duration.ofSeconds(5));
    }

    @Test
    void startupFailureClosesTheRepositoryChannel() throws InterruptedException {
        AtomicReference<io.grpc.ManagedChannel> opened = new AtomicReference<>();

        assertThatThrownBy(() -> DelegationRuntime.open(
                new ProtoMoltServe.DelegationOptions(endpoint, false,
                        ProtoMoltServe.DelegationOptions.DEFAULT_DRIVE,
                        ProtoMoltServe.DelegationOptions.DEFAULT_OBJECT_KEY,
                        "not-a-key-reference"),
                KEYS, Duration.ofSeconds(1), ignored -> {
                    io.grpc.ManagedChannel channel =
                            io.grpc.inprocess.InProcessChannelBuilder
                                    .forName(endpoint.substring(
                                            DelegationRuntime.IN_PROCESS_PREFIX.length()))
                                    .build();
                    opened.set(channel);
                    return channel;
                }))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("configuration is invalid");

        assertThat(opened.get()).isNotNull();
        assertThat(opened.get().isShutdown()).isTrue();
        assertThat(opened.get().awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    }

    private ProtoMoltServe.DelegationOptions options() {
        return new ProtoMoltServe.DelegationOptions(endpoint, false,
                ProtoMoltServe.DelegationOptions.DEFAULT_DRIVE,
                ProtoMoltServe.DelegationOptions.DEFAULT_OBJECT_KEY,
                ProtoMoltServe.DelegationOptions.DEFAULT_KEY_REFERENCE);
    }

    private static WorkerHello hello() {
        return WorkerHello.newBuilder()
                .setWorkerId(WORKER)
                .setProtocolVersion(1)
                .setProvider("kimi")
                .addCapabilities(WorkerCapability.newBuilder().setName("java-build"))
                .build();
    }

    private static TaskSpec spec() {
        return TaskSpec.newBuilder()
                .setObjective("prove the bounded change")
                .addRequiredChecks(AcceptanceCheck.newBuilder()
                        .setName("unit-tests")
                        .setDescription("the module tests pass"))
                .build();
    }

    private static final class FakeDocumentService
            extends DocumentServiceGrpc.DocumentServiceImplBase {
        private ByteString stored;
        private PutBlobRequest lastPut;
        private boolean hangReads;

        @Override
        public void putBlob(PutBlobRequest request,
                            StreamObserver<PutBlobResponse> observer) {
            lastPut = request;
            stored = request.getData();
            observer.onNext(PutBlobResponse.newBuilder()
                    .setStorageRef(FileStorageReference.newBuilder()
                            .setDriveName(request.getDriveName())
                            .setObjectKey(request.getObjectKey()))
                    .setSizeBytes(stored.size())
                    .setSha256(sha256(stored))
                    .build());
            observer.onCompleted();
        }

        @Override
        public void getBlob(GetBlobRequest request,
                            StreamObserver<GetBlobResponse> observer) {
            if (hangReads) {
                return;
            }
            if (stored == null) {
                observer.onError(Status.NOT_FOUND.asRuntimeException());
                return;
            }
            observer.onNext(GetBlobResponse.newBuilder()
                    .setData(stored)
                    .setSizeBytes(stored.size())
                    .setMimeType(RepositoryServiceTranscriptRepository.MIME_TYPE)
                    .build());
            observer.onCompleted();
        }

        private static String sha256(ByteString data) {
            try {
                return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                        .digest(data.toByteArray()));
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException(e);
            }
        }
    }
}
