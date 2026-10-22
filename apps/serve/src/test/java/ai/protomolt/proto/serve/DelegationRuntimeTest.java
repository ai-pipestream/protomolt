package ai.protomolt.proto.serve;

import ai.protomolt.proto.delegation.DelegationBridge;
import ai.protomolt.proto.delegation.lifecycle.DelegationReducer;
import ai.protomolt.proto.delegation.repository.RepositoryServiceTranscriptRepository;
import ai.protomolt.proto.delegation.repository.RepositoryStateKeyResolver;
import ai.protomolt.proto.delegation.v1.AcceptanceCheck;
import ai.protomolt.proto.delegation.v1.TaskSpec;
import ai.protomolt.proto.delegation.v1.WorkerCapability;
import ai.protomolt.proto.delegation.v1.WorkerHello;
import ai.protomolt.proto.repo.v2.Accession;
import ai.protomolt.proto.repo.v2.DocumentManifest;
import ai.protomolt.proto.repo.v2.DocumentPart;
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v2.GetDocumentRequest;
import ai.protomolt.proto.repo.v2.GetDocumentResponse;
import ai.protomolt.proto.repo.v2.IntakeOrigin;
import ai.protomolt.proto.repo.v2.PartManifestEntry;
import ai.protomolt.proto.repo.v2.PartSelector;
import ai.protomolt.proto.repo.v2.PartState;
import ai.protomolt.proto.repo.v2.ProcessAuthorityEvidence;
import ai.protomolt.proto.repo.v2.RevisionConflict;
import ai.protomolt.proto.repo.v2.RevisionValidation;
import ai.protomolt.proto.repo.v2.SaveDocumentRequest;
import ai.protomolt.proto.repo.v2.SaveDocumentResponse;
import ai.protomolt.proto.repo.v2.SaveOwner;
import ai.protomolt.proto.repo.v2.ValidationStatus;
import com.google.protobuf.Any;
import com.google.protobuf.Timestamp;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
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
        assertThat(documents.lastSave).isNull();

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
        assertThat(documents.lastSave).isNotNull();
        assertThat(documents.lastSave.getTarget().getAddress().getAccountId())
                .isEqualTo(ProtoMoltServe.DelegationOptions.DEFAULT_DRIVE);
        assertThat(documents.lastSave.getDocument().getStructuredData().getValue().toStringUtf8())
                .doesNotContain("prove the bounded change");

        try (DelegationRuntime restored = DelegationRuntime.open(options, KEYS)) {
            DelegationBridge bridge = restored.bridge();
            assertThat(bridge.coordinator().transcript().getEntriesCount())
                    .isEqualTo(entriesBeforeRestart);
            assertThat(bridge.coordinator().state().tasks().get(taskId).phase())
                    .isEqualTo(DelegationReducer.Phase.LEASED);
            assertThat(bridge.coordinator().workers()).singleElement().satisfies(worker -> {
                assertThat(worker.workerId()).isEqualTo(WORKER);
                assertThat(worker.admitted()).isTrue();
                assertThat(worker.connected()).isFalse();
            });

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
        private volatile ai.protomolt.proto.repo.v1.Document stored;
        private volatile long revision;
        private volatile SaveDocumentRequest lastSave;
        private volatile boolean hangReads;

        private static final Timestamp COMMITTED_AT =
                Timestamp.newBuilder().setSeconds(1786449600L).build();
        private static final SaveOwner OWNER = SaveOwner.newBuilder()
                .setIntake(IntakeOrigin.newBuilder().setDatasourceId("serve-test")).build();
        private static final RevisionValidation VALIDATION = RevisionValidation.newBuilder()
                .setStatus(ValidationStatus.VALIDATION_STATUS_TYPED_VALID)
                .setCandidateSha256("0".repeat(64))
                .setValidatorProfile("serve-test")
                .setProcessAuthority(ProcessAuthorityEvidence.newBuilder().setPrincipal("serve-test"))
                .build();

        @Override
        public synchronized void getDocument(GetDocumentRequest request,
                                             StreamObserver<GetDocumentResponse> observer) {
            if (hangReads) {
                return;
            }
            if (stored == null) {
                observer.onError(Status.NOT_FOUND.asRuntimeException());
                return;
            }
            observer.onNext(GetDocumentResponse.newBuilder().setDocument(stored)
                    .setManifest(manifest(request.getAddress(), revision))
                    .setValidation(VALIDATION).build());
            observer.onCompleted();
        }

        @Override
        public synchronized void saveDocument(SaveDocumentRequest request,
                                              StreamObserver<SaveDocumentResponse> observer) {
            lastSave = request;
            long expected = request.getExpected().hasNone() ? 0 : request.getExpected().getRevision();
            if (stored == null ? expected != 0 : revision != expected) {
                var detail = RevisionConflict.newBuilder().setAddress(request.getTarget().getAddress())
                        .setExpected(request.getExpected()).setCurrentRevision(revision).build();
                observer.onError(StatusProto.toStatusRuntimeException(com.google.rpc.Status.newBuilder()
                        .setCode(Status.Code.ABORTED.value()).setMessage("conditional write conflict")
                        .addDetails(Any.pack(detail)).build()));
                return;
            }
            stored = request.getDocument();
            revision++;
            observer.onNext(SaveDocumentResponse.newBuilder().setOperationId(request.getOperationId())
                    .setAccession(Accession.newBuilder().setAddress(request.getTarget().getAddress())
                            .setRevision(revision).setPreviousRevision(expected)
                            .setCommittedAt(COMMITTED_AT)
                            .setManifest(manifest(request.getTarget().getAddress(), revision))
                            .setValidation(VALIDATION)
                            .setOwner(OWNER))
                    .build());
            observer.onCompleted();
        }

        private static DocumentManifest manifest(
                ai.protomolt.proto.repo.v1.NodeAddress address, long revision) {
            return DocumentManifest.newBuilder().setAddress(address).setRevision(revision)
                    .setCommittedAt(COMMITTED_AT)
                    .addParts(PartManifestEntry.newBuilder()
                            .setSelector(PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE))
                            .setState(PartState.PART_STATE_PRESENT).setWrittenInRevision(revision)
                            .setOwner(OWNER).setWrittenAt(COMMITTED_AT))
                    .build();
        }
    }
}
