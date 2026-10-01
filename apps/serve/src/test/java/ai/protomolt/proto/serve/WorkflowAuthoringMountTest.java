package ai.protomolt.proto.serve;

import ai.protomolt.proto.actions.ActionContext;
import ai.protomolt.proto.delegation.CandidateReviewer;
import ai.protomolt.proto.delegation.DelegationBridge;
import ai.protomolt.proto.delegation.DelegationReducer;
import ai.protomolt.proto.delegation.RepositoryStateKeyResolver;
import ai.protomolt.proto.grpc.workflow.ArtifactRepository;
import ai.protomolt.proto.grpc.workflow.FileSystemArtifactRepository;
import ai.protomolt.proto.grpc.workflow.FileSystemRunEvidenceRepository;
import ai.protomolt.proto.grpc.workflow.RunEvidenceRepository;
import ai.protomolt.proto.grpc.workflow.WorkflowVersionRepository;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.jobs.service.store.WorkflowRunStore;
import ai.protomolt.proto.receipt.*;
import ai.protomolt.proto.samples.starter.v1.WorkflowAcceptanceFixture;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringDeliverable;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringPolicy;
import ai.protomolt.proto.samples.starter.v1.WorkflowPermittedCall;
import ai.protomolt.proto.workflow.WorkflowRunner;
import ai.protomolt.proto.workflow.authoring.WorkflowAuthoringReviewer;
import ai.protomolt.proto.delegation.v1.AcceptanceCheck;
import ai.protomolt.proto.delegation.v1.CheckEvidence;
import ai.protomolt.proto.delegation.v1.CheckVerdict;
import ai.protomolt.proto.delegation.v1.CompletionCandidate;
import ai.protomolt.proto.delegation.v1.DeliverableContract;
import ai.protomolt.proto.delegation.v1.TaskSpec;
import ai.protomolt.proto.delegation.v1.WorkerCapability;
import ai.protomolt.proto.delegation.v1.WorkerHello;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Timestamp;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkflowAuthoringMountTest {
    private static final String WORKER = "mount-test-worker";
    private static final byte[] KEY = "0123456789abcdef0123456789abcdef".getBytes();
    private static final RepositoryStateKeyResolver KEYS = ignored ->
            new javax.crypto.spec.SecretKeySpec(KEY, "AES");

    @TempDir Path directory;

    @Test
    void authoringIsOptInAndMissingServePrerequisitesFailClosed() {
        assertThat(new ProtoMoltServe.Options("127.0.0.1", 0, 0, null, 0)
                .workflowAuthoring()).isNull();
        var authoring = new ProtoMoltServe.WorkflowAuthoringOptions("a".repeat(64), directory);
        var jobs = new ProtoMoltServe.JobsOptions("jdbc:postgresql://localhost/protomolt",
                "user", "password", null, null, 0, 0);
        var delegation = new ProtoMoltServe.DelegationOptions("repo.example.test:443", true);
        var registry = directory.resolve("registry.git");
        var workspace = directory.resolve("workflows");

        assertThatThrownBy(() -> options(null, registry, workspace, delegation, jobs, false, authoring))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("API token");
        assertThatThrownBy(() -> options(" ", registry, workspace, delegation, jobs, false, authoring))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("API token");
        assertThatThrownBy(() -> options("token", null, workspace, delegation, jobs, false, authoring))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("persistent registry");
        assertThatThrownBy(() -> options("token", registry, null, delegation, jobs, false, authoring))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("workflow workspaces");
        assertThatThrownBy(() -> options("token", registry, workspace, null, jobs, false, authoring))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("durable delegation");
        assertThatThrownBy(() -> options("token", registry, workspace, delegation, null, false, authoring))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("jobs database");
        // Demo mode's temporary fallbacks do not satisfy the persistent authoring prerequisites.
        assertThatThrownBy(() -> options("token", null, null, delegation, jobs, true, authoring))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("persistent registry");
        assertThatThrownBy(() -> new ProtoMoltServe.WorkflowAuthoringOptions("A".repeat(64), directory))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("lowercase SHA-256");
    }

    @Test
    void mountRequiresTrustAndAnAlreadyStoredPolicyArtifact() throws Exception {
        var artifacts = new FileSystemArtifactRepository(directory.resolve("artifacts"));
        var runs = new FileSystemRunEvidenceRepository(directory.resolve("runs"));
        var options = new ProtoMoltServe.WorkflowAuthoringOptions("a".repeat(64),
                directory.resolve("authorizations"));

        assertThatThrownBy(() -> prepare(options, artifacts, runs, () -> null))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("trust is unavailable");
        assertThatThrownBy(() -> prepare(options, artifacts, runs, WorkflowAuthoringMountTest::trust))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("policy artifact is absent");
    }

    @Test
    void selectingReviewerKeepsOrdinaryTasksManualAndRoutesAuthoredContracts() throws Exception {
        var artifacts = new FileSystemArtifactRepository(directory.resolve("artifacts"));
        var runs = new FileSystemRunEvidenceRepository(directory.resolve("runs"));
        var prepared = prepared(artifacts, runs);
        CandidateReviewer reviewer = prepared.selectingReviewer();

        var ordinary = ordinarySpec();
        var ordinaryCandidate = ordinaryCandidate();
        assertThat(reviewer.review(new CandidateReviewer.ReviewContext(
                UUID.randomUUID().toString(), WORKER, ordinary, ordinaryCandidate)))
                .isEqualTo(CandidateReviewer.ReviewDecision.pending());

        var authored = authoringSpec(prepared.policyReference());
        var authoredCandidate = ordinaryCandidate().toBuilder()
                .setResult(Any.pack(WorkflowAuthoringDeliverable.getDefaultInstance())).build();
        assertThat(reviewer.review(new CandidateReviewer.ReviewContext(
                UUID.randomUUID().toString(), WORKER, authored, authoredCandidate)))
                .isInstanceOf(CandidateReviewer.ReviewDecision.Revise.class);
    }

    @Test
    void trustIsRefreshedForEachAuthoringOperationButNotForManualTasks() throws Exception {
        var artifacts = new FileSystemArtifactRepository(directory.resolve("artifacts"));
        var runs = new FileSystemRunEvidenceRepository(directory.resolve("runs"));
        var reads = new AtomicInteger();
        var prepared = prepared(artifacts, runs, () -> {
            int read = reads.incrementAndGet();
            return trust(read == 1 ? "mount-initial" : "mount-rotated");
        });
        CandidateReviewer reviewer = prepared.selectingReviewer();
        assertThat(reads).hasValue(1); // startup validates the configured trust snapshot

        reviewer.review(new CandidateReviewer.ReviewContext(UUID.randomUUID().toString(), WORKER,
                ordinarySpec(), ordinaryCandidate()));
        assertThat(reads).hasValue(1); // unrelated work stays manual and does not consult authoring trust

        reviewer.review(new CandidateReviewer.ReviewContext(UUID.randomUUID().toString(), WORKER,
                authoringSpec(prepared.policyReference()), ordinaryCandidate().toBuilder()
                        .setResult(Any.pack(WorkflowAuthoringDeliverable.getDefaultInstance())).build()));
        assertThat(reads).hasValue(2); // each routed authoring review gets the current snapshot

        var failureReads = new AtomicInteger();
        var failing = prepared(artifacts, runs, () -> {
            if (failureReads.incrementAndGet() > 1) throw new IllegalStateException("trust source failed");
            return trust("mount-initial");
        });
        assertThatThrownBy(() -> failing.selectingReviewer().review(
                new CandidateReviewer.ReviewContext(UUID.randomUUID().toString(), WORKER,
                        authoringSpec(failing.policyReference()), ordinaryCandidate().toBuilder()
                                .setResult(Any.pack(WorkflowAuthoringDeliverable.getDefaultInstance())).build())))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("trust source failed");
    }

    @Test
    void launcherOperationsUseTheRuntimeTranscriptRepository() throws Exception {
        var artifacts = new FileSystemArtifactRepository(directory.resolve("artifacts"));
        var runs = new FileSystemRunEvidenceRepository(directory.resolve("runs"));
        var prepared = prepared(artifacts, runs);
        String taskId = UUID.randomUUID().toString();

        try (DelegationRuntime runtime = DelegationRuntime.open(null, KEYS,
                Duration.ofSeconds(1), ignored -> null, CandidateReviewer.acceptAll())) {
            DelegationBridge bridge = runtime.bridge();
            assertThat(bridge.registerWorker(hello()).admitted()).isTrue();
            bridge.offer(WORKER, taskId, ordinarySpec(), Duration.ofMinutes(1), null);
            bridge.accept(WORKER, taskId, 1);
            long cursor = bridge.coordinator().eventsAfter(taskId, 0).getLast().cursor();
            bridge.submitCandidate(WORKER, taskId, ordinaryCandidate());
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (bridge.coordinator().state().tasks().get(taskId).phase()
                    != DelegationReducer.Phase.ACCEPTED) {
                long remaining = deadline - System.nanoTime();
                assertThat(remaining).as("automatic reviewer should accept").isPositive();
                var event = bridge.coordinator().waitForEvent(taskId, cursor,
                        Duration.ofNanos(remaining)).orElseThrow();
                cursor = event.cursor();
            }
            assertThat(bridge.coordinator().state().tasks().get(taskId).phase())
                    .isEqualTo(DelegationReducer.Phase.ACCEPTED);
            assertThat(runtime.transcripts().load()).isPresent();

            // The operation reaches the accepted ordinary task in the runtime's live
            // transcript. A disconnected/empty repository would instead report no transcript.
            assertThatThrownBy(() -> prepared.operations(runtime.transcripts())
                    .acceptedCandidate(taskId))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("does not offer the authoring result contract");
        }
    }

    private WorkflowAuthoringMount.Prepared prepared(ArtifactRepository artifacts,
            RunEvidenceRepository runs) throws Exception {
        return prepared(artifacts, runs, WorkflowAuthoringMountTest::trust);
    }

    private WorkflowAuthoringMount.Prepared prepared(ArtifactRepository artifacts,
            RunEvidenceRepository runs, Supplier<TrustSnapshot> trust) throws Exception {
        ArtifactReference descriptor = artifacts.save(new byte[]{1}, "application/x-protobuf", false);
        ArtifactReference input = artifacts.save(new byte[]{2}, "application/x-protobuf", false);
        ArtifactReference expected = artifacts.save(new byte[]{3}, "application/x-protobuf", false);
        var policy = WorkflowAuthoringPolicy.newBuilder().setDescriptors(descriptor)
                .addFixtures(WorkflowAcceptanceFixture.newBuilder().setName("basic")
                        .setInput(input).setExpectedOutput(expected))
                .addPermittedCalls(WorkflowPermittedCall.newBuilder()
                        .setTarget("localhost:9090").setMethod("example.v1.Echo/Run"))
                .build();
        ArtifactReference policyRef = artifacts.save(policy.toByteArray(),
                "application/x-protobuf", false);
        return prepare(new ProtoMoltServe.WorkflowAuthoringOptions(policyRef.getSha256(),
                        directory.resolve("authorizations")), artifacts, runs,
                trust);
    }

    private WorkflowAuthoringMount.Prepared prepare(ProtoMoltServe.WorkflowAuthoringOptions options,
            ArtifactRepository artifacts, RunEvidenceRepository runs,
            Supplier<TrustSnapshot> trust) {
        return WorkflowAuthoringMount.prepare(options, artifacts, runs,
                proxy(WorkflowVersionRepository.class), proxy(WorkflowRunStore.class),
                ActionContext.create(), new WorkflowRunner(), 3, trust);
    }

    private static ProtoMoltServe.Options options(String apiToken, Path registry, Path workspace,
            ProtoMoltServe.DelegationOptions delegation, ProtoMoltServe.JobsOptions jobs,
            boolean demo, ProtoMoltServe.WorkflowAuthoringOptions authoring) {
        return new ProtoMoltServe.Options("127.0.0.1", 0, 0, registry, 0, apiToken, demo,
                null, jobs, java.util.List.of(), null, null, workspace, delegation,
                null, null, null, null, null, authoring);
    }

    private static <T> T proxy(Class<T> type) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
                (ignored, method, arguments) -> {
                    Class<?> result = method.getReturnType();
                    if (!result.isPrimitive()) return null;
                    if (result == boolean.class) return false;
                    if (result == int.class) return 0;
                    if (result == long.class) return 0L;
                    return 0;
                }));
    }

    private static TrustSnapshot trust() {
        return trust("mount-test");
    }

    private static TrustSnapshot trust(String issuer) {
        var keys = RecordKeys.generate();
        return TrustSnapshot.newBuilder().addIssuers(TrustedIssuer.newBuilder()
                .setIssuer(issuer).addSubjectKinds(WorkRecords.SUBJECT_KIND_WORKFLOW_RUN)
                .addKeys(TrustedKey.newBuilder().setKeyId("mount-key")
                        .setAlgorithm(SignatureAlgorithm.SIGNATURE_ALGORITHM_ED25519)
                        .setState(KeyState.KEY_STATE_ACTIVE)
                        .setPublicKey(ByteString.copyFrom(RecordKeys.rawPublicKey(keys.getPublic())))))
                .build();
    }

    private static TaskSpec ordinarySpec() {
        return TaskSpec.newBuilder().setObjective("verify serve reviewer routing")
                .addRequiredChecks(AcceptanceCheck.newBuilder().setName("tests"))
                .build();
    }

    private static CompletionCandidate ordinaryCandidate() {
        return CompletionCandidate.newBuilder().setAttempt(1).setRevision(1)
                .setSummary("Candidate summary")
                .addEvidence(CheckEvidence.newBuilder().setCheckName("tests")
                        .setVerdict(CheckVerdict.CHECK_VERDICT_PASSED)
                        .setRanAt(Timestamp.newBuilder().setSeconds(1)))
                .addArtifacts(artifact("c", "application/x-protobuf"))
                .build();
    }

    private static TaskSpec authoringSpec(ArtifactReference policy) {
        var files = new LinkedHashMap<String, FileDescriptor>();
        collect(WorkflowAuthoringDeliverable.getDescriptor().getFile(), files);
        var descriptorSet = FileDescriptorSet.newBuilder();
        files.values().forEach(file -> descriptorSet.addFile(file.toProto()));
        var spec = TaskSpec.newBuilder().setObjective("verify authored workflow routing")
                .setContract(DeliverableContract.newBuilder()
                        .setTypeName(WorkflowAuthoringDeliverable.getDescriptor().getFullName())
                        .setDescriptorSet(descriptorSet.build().toByteString()))
                .addContext(policy);
        WorkflowAuthoringReviewer.REQUIRED_CHECKS.forEach(name -> spec.addRequiredChecks(
                AcceptanceCheck.newBuilder().setName(name).setDescription(name)));
        return spec.build();
    }

    private static void collect(FileDescriptor file, Map<String, FileDescriptor> files) {
        if (files.putIfAbsent(file.getName(), file) != null) return;
        file.getDependencies().forEach(dependency -> collect(dependency, files));
    }

    private static ArtifactReference artifact(String hash, String mediaType) {
        return ArtifactReference.newBuilder().setSha256(hash.repeat(64)).setMediaType(mediaType)
                .setSizeBytes(1).build();
    }

    private static WorkerHello hello() {
        return WorkerHello.newBuilder().setWorkerId(WORKER).setProtocolVersion(1)
                .setProvider("test").addCapabilities(WorkerCapability.newBuilder().setName("review"))
                .build();
    }
}
