package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.delegation.lifecycle.InMemoryTranscriptRepository;

import ai.protomolt.proto.delegation.lifecycle.DelegationReducer;

import ai.protomolt.proto.delegation.contract.DeliverableContracts;

import ai.protomolt.proto.receipt.WorkRecords;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.delegation.*;
import ai.protomolt.proto.delegation.v1.*;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.grpc.workflow.ArtifactRepository;
import ai.protomolt.proto.grpc.workflow.FileSystemArtifactRepository;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringDeliverable;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringPolicy;
import ai.protomolt.proto.samples.starter.v1.WorkflowPermittedCall;
import ai.protomolt.proto.samples.starter.v1.WorkflowAcceptanceFixture;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import java.time.Clock;
import java.time.Duration;
import java.nio.file.Path;
import java.io.IOException;
import java.util.Optional;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkflowAuthorTaskMutationsTest {
    private static final String TASK = "00000000-0000-4000-8000-000000000001";
    private static final Caller AUTHOR = Caller.scoped("author", Set.of(Scopes.WORKFLOW_AUTHOR));
    @TempDir Path temp;

    @Test void matchingRetriesDoNotRepeatFramesAndChangedRevisionConflicts() throws Exception {
        try (Fixture fixture = new Fixture(new InMemoryTranscriptRepository(), temp.resolve("retry"))) {
            fixture.registerAndOffer();
            fixture.mutations.accept(accept(), AUTHOR);
            int accepted = fixture.coordinator.transcript().getEntriesCount();
            fixture.mutations.accept(accept(), AUTHOR);
            assertThat(fixture.coordinator.transcript().getEntriesCount()).isEqualTo(accepted);
            var candidate = submit();
            fixture.mutations.submit(candidate, AUTHOR);
            int submitted = fixture.coordinator.transcript().getEntriesCount();
            fixture.mutations.submit(candidate, AUTHOR);
            assertThat(fixture.coordinator.transcript().getEntriesCount()).isEqualTo(submitted);
            fails(() -> fixture.mutations.submit(candidate.toBuilder().setCandidate(candidate.getCandidate()
                    .toBuilder().setSummary("different content")).build(), AUTHOR), WorkflowPreparationException.Kind.CONFLICT);
            assertThat(fixture.coordinator.transcript().getEntriesCount()).isEqualTo(submitted);
            fixture.coordinator.cancel(TASK, "stop task");
            int cancelled = fixture.coordinator.transcript().getEntriesCount();
            fixture.mutations.submit(candidate, AUTHOR); // Acknowledge history, not active permission.
            fixture.mutations.accept(accept(), AUTHOR);
            assertThat(fixture.coordinator.transcript().getEntriesCount()).isEqualTo(cancelled);
        }
    }

    @Test void concurrentAcceptanceWritesOnceAndRecoveryUsesDurableHistory() throws Exception {
        var repository = new InMemoryTranscriptRepository();
        try (Fixture fixture = new Fixture(repository, temp.resolve("restart"))) {
            fixture.registerAndOffer();
            try (var threads = Executors.newFixedThreadPool(2)) {
                var start = new java.util.concurrent.CountDownLatch(1);
                var one = threads.submit(() -> { start.await(); return fixture.mutations.accept(accept(), AUTHOR); });
                var two = threads.submit(() -> { start.await(); return fixture.mutations.accept(accept(), AUTHOR); });
                start.countDown();
                assertThat(one.get(5, java.util.concurrent.TimeUnit.SECONDS)).isEqualTo(two.get(5, java.util.concurrent.TimeUnit.SECONDS));
            }
            assertThat(fixture.coordinator.transcript().getEntriesList().stream().filter(entry ->
                    entry.hasWorkerFrame() && entry.getWorkerFrame().hasAccept()).count()).isEqualTo(1);
            fixture.mutations.submit(submit(), AUTHOR);
        }
        try (Fixture recovered = new Fixture(repository, temp.resolve("restart"))) {
            recovered.mutations.register(registration(), AUTHOR);
            int before = recovered.coordinator.transcript().getEntriesCount();
            recovered.mutations.accept(accept(), AUTHOR);
            recovered.mutations.submit(submit(), AUTHOR);
            assertThat(recovered.coordinator.transcript().getEntriesCount()).isEqualTo(before);
        }
    }

    @Test void wrongIdentityInactiveAttemptAndBadDeliverableCannotMutateOrReachReview() throws Exception {
        try (Fixture fixture = new Fixture(new InMemoryTranscriptRepository(), temp.resolve("invalid"))) {
            fixture.registerAndOffer();
            int before = fixture.coordinator.transcript().getEntriesCount();
            fails(() -> fixture.mutations.accept(accept(), Caller.operator()), WorkflowPreparationException.Kind.PERMISSION_DENIED);
            fails(() -> fixture.mutations.accept(accept().toBuilder().setAttempt(2).build(), AUTHOR), WorkflowPreparationException.Kind.INACTIVE);
            assertThat(fixture.coordinator.transcript().getEntriesCount()).isEqualTo(before);
            fixture.mutations.accept(accept(), AUTHOR);
            before = fixture.coordinator.transcript().getEntriesCount();
            var invalid = submit().toBuilder().setCandidate(submit().getCandidate().toBuilder().setResult(
                    Any.pack(WorkflowAuthoringDeliverable.getDefaultInstance()))).build();
            fails(() -> fixture.mutations.submit(invalid, AUTHOR), WorkflowPreparationException.Kind.INVALID_INPUT);
            assertThat(fixture.coordinator.transcript().getEntriesCount()).isEqualTo(before);
            assertThat(fixture.coordinator.state().tasks().get(TASK).phase()).isEqualTo(DelegationReducer.Phase.LEASED);
        }
    }

    @Test void duplicateLiveRegistrationAndExpiredNewMutationAreRejected() throws Exception {
        try (Fixture fixture = new Fixture(new InMemoryTranscriptRepository(), temp.resolve("expiry"))) {
            fixture.registerAndOffer();
            fails(() -> fixture.mutations.register(registration(), AUTHOR), WorkflowPreparationException.Kind.CONFLICT);
            var expired = new WorkflowAuthorTaskMutations(fixture.bridge, fixture.artifacts, fixture.policy,
                    Clock.offset(Clock.systemUTC(), Duration.ofDays(1)));
            int before = fixture.coordinator.transcript().getEntriesCount();
            fails(() -> expired.accept(accept(), AUTHOR), WorkflowPreparationException.Kind.INACTIVE);
            assertThat(fixture.coordinator.transcript().getEntriesCount()).isEqualTo(before);
        }
    }

    @Test void missingOrCorruptPolicyCannotAcceptAndCandidateEvidenceMustMatchPackedDeliverable()
            throws Exception {
        try (Fixture fixture = new Fixture(new InMemoryTranscriptRepository(), temp.resolve("policy"))) {
            fixture.registerAndOffer();
            WorkflowAuthorTaskMutations withoutPolicy = new WorkflowAuthorTaskMutations(fixture.bridge,
                    fixture.absentArtifact(fixture.policy), fixture.policy, Clock.systemUTC());
            int before = fixture.coordinator.transcript().getEntriesCount();
            fails(() -> withoutPolicy.accept(accept(), AUTHOR), WorkflowPreparationException.Kind.CORRUPT_EVIDENCE);
            assertThat(fixture.coordinator.transcript().getEntriesCount()).isEqualTo(before);
            assertThat(fixture.coordinator.state().tasks().get(TASK).phase()).isEqualTo(DelegationReducer.Phase.OFFERED);

            fixture.mutations.accept(accept(), AUTHOR);
            var valid = submit();
            WorkflowAuthoringDeliverable authored = valid.getCandidate().getResult()
                    .unpack(WorkflowAuthoringDeliverable.class);
            var changed = authored.toBuilder().setDeliverable(authored.getDeliverable().toBuilder()
                    .setChecks(0, authored.getDeliverable().getChecks(0).toBuilder()
                            .setVerdict(CheckVerdict.CHECK_VERDICT_FAILED))).build();
            var mismatchedEvidence = valid.toBuilder().setCandidate(valid.getCandidate().toBuilder()
                    .setResult(Any.pack(changed))).build();
            before = fixture.coordinator.transcript().getEntriesCount();
            fails(() -> fixture.mutations.submit(mismatchedEvidence, AUTHOR),
                    WorkflowPreparationException.Kind.INVALID_INPUT);
            assertThat(fixture.coordinator.transcript().getEntriesCount()).isEqualTo(before);
            assertThat(fixture.coordinator.state().tasks().get(TASK).phase()).isEqualTo(DelegationReducer.Phase.LEASED);
        }
        try (Fixture fixture = new Fixture(new InMemoryTranscriptRepository(), temp.resolve("corrupt-policy"))) {
            fixture.mutations.register(registration(), AUTHOR);
            ArtifactReference malformed = fixture.artifacts.save(new byte[] {(byte) 0xff},
                    "application/x-protobuf", false);
            var configured = new WorkflowAuthorTaskMutations(fixture.bridge, fixture.artifacts,
                    malformed, Clock.systemUTC());
            fixture.coordinator.offer("author", TASK, spec(malformed), Duration.ofMinutes(5));
            int before = fixture.coordinator.transcript().getEntriesCount();
            fails(() -> configured.accept(accept(), AUTHOR), WorkflowPreparationException.Kind.CORRUPT_EVIDENCE);
            assertThat(fixture.coordinator.transcript().getEntriesCount()).isEqualTo(before);
            assertThat(fixture.coordinator.state().tasks().get(TASK).phase()).isEqualTo(DelegationReducer.Phase.OFFERED);
        }
    }

    @Test void nativelyValidOversizedCompletionIsRejectedWithoutMutationThenValidRetrySucceeds()
            throws Exception {
        try (Fixture fixture = new Fixture(new InMemoryTranscriptRepository(), temp.resolve("frame-limit"))) {
            fixture.registerAndOffer();
            fixture.mutations.accept(accept(), AUTHOR);
            SubmitCandidateRequest validRequest = submit();
            WorkflowAuthoringDeliverable valid = validRequest.getCandidate().getResult()
                    .unpack(WorkflowAuthoringDeliverable.class);
            WorkflowAuthoringDeliverable oversized = valid.toBuilder().setDeliverable(
                    valid.getDeliverable().toBuilder().setWorkflow(valid.getDeliverable().getWorkflow()
                            .toBuilder().setDescription("x".repeat(1_050_000)))).build();
            WorkflowPreparationValidation.validate(oversized, 4 * 1024 * 1024);
            var oversizedCandidate = validRequest.getCandidate().toBuilder().setResult(Any.pack(oversized)).build();
            assertThat(oversizedCandidate.getSerializedSize()).isGreaterThan(1_048_576);
            assertThat(DeliverableContracts.check(spec(fixture.policy).getContract(),
                    oversizedCandidate.getResult())).isEmpty();
            SubmitCandidateRequest oversizedRequest = validRequest.toBuilder().setCandidate(oversizedCandidate).build();
            WorkflowPreparationValidation.validate(oversizedRequest, 16 * 1024 * 1024);
            long acceptedSequence = fixture.coordinator.transcript().getEntriesList().stream()
                    .filter(entry -> entry.hasWorkerFrame() && entry.getWorkerFrame().hasAccept())
                    .mapToLong(entry -> entry.getWorkerFrame().getSeq()).findFirst().orElseThrow();
            int before = fixture.coordinator.transcript().getEntriesCount();

            fails(() -> fixture.mutations.submit(oversizedRequest, AUTHOR),
                    WorkflowPreparationException.Kind.INVALID_INPUT);
            assertThat(fixture.coordinator.transcript().getEntriesCount()).isEqualTo(before);
            assertThat(fixture.coordinator.state().tasks().get(TASK).phase()).isEqualTo(DelegationReducer.Phase.LEASED);

            fixture.mutations.submit(validRequest, AUTHOR);
            Transcript completed = fixture.coordinator.transcript();
            assertThat(completed.getEntriesCount()).isBetween(before + 2, before + 3);
            assertThat(completed.getEntries(before).getWorkerFrame().hasCompletion()).isTrue();
            assertThat(completed.getEntries(before + 1).getCoordinatorFrame().hasReviewStarted()).isTrue();
            if (completed.getEntriesCount() == before + 3) {
                assertThat(completed.getEntries(before + 2).getCoordinatorFrame().hasReviewDeferred()).isTrue();
            }
            assertThat(completed.getEntries(before + 1).getCoordinatorFrame().getReviewStarted()
                    .getIdentity().getCandidateEntrySha256())
                    .isEqualTo(WorkRecords.fingerprint(completed.getEntries(before)));
            assertThat(completed.getEntriesList().stream()
                    .filter(entry -> entry.hasWorkerFrame() && entry.getWorkerFrame().hasCompletion())
                    .mapToLong(entry -> entry.getWorkerFrame().getSeq()).findFirst().orElseThrow())
                    .isEqualTo(acceptedSequence + 1);
            assertThat(new DelegationReducer().reduce(completed).clean()).isTrue();
        }
    }

    private static final class Fixture implements AutoCloseable {
        final InProcessDelegationCoordinator coordinator;
        final DelegationBridge bridge;
        final WorkflowAuthorTaskMutations mutations;
        final FileSystemArtifactRepository artifacts;
        final ArtifactReference policy;
        Fixture(InMemoryTranscriptRepository repository, Path artifactRoot) throws Exception {
            artifacts = new FileSystemArtifactRepository(artifactRoot);
            var descriptors = FileDescriptorSet.newBuilder();
            Map<String, FileDescriptor> files = new LinkedHashMap<>();
            collect(WorkflowAuthoringDeliverable.getDescriptor().getFile(), files);
            files.values().forEach(file -> descriptors.addFile(file.toProto()));
            ArtifactReference descriptorRef = artifacts.save(descriptors.build().toByteArray(),
                    "application/x-protobuf", false);
            var policyMessage = WorkflowAuthoringPolicy.newBuilder().setDescriptors(descriptorRef)
                    .addFixtures(WorkflowAcceptanceFixture.newBuilder().setName("fixture")
                            .setInput(artifact("c".repeat(64), "application/x-protobuf"))
                            .setExpectedOutput(artifact("d".repeat(64), "application/x-protobuf")))
                    .addPermittedCalls(WorkflowPermittedCall.newBuilder().setTarget("local")
                            .setMethod("example.Echo/Run"));
            policy = artifacts.save(policyMessage.build().toByteArray(), "application/x-protobuf", false);
            coordinator = new InProcessDelegationCoordinator(AdmissionPolicy.allowAll(), CandidateReviewer.manual(), Clock.systemUTC(), repository);
            bridge = new DelegationBridge(coordinator);
            mutations = new WorkflowAuthorTaskMutations(bridge, artifacts, policy, Clock.systemUTC());
        }
        void registerAndOffer() throws Exception {
            mutations.register(registration(), AUTHOR);
            coordinator.offer("author", TASK, spec(policy), Duration.ofMinutes(5));
        }
        ArtifactRepository absentArtifact(ArtifactReference absent) {
            return new ArtifactRepository() {
                @Override public ArtifactReference save(byte[] content, String mediaType, boolean redacted)
                        throws IOException { return artifacts.save(content, mediaType, redacted); }
                @Override public Optional<StoredArtifact> find(String sha256) throws IOException {
                    if (sha256.equals(absent.getSha256())) return Optional.empty();
                    return artifacts.find(sha256);
                }
            };
        }
        @Override public void close() { bridge.close(); coordinator.close(); }
    }
    private static RegisterWorkerRequest registration() { return RegisterWorkerRequest.newBuilder().setWorkerId("author").build(); }
    private static AcceptTaskRequest accept() { return AcceptTaskRequest.newBuilder().setTaskId(TASK).setWorkerId("author").setAttempt(1).build(); }
    private static SubmitCandidateRequest submit() {
        var authored = WorkflowPreparationContractTest.authored("a".repeat(64));
        var deliverable = authored.getDeliverable().toBuilder().clearChecks();
        for (String check : WorkflowAuthoringReviewer.REQUIRED_CHECKS) {
            deliverable.addChecks(authored.getDeliverable().getChecks(0).toBuilder().setCheckName(check));
        }
        authored = authored.toBuilder().setDeliverable(deliverable).build();
        var candidate = CompletionCandidate.newBuilder().setAttempt(1).setRevision(1).setSummary("Prepared workflow")
                .setResult(Any.pack(authored)).addArtifacts(authored.getExecutableSource());
        candidate.addAllEvidence(authored.getDeliverable().getChecksList());
        return SubmitCandidateRequest.newBuilder().setWorkerId("author").setTaskId(TASK).setCandidate(candidate).build();
    }
    private static ArtifactReference artifact(String hash, String mediaType) {
        return ArtifactReference.newBuilder().setSha256(hash).setMediaType(mediaType).setSizeBytes(24).build();
    }
    private static TaskSpec spec(ArtifactReference policy) {
        Map<String, FileDescriptor> files = new LinkedHashMap<>();
        collect(WorkflowAuthoringDeliverable.getDescriptor().getFile(), files);
        var descriptors = FileDescriptorSet.newBuilder();
        files.values().forEach(file -> descriptors.addFile(file.toProto()));
        var spec = TaskSpec.newBuilder().setObjective("Author a workflow").addContext(policy)
                .setContract(DeliverableContract.newBuilder().setTypeName(WorkflowAuthoringDeliverable.getDescriptor().getFullName())
                        .setDescriptorSet(ByteString.copyFrom(descriptors.build().toByteArray())));
        WorkflowAuthoringReviewer.REQUIRED_CHECKS.forEach(check -> spec.addRequiredChecks(AcceptanceCheck.newBuilder().setName(check)));
        return spec.build();
    }
    private static void collect(FileDescriptor file, Map<String, FileDescriptor> files) {
        if (files.containsKey(file.getName())) return;
        file.getDependencies().forEach(dependency -> collect(dependency, files));
        files.put(file.getName(), file);
    }
    private static void fails(org.assertj.core.api.ThrowableAssert.ThrowingCallable operation, WorkflowPreparationException.Kind kind) {
        assertThatThrownBy(operation).isInstanceOfSatisfying(WorkflowPreparationException.class,
                failure -> assertThat(failure.kind()).isEqualTo(kind));
    }
}
