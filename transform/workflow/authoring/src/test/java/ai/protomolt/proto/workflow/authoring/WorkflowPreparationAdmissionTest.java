package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.delegation.AdmissionPolicy;
import ai.protomolt.proto.delegation.CandidateReviewer;
import ai.protomolt.proto.delegation.CandidateReviewer.ReviewContext;
import ai.protomolt.proto.delegation.CandidateReviewer.ReviewDecision;
import ai.protomolt.proto.delegation.DelegationReducer;
import ai.protomolt.proto.delegation.DelegationWorker;
import ai.protomolt.proto.delegation.InMemoryTranscriptRepository;
import ai.protomolt.proto.delegation.InProcessDelegationCoordinator;
import ai.protomolt.proto.delegation.ScriptedWorkerRunner;
import ai.protomolt.proto.delegation.TranscriptRepository;
import ai.protomolt.proto.delegation.WorkerRunner;
import ai.protomolt.proto.delegation.v1.AcceptanceCheck;
import ai.protomolt.proto.delegation.v1.CheckEvidence;
import ai.protomolt.proto.delegation.v1.CheckVerdict;
import ai.protomolt.proto.delegation.v1.CompletionCandidate;
import ai.protomolt.proto.delegation.v1.DeliverableContract;
import ai.protomolt.proto.delegation.v1.TaskSpec;
import ai.protomolt.proto.delegation.v1.Transcript;
import ai.protomolt.proto.delegation.v1.TranscriptEntry;
import ai.protomolt.proto.delegation.v1.WorkerCapability;
import ai.protomolt.proto.delegation.v1.WorkerHello;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.grpc.workflow.v1.ServiceDependency;
import ai.protomolt.proto.grpc.workflow.v1.StepCompletion;
import ai.protomolt.proto.grpc.workflow.v1.Workflow;
import ai.protomolt.proto.grpc.workflow.v1.WorkflowStep;
import ai.protomolt.proto.samples.starter.v1.WorkflowAcceptanceFixture;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringDeliverable;
import ai.protomolt.proto.samples.starter.v1.WorkflowDeliverable;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringPolicy;
import ai.protomolt.proto.receipt.WorkRecords;
import ai.protomolt.proto.workflow.CompiledWorkflow;
import ai.protomolt.proto.workflow.WorkflowCompiler;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import com.google.protobuf.UnknownFieldSet;
import com.google.protobuf.util.Timestamps;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Pure admission proof from the real reducer's durable transcript. */
class WorkflowPreparationAdmissionTest {
    private static final String TASK_ID = "00000000-0000-4000-8000-000000000001";
    private static final String HOLDER = "author-worker";
    private static final String SHA = "a".repeat(64);
    private static final String VALIDATE = "ai/protomolt/proto/validate/v1/validate.proto";
    private static final String PROTO = """
            syntax = "proto3";
            package preparation.admission.fixture;
            message Input { string name = 1; }
            message Result { string text = 1; }
            service Echo { rpc Run(Input) returns (Result); }
            """;
    private static final Instant START = Instant.parse("2026-09-30T12:34:56Z");
    private static final Clock CLOCK = Clock.fixed(START, ZoneOffset.UTC);
    private static final ArtifactReference POLICY = artifact("c".repeat(64), "application/x-protobuf", 32);
    private static final WorkflowAuthoringDeliverable AUTHORED;
    private static final TaskSpec SPEC;
    private static final CompletionCandidate CANDIDATE;

    static {
        try {
            var compiled = new ProtoSourceCompiler().compile(ai.protomolt.proto.sources.ProtoSourceSet.builder()
                    .add(VALIDATE, resource(VALIDATE), "test")
                    .add("preparation/admission.proto", PROTO, "test").build());
            FileDescriptor file = compiled.descriptorFor("preparation/admission.proto").orElseThrow();
            Workflow workflow = WorkflowCompiler.compile(new CompiledWorkflow("prepared-workflow", List.of(file),
                    file.findMessageTypeByName("Input"), 30_000,
                    List.of(CompiledWorkflow.Step.grpc("echo", "fixture:9090", false,
                            CompiledWorkflow.resolveMethod(List.of(file),
                                    "preparation.admission.fixture.Echo/Run"), null,
                            List.of("name = input.name"), List.of(), false, 0, "")), null, true));
            AUTHORED = authored(workflow);
            SPEC = spec();
            CANDIDATE = candidate(AUTHORED);
        } catch (Exception failure) {
            throw new ExceptionInInitializerError(failure);
        }
    }

    @Test
    void validLeasedOfferAndAcceptProduceBoundSnapshot() throws Exception {
        try (Fixture fixture = activeLease(SPEC, null)) {
            var proof = inspect(fixture, request(1, 1), holder(),
                    CLOCK, WorkflowPreparationAdmission.Mode.EXECUTE, POLICY);
            TranscriptEntry offer = fixture.transcript().getEntriesList().stream()
                    .filter(TranscriptEntry::hasCoordinatorFrame)
                    .filter(entry -> entry.getCoordinatorFrame().hasOffer()).findFirst().orElseThrow();
            assertThat(proof.selectedOffer()).isEqualTo(offer);
            assertThat(proof.binding().getTaskId()).isEqualTo(TASK_ID);
            assertThat(proof.binding().getAttempt()).isEqualTo(1);
            assertThat(proof.binding().getRevision()).isEqualTo(1);
            assertThat(proof.binding().getPreparationId()).isEqualTo(request(1, 1).getPreparationId());
            assertThat(proof.binding().getOfferEntrySha256()).isEqualTo(WorkRecords.sha256Hex(offer.toByteArray()));
            assertThat(proof.leaseExpiry()).isEqualTo(offer.getCoordinatorFrame().getOffer().getExpiresAt());
        }
    }

    @Test
    void callerMustBeHolderEvenWhenCallerIsUnrestrictedOperator() throws Exception {
        try (Fixture fixture = activeLease(SPEC, null)) {
            assertThatThrownBy(() -> inspect(fixture, request(1, 1), Caller.operator(), CLOCK,
                    WorkflowPreparationAdmission.Mode.EXECUTE, POLICY)).isInstanceOf(SecurityException.class);
            assertThatThrownBy(() -> inspect(fixture, request(1, 1), Caller.scoped("other-worker",
                    Set.of(Scopes.WORKER_COORDINATE)), CLOCK, WorkflowPreparationAdmission.Mode.EXECUTE, POLICY))
                    .isInstanceOf(SecurityException.class);
        }
    }

    @Test
    void exactLeaseExpiryIsRefusedEvenWithoutAnExpiredFrame() throws Exception {
        try (Fixture fixture = activeLease(SPEC, null)) {
            Timestamp expiry = leaseExpiry(fixture.transcript());
            assertThat(fixture.transcript().getEntriesList().stream()
                    .noneMatch(entry -> entry.hasCoordinatorFrame() && entry.getCoordinatorFrame().hasExpired()))
                    .isTrue();
            assertThat(inspect(fixture, request(1, 1), holder(), offset(clock(expiry), -1),
                    WorkflowPreparationAdmission.Mode.EXECUTE, POLICY).leaseExpiry()).isEqualTo(expiry);
            assertRejected(fixture, request(1, 1), clock(expiry), WorkflowPreparationAdmission.Mode.EXECUTE, POLICY);
            assertRejected(fixture, request(1, 1), offset(clock(expiry), 1),
                    WorkflowPreparationAdmission.Mode.EXECUTE, POLICY);
        }
    }

    @Test
    void renewalExtendsLeaseButBindingRetainsSelectedOriginalOfferHash() throws Exception {
        try (Fixture fixture = activeLease(SPEC, null)) {
            TranscriptEntry offer = selectedOffer(fixture.transcript());
            Timestamp oldExpiry = offer.getCoordinatorFrame().getOffer().getExpiresAt();
            fixture.events().get().heartbeat("test renewal");
            Transcript renewed = awaitRenewal(fixture.repository());
            var proof = inspect(fixture, request(1, 1), holder(), offset(clock(oldExpiry), 1000),
                    WorkflowPreparationAdmission.Mode.EXECUTE, POLICY);
            TranscriptEntry renewedOffer = selectedOffer(renewed);
            assertThat(renewedOffer).isEqualTo(offer);
            assertThat(proof.selectedOffer()).isEqualTo(offer);
            assertThat(proof.binding().getOfferEntrySha256()).isEqualTo(WorkRecords.sha256Hex(offer.toByteArray()));
            assertThat(proof.leaseExpiry()).isEqualTo(renewalExpiry(renewed));
            assertThat(Timestamps.compare(proof.leaseExpiry(), oldExpiry)).isPositive();
        }
    }

    @Test
    void duplicateOfferAndReplayedOldRenewalDoNotChangeSelectedOfferOrExpiry() throws Exception {
        try (Fixture fixture = activeLease(SPEC, null)) {
            Transcript initial = fixture.transcript();
            TranscriptEntry offer = selectedOffer(initial);
            Transcript duplicateOffer = initial.toBuilder().addEntries(offer).build();
            var firstProof = inspect(repository(duplicateOffer), request(1, 1), holder(), CLOCK,
                    WorkflowPreparationAdmission.Mode.EXECUTE, POLICY);
            assertThat(firstProof.selectedOffer()).isEqualTo(offer);

            fixture.events().get().heartbeat("first renewal");
            Transcript oneRenewal = awaitRenewal(fixture.repository());
            TranscriptEntry oldRenewal = oneRenewal.getEntriesList().stream()
                    .filter(TranscriptEntry::hasCoordinatorFrame)
                    .filter(entry -> entry.getCoordinatorFrame().hasRenewal()).findFirst().orElseThrow();
            fixture.events().get().heartbeat("second renewal");
            Transcript twoRenewals = awaitRenewalCount(fixture.repository(), 2);
            Timestamp newest = renewalExpiry(twoRenewals);
            Transcript replayedOld = twoRenewals.toBuilder().addEntries(oldRenewal).build();
            var proof = inspect(repository(replayedOld), request(1, 1), holder(), offset(clock(newest), -1),
                    WorkflowPreparationAdmission.Mode.EXECUTE, POLICY);
            assertThat(proof.selectedOffer()).isEqualTo(offer);
            assertThat(proof.binding().getOfferEntrySha256()).isEqualTo(WorkRecords.sha256Hex(offer.toByteArray()));
            assertThat(proof.leaseExpiry()).isEqualTo(newest);
        }
    }

    @Test
    void staleAttemptAndPreviousRevisionAreRejectedAfterRevisionRequest() throws Exception {
        CandidateReviewer reviseOnce = new CandidateReviewer() {
            private int calls;
            @Override public ReviewDecision review(ReviewContext context) {
                return ++calls == 1 ? ReviewDecision.revise("correct mapping", WorkflowAuthoringReviewer.REQUIRED_CHECKS)
                        : ReviewDecision.pending();
            }
        };
        try (Fixture fixture = revisedLease(reviseOnce)) {
            assertThat(new DelegationReducer().reduce(fixture.transcript()).clean()).isTrue();
            assertThat(fixture.transcript().getEntriesList().stream()
                    .anyMatch(entry -> entry.hasCoordinatorFrame()
                            && entry.getCoordinatorFrame().hasRevisionRequested())).isTrue();
            var current = inspect(fixture, request(1, 2), holder(), CLOCK,
                    WorkflowPreparationAdmission.Mode.EXECUTE, POLICY);
            assertThat(current.binding().getRevision()).isEqualTo(2);
            assertRejected(fixture, request(1, 1), CLOCK, WorkflowPreparationAdmission.Mode.EXECUTE, POLICY);
            assertRejected(fixture, request(2, 2), CLOCK, WorkflowPreparationAdmission.Mode.EXECUTE, POLICY);
        }
    }

    @Test
    void candidateIsReplayOnlyAndCompletedReplayAllowsElapsedLeaseAtSameRevision() throws Exception {
        try (Fixture fixture = candidateTranscript(CandidateReviewer.manual(), false, false)) {
            Timestamp expiry = leaseExpiry(fixture.transcript());
            assertRejected(fixture, request(1, 1), CLOCK, WorkflowPreparationAdmission.Mode.EXECUTE, POLICY);
            var replay = inspect(fixture, request(1, 1), holder(), offset(clock(expiry), 20_000),
                    WorkflowPreparationAdmission.Mode.REPLAY_COMPLETED, POLICY);
            assertThat(replay.binding().getRevision()).isEqualTo(1);
            assertThat(replay.leaseExpiry()).isEqualTo(expiry);
            assertRejected(fixture, request(1, 2), CLOCK,
                    WorkflowPreparationAdmission.Mode.REPLAY_COMPLETED, POLICY);
        }
    }

    @Test
    void cancelledAcceptedDirtyAndUnknownTranscriptsFailClosed() throws Exception {
        try (Fixture cancelled = candidateTranscript(CandidateReviewer.manual(), true, false)) {
            assertRejected(cancelled, request(1, 1), CLOCK,
                    WorkflowPreparationAdmission.Mode.REPLAY_COMPLETED, POLICY);
        }
        try (Fixture accepted = candidateTranscript(CandidateReviewer.acceptAll(), false, true)) {
            assertRejected(accepted, request(1, 1), CLOCK,
                    WorkflowPreparationAdmission.Mode.REPLAY_COMPLETED, POLICY);
        }

        try (Fixture fixture = candidateTranscript(CandidateReviewer.manual(), false, false)) {
            Transcript source = fixture.transcript();
            TranscriptEntry workerEntry = source.getEntriesList().stream()
                    .filter(TranscriptEntry::hasWorkerFrame).filter(entry -> entry.getWorkerFrame().hasCompletion())
                    .findFirst().orElseThrow();
            int index = source.getEntriesList().indexOf(workerEntry);
            Transcript dirty = source.toBuilder().setEntries(index, workerEntry.toBuilder()
                    .setWorkerFrame(workerEntry.getWorkerFrame().toBuilder().setSeq(99))).build();
            assertThatThrownBy(() -> inspect(repository(dirty), request(1, 1), holder(), CLOCK,
                    WorkflowPreparationAdmission.Mode.REPLAY_COMPLETED, POLICY))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("findings");

            TranscriptEntry offer = selectedOffer(source);
            int offerIndex = source.getEntriesList().indexOf(offer);
            Transcript unknown = source.toBuilder().setEntries(offerIndex, offer.toBuilder().setUnknownFields(
                    UnknownFieldSet.newBuilder().addField(99,
                            UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build())).build();
            assertThatThrownBy(() -> inspect(repository(unknown), request(1, 1), holder(), CLOCK,
                    WorkflowPreparationAdmission.Mode.REPLAY_COMPLETED, POLICY))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("unknown protocol fields");
        }
    }

    @Test
    void configuredPolicyAuthoringTypeAndRequiredChecksAreExact() throws Exception {
        try (Fixture base = activeLease(SPEC, null)) {
            assertRejected(base, request(1, 1), CLOCK, WorkflowPreparationAdmission.Mode.EXECUTE,
                    artifact("d".repeat(64), "application/x-protobuf", 32));
            assertRejected(base, request(1, 1), CLOCK, WorkflowPreparationAdmission.Mode.EXECUTE,
                    ArtifactReference.newBuilder().setSha256("d".repeat(64))
                            .setMediaType("application/x-protobuf").setSizeBytes(32).setRedacted(true).build());
            assertRejected(base, request(1, 1), CLOCK, WorkflowPreparationAdmission.Mode.EXECUTE,
                    artifact("d".repeat(64), "application/x-protobuf", 4 * 1024 * 1024 + 1));
        }
        TaskSpec otherType = SPEC.toBuilder().setContract(SPEC.getContract().toBuilder()
                .setTypeName(WorkflowDeliverable.getDescriptor().getFullName())).build();
        try (Fixture fixture = activeLease(otherType, null)) {
            assertRejected(fixture, request(1, 1), CLOCK, WorkflowPreparationAdmission.Mode.EXECUTE, POLICY);
        }
        TaskSpec missingCheck = SPEC.toBuilder().clearRequiredChecks().addRequiredChecks(
                AcceptanceCheck.newBuilder().setName("artifact_and_inline_workflow_match"))
                .addRequiredChecks(AcceptanceCheck.newBuilder().setName("invented-check")).build();
        try (Fixture fixture = activeLease(missingCheck, null)) {
            assertRejected(fixture, request(1, 1), CLOCK, WorkflowPreparationAdmission.Mode.EXECUTE, POLICY);
        }
    }

    private static Fixture activeLease(TaskSpec taskSpec, CandidateReviewer reviewer) throws Exception {
        return Fixture.start(taskSpec, reviewer == null ? CandidateReviewer.manual() : reviewer, false, false);
    }

    private static Fixture candidateTranscript(CandidateReviewer reviewer, boolean cancel, boolean expectAccepted)
            throws Exception {
        Fixture fixture = Fixture.start(SPEC, reviewer, false, false);
        fixture.release();
        fixture.awaitPhase(expectAccepted ? DelegationReducer.Phase.ACCEPTED : DelegationReducer.Phase.CANDIDATE);
        if (cancel) fixture.coordinator().cancel(TASK_ID, "cancel for admission test");
        fixture.awaitTerminalOrCandidate(cancel ? DelegationReducer.Phase.CANCELLED :
                (expectAccepted ? DelegationReducer.Phase.ACCEPTED : DelegationReducer.Phase.CANDIDATE));
        return fixture;
    }

    private static Fixture revisedLease(CandidateReviewer reviewer) throws Exception {
        return Fixture.start(SPEC, reviewer, true, false);
    }

    private static WorkflowPreparationAdmission.Snapshot inspect(Fixture fixture,
            ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowCandidateRequest request,
            Caller caller, Clock clock, WorkflowPreparationAdmission.Mode mode,
            ArtifactReference policy) {
        return inspect(fixture.repository(), request, caller, clock, mode, policy);
    }

    private static WorkflowPreparationAdmission.Snapshot inspect(TranscriptRepository repository,
            ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowCandidateRequest request,
            Caller caller, Clock clock, WorkflowPreparationAdmission.Mode mode,
            ArtifactReference policy) {
        return WorkflowPreparationAdmission.inspect(repository, request, caller, clock, mode, policy);
    }

    private static void assertRejected(Fixture fixture,
            ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowCandidateRequest request,
            Clock clock, WorkflowPreparationAdmission.Mode mode, ArtifactReference policy) {
        assertThatThrownBy(() -> inspect(fixture, request, holder(), clock, mode, policy))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowCandidateRequest request(
            int attempt, int revision) {
        return ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowCandidateRequest.newBuilder()
                .setTaskId(TASK_ID).setAttempt(attempt).setRevision(revision)
                .setPreparationId("00000000-0000-4000-8000-000000000099")
                .setExecutableSourceJson(ByteString.copyFromUtf8("{}")) .build();
    }

    private static Caller holder() { return Caller.scoped(HOLDER, Set.of(Scopes.WORKER_COORDINATE)); }

    private static Clock offset(Clock base, long millis) {
        return Clock.offset(base, java.time.Duration.ofMillis(millis));
    }

    private static Clock clock(Timestamp timestamp) {
        return Clock.fixed(Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos()), ZoneOffset.UTC);
    }

    private static Timestamp leaseExpiry(Transcript transcript) { return selectedOffer(transcript)
            .getCoordinatorFrame().getOffer().getExpiresAt(); }

    private static Timestamp renewalExpiry(Transcript transcript) {
        return transcript.getEntriesList().stream().filter(TranscriptEntry::hasCoordinatorFrame)
                .filter(entry -> entry.getCoordinatorFrame().hasRenewal())
                .reduce((left, right) -> right).orElseThrow().getCoordinatorFrame().getRenewal().getExpiresAt();
    }

    private static TranscriptEntry selectedOffer(Transcript transcript) {
        return transcript.getEntriesList().stream().filter(TranscriptEntry::hasCoordinatorFrame)
                .filter(entry -> entry.getCoordinatorFrame().hasOffer()).findFirst().orElseThrow();
    }

    private static Transcript awaitRenewal(InMemoryTranscriptRepository repository) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < until) {
            Transcript transcript = repository.load().orElseThrow();
            if (transcript.getEntriesList().stream().anyMatch(entry -> entry.hasCoordinatorFrame()
                    && entry.getCoordinatorFrame().hasRenewal())) return transcript;
            Thread.sleep(5);
        }
        throw new AssertionError("coordinator did not persist lease renewal");
    }

    private static Transcript awaitRenewalCount(InMemoryTranscriptRepository repository, int count) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < until) {
            Transcript transcript = repository.load().orElseThrow();
            long renewals = transcript.getEntriesList().stream().filter(TranscriptEntry::hasCoordinatorFrame)
                    .filter(entry -> entry.getCoordinatorFrame().hasRenewal()).count();
            if (renewals >= count) return transcript;
            Thread.sleep(5);
        }
        throw new AssertionError("coordinator did not persist " + count + " renewals");
    }

    private static TranscriptRepository repository(Transcript transcript) {
        return new TranscriptRepository() {
            @Override public Optional<Transcript> load() { return Optional.of(transcript); }
            @Override public void save(Transcript ignored) { throw new UnsupportedOperationException(); }
        };
    }

    private static WorkflowAuthoringDeliverable authored(Workflow workflow) {
        ArtifactReference evidence = artifact(SHA, "application/x-protobuf", 24);
        var deliverable = WorkflowDeliverable.newBuilder().setWorkflow(workflow)
                .setWorkflowArtifact(evidence).setDescriptors(evidence).addFixtures(evidence)
                .addChecks(CheckEvidence.newBuilder().setCheckName(WorkflowAuthoringReviewer.REQUIRED_CHECKS.get(0))
                        .setVerdict(CheckVerdict.CHECK_VERDICT_PASSED)
                        .setRanAt(Timestamp.newBuilder().setSeconds(1)).addArtifacts(evidence))
                .setRunId("admission-fixture-run").setReceipt(evidence).build();
        return WorkflowAuthoringDeliverable.newBuilder().setDeliverable(deliverable)
                .setExecutableSource(evidence.toBuilder().setMediaType("application/json"))
                .addAcceptanceFixtures(WorkflowAcceptanceFixture.newBuilder().setName("input-fixture")
                        .setInput(evidence).setExpectedOutput(evidence)).build();
    }

    private static TaskSpec spec() {
        Map<String, FileDescriptor> closure = new LinkedHashMap<>();
        collect(WorkflowAuthoringDeliverable.getDescriptor().getFile(), closure);
        FileDescriptorSet.Builder descriptorSet = FileDescriptorSet.newBuilder();
        closure.values().forEach(file -> descriptorSet.addFile(file.toProto()));
        ArtifactReference policy = POLICY;
        DeliverableContract contract = DeliverableContract.newBuilder()
                .setTypeName(WorkflowAuthoringDeliverable.getDescriptor().getFullName())
                .setDescriptorSet(descriptorSet.build().toByteString()).build();
        var builder = TaskSpec.newBuilder().setObjective("Prepare a workflow candidate")
                .setContract(contract).addContext(policy);
        WorkflowAuthoringReviewer.REQUIRED_CHECKS.forEach(name -> builder.addRequiredChecks(
                AcceptanceCheck.newBuilder().setName(name).setDescription("verify " + name)));
        return builder.build();
    }

    private static CompletionCandidate candidate(WorkflowAuthoringDeliverable authored) {
        ArtifactReference evidence = artifact(SHA, "application/x-protobuf", 24);
        var builder = CompletionCandidate.newBuilder().setSummary("candidate for admission tests")
                .setResult(Any.pack(authored)).addArtifacts(evidence);
        for (String check : WorkflowAuthoringReviewer.REQUIRED_CHECKS) {
            builder.addEvidence(CheckEvidence.newBuilder().setCheckName(check)
                    .setVerdict(CheckVerdict.CHECK_VERDICT_PASSED)
                    .setRanAt(Timestamp.newBuilder().setSeconds(2)).addArtifacts(evidence));
        }
        return builder.build();
    }

    private static ArtifactReference artifact(String hash, String media, int size) {
        return ArtifactReference.newBuilder().setSha256(hash).setMediaType(media).setSizeBytes(size).build();
    }

    private static void collect(FileDescriptor file, Map<String, FileDescriptor> closure) {
        if (closure.containsKey(file.getName())) return;
        file.getDependencies().forEach(dep -> collect(dep, closure));
        closure.put(file.getName(), file);
    }

    private static String resource(String path) throws IOException {
        try (InputStream input = WorkflowPreparationAdmissionTest.class.getClassLoader().getResourceAsStream(path)) {
            if (input == null) throw new IllegalStateException(path + " not on test classpath");
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static final class Fixture implements AutoCloseable {
        private final InMemoryTranscriptRepository repository = new InMemoryTranscriptRepository();
        private final InProcessDelegationCoordinator coordinator;
        private final Server server;
        private final ManagedChannel channel;
        private final DelegationWorker worker;
        private final CountDownLatch runnerEntered = new CountDownLatch(1);
        private final CountDownLatch releaseRunner = new CountDownLatch(1);
        private final AtomicReference<WorkerRunner.WorkerEvents> events = new AtomicReference<>();
        private final boolean revised;

        private Fixture(TaskSpec spec, CandidateReviewer reviewer, boolean revised) throws Exception {
            this.revised = revised;
            coordinator = new InProcessDelegationCoordinator(AdmissionPolicy.allowAll(), reviewer, CLOCK, repository);
            String name = InProcessServerBuilder.generateName();
            server = InProcessServerBuilder.forName(name).addService(coordinator).build().start();
            channel = InProcessChannelBuilder.forName(name).build();
            WorkerHello hello = WorkerHello.newBuilder().setWorkerId(HOLDER).setProtocolVersion(1)
                    .setProvider("scripted").setModel("admission-fixture")
                    .addCapabilities(WorkerCapability.newBuilder().setName("workflow-authoring")).build();
            List<ScriptedWorkerRunner.Step> steps = revised
                    ? List.of((task, workerEvents) -> result(task), (task, workerEvents) -> {
                        events.set(workerEvents);
                        runnerEntered.countDown();
                        if (!releaseRunner.await(10, TimeUnit.SECONDS)) throw new AssertionError("runner wait expired");
                        return result(task);
                    })
                    : List.of((task, workerEvents) -> {
                        events.set(workerEvents);
                        runnerEntered.countDown();
                        if (!releaseRunner.await(10, TimeUnit.SECONDS)) throw new AssertionError("runner wait expired");
                        return result(task);
                    });
            worker = new DelegationWorker(ai.protomolt.proto.delegation.v1.AgentDelegationServiceGrpc
                    .newStub(channel), new ScriptedWorkerRunner(hello, steps));
            worker.start();
            if (!worker.awaitAdmission(java.time.Duration.ofSeconds(10))) {
                throw new AssertionError("scripted worker was not admitted");
            }
            coordinator.offer(HOLDER, TASK_ID, spec, java.time.Duration.ofSeconds(30));
            if (!runnerEntered.await(10, TimeUnit.SECONDS)) throw new AssertionError("worker did not start task");
            if (!revised) awaitPhase(DelegationReducer.Phase.LEASED);
            else awaitRevisionLease();
        }

        static Fixture start(TaskSpec spec, CandidateReviewer reviewer,
                boolean revised, boolean unused) throws Exception {
            return new Fixture(spec, reviewer, revised);
        }

        InProcessDelegationCoordinator coordinator() { return coordinator; }
        InMemoryTranscriptRepository repository() { return repository; }
        AtomicReference<WorkerRunner.WorkerEvents> events() { return events; }
        Transcript transcript() { return repository.load().orElseThrow(); }

        void awaitPhase(DelegationReducer.Phase phase) throws Exception {
            long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (System.nanoTime() < limit) {
                var state = coordinator.state().tasks().get(TASK_ID);
                if (state != null && state.phase() == phase) return;
                Thread.sleep(5);
            }
            throw new AssertionError("task did not reach " + phase);
        }

        void awaitRevisionLease() throws Exception {
            long limit = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (System.nanoTime() < limit) {
                var state = coordinator.state().tasks().get(TASK_ID);
                if (state != null && state.phase() == DelegationReducer.Phase.LEASED) return;
                Thread.sleep(5);
            }
            throw new AssertionError("revised task did not return to leased phase");
        }

        void awaitTerminalOrCandidate(DelegationReducer.Phase phase) throws Exception { awaitPhase(phase); }
        void release() { releaseRunner.countDown(); }

        @Override public void close() throws Exception {
            releaseRunner.countDown();
            worker.close();
            channel.shutdownNow();
            channel.awaitTermination(5, TimeUnit.SECONDS);
            coordinator.close();
            server.shutdownNow();
            server.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static CompletionCandidate result(WorkerRunner.WorkerTask task) {
        return CANDIDATE.toBuilder().setAttempt(task.offer().getAttempt())
                .setRevision(task.expectedRevision()).build();
    }
}
