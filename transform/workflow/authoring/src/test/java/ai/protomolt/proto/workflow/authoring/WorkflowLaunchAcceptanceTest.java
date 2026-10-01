package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.delegation.AdmissionPolicy;
import ai.protomolt.proto.delegation.CandidateReviewer;
import ai.protomolt.proto.delegation.DelegationReducer;
import ai.protomolt.proto.delegation.DelegationWorker;
import ai.protomolt.proto.delegation.InMemoryTranscriptRepository;
import ai.protomolt.proto.delegation.InProcessDelegationCoordinator;
import ai.protomolt.proto.delegation.ScriptedWorkerRunner;
import ai.protomolt.proto.delegation.TranscriptRepository;
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
import ai.protomolt.proto.grpc.workflow.v1.VersionedWorkflow;
import ai.protomolt.proto.grpc.workflow.v1.Workflow;
import ai.protomolt.proto.receipt.WorkRecords;
import ai.protomolt.proto.samples.starter.v1.WorkflowAcceptanceFixture;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringDeliverable;
import ai.protomolt.proto.samples.starter.v1.WorkflowDeliverable;
import ai.protomolt.proto.sources.CompiledProtos;
import ai.protomolt.proto.sources.ProtoSourceCompiler;
import ai.protomolt.proto.sources.ProtoSourceSet;
import ai.protomolt.proto.workflow.CompiledWorkflow;
import ai.protomolt.proto.workflow.WorkflowCompiler;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Timestamp;
import com.google.protobuf.UnknownFieldSet;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** A launch identity can only be derived from clean durable accepted transcript evidence. */
class WorkflowLaunchAcceptanceTest {
    private static final String VALIDATE = "ai/protomolt/proto/validate/v1/validate.proto";
    private static final String PROTO = """
            syntax = "proto3";
            package launch.acceptance.fixture;
            message Input { string name = 1; }
            message Result { string text = 1; }
            service Echo { rpc Run(Input) returns (Result); }
            """;
    private static final String CHECK = "launch-contract";
    private static final String SHA = "a".repeat(64);
    private static final String WORKER_ID = "launch-fixture-worker";
    private static final String TASK_ID = "00000000-0000-4000-8000-000000000001";
    private static final Instant SENT_AT = Instant.parse("2026-09-30T12:34:56.123456789Z");
    private static final Clock CLOCK = Clock.fixed(SENT_AT, ZoneOffset.UTC);

    private static Workflow workflow;
    private static WorkflowAuthoringDeliverable authored;
    private static TaskSpec spec;
    private static CompletionCandidate candidate;

    @BeforeAll static void fixtures() throws Exception {
        CompiledProtos compiled = new ProtoSourceCompiler().compile(ProtoSourceSet.builder()
                .add(VALIDATE, resource(VALIDATE), "test")
                .add("launch/acceptance.proto", PROTO, "test").build());
        FileDescriptor file = compiled.descriptorFor("launch/acceptance.proto").orElseThrow();
        var files = List.of(file);
        workflow = WorkflowCompiler.compile(new CompiledWorkflow("launch-fixture", files,
                file.findMessageTypeByName("Input"), 30_000,
                List.of(CompiledWorkflow.Step.grpc("echo", "fixture:9090", false,
                        CompiledWorkflow.resolveMethod(files,
                                "launch.acceptance.fixture.Echo/Run"), null,
                        List.of("name = input.name"), List.of(), false, 0, "")),
                null, true));
        authored = authored(workflow);
        spec = taskSpec();
        candidate = candidate(authored);
    }

    private static String resource(String path) throws IOException {
        try (InputStream in = WorkflowLaunchAcceptanceTest.class.getClassLoader()
                .getResourceAsStream(path)) {
            if (in == null) throw new IllegalStateException(path + " not on the test classpath");
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static ArtifactReference artifact(String hash) {
        return ArtifactReference.newBuilder().setSha256(hash).setMediaType("application/x-protobuf")
                .setSizeBytes(24).build();
    }

    private static WorkflowAuthoringDeliverable authored(Workflow durable) {
        var evidenceArtifact = artifact(SHA);
        var deliverable = WorkflowDeliverable.newBuilder().setWorkflow(durable)
                .setWorkflowArtifact(evidenceArtifact).setDescriptors(evidenceArtifact)
                .addFixtures(evidenceArtifact)
                .addChecks(CheckEvidence.newBuilder().setCheckName(CHECK)
                        .setVerdict(CheckVerdict.CHECK_VERDICT_PASSED)
                        .setRanAt(Timestamp.newBuilder().setSeconds(1))
                        .addArtifacts(evidenceArtifact))
                .setRunId("launch-fixture-run").setReceipt(evidenceArtifact).build();
        return WorkflowAuthoringDeliverable.newBuilder().setDeliverable(deliverable)
                .setExecutableSource(evidenceArtifact.toBuilder()
                        .setMediaType("application/json"))
                .addAcceptanceFixtures(WorkflowAcceptanceFixture.newBuilder()
                        .setName("default-fixture").setInput(evidenceArtifact)
                        .setExpectedOutput(evidenceArtifact)).build();
    }

    private static TaskSpec taskSpec() {
        Map<String, FileDescriptor> closure = new LinkedHashMap<>();
        collect(WorkflowAuthoringDeliverable.getDescriptor().getFile(), closure);
        FileDescriptorSet.Builder set = FileDescriptorSet.newBuilder();
        closure.values().forEach(file -> set.addFile(file.toProto()));
        DeliverableContract contract = DeliverableContract.newBuilder()
                .setTypeName(WorkflowAuthoringDeliverable.getDescriptor().getFullName())
                .setDescriptorSet(set.build().toByteString()).build();
        return TaskSpec.newBuilder().setObjective("Produce an accepted workflow deliverable")
                .addRequiredChecks(AcceptanceCheck.newBuilder().setName(CHECK)
                        .setDescription("validate the deliverable"))
                .setContract(contract).build();
    }

    private static CompletionCandidate candidate(WorkflowAuthoringDeliverable result) {
        var ref = artifact(SHA);
        return CompletionCandidate.newBuilder().setAttempt(1).setRevision(1)
                .setSummary("workflow deliverable accepted")
                .addEvidence(CheckEvidence.newBuilder().setCheckName(CHECK)
                        .setVerdict(CheckVerdict.CHECK_VERDICT_PASSED)
                        .setRanAt(Timestamp.newBuilder().setSeconds(2)).addArtifacts(ref))
                .addArtifacts(ref).setResult(Any.pack(result)).build();
    }

    private static void collect(FileDescriptor file, Map<String, FileDescriptor> closure) {
        if (closure.containsKey(file.getName())) return;
        file.getDependencies().forEach(dep -> collect(dep, closure));
        closure.put(file.getName(), file);
    }

    private static AcceptedRun transcript(CandidateReviewer reviewer, CompletionCandidate value,
                                          int revision, boolean cancelAfterCandidate)
            throws Exception {
        return transcript(spec, reviewer, value, revision, cancelAfterCandidate, false);
    }

    private static AcceptedRun acceptedTranscript(CandidateReviewer reviewer,
                                                  CompletionCandidate value) throws Exception {
        return transcript(spec, reviewer, value, 1, false, true);
    }

    private static AcceptedRun transcript(TaskSpec offerSpec, CandidateReviewer reviewer,
                                          CompletionCandidate value, int revision,
                                          boolean cancelAfterCandidate,
                                          boolean expectAccepted) throws Exception {
        String taskId = TASK_ID;
        InMemoryTranscriptRepository repository = new InMemoryTranscriptRepository();
        try (InProcessDelegationCoordinator coordinator = new InProcessDelegationCoordinator(
                AdmissionPolicy.allowAll(), reviewer, CLOCK, repository)) {
            String name = InProcessServerBuilder.generateName();
            Server server = InProcessServerBuilder.forName(name)
                    .addService(coordinator).build().start();
            ManagedChannel channel = InProcessChannelBuilder.forName(name).build();
            WorkerHello hello = WorkerHello.newBuilder().setWorkerId(WORKER_ID)
                    .setProtocolVersion(1).setProvider("scripted").setModel("launch-fixture")
                    .addCapabilities(WorkerCapability.newBuilder().setName("workflow-authoring"))
                    .build();
            ScriptedWorkerRunner scripted = new ScriptedWorkerRunner(hello, List.of((task, events) -> {
                return value.toBuilder().setAttempt(task.offer().getAttempt())
                        .setRevision(task.expectedRevision()).build();
            }, (task, events) -> value.toBuilder().setAttempt(task.offer().getAttempt())
                    .setRevision(task.expectedRevision()).build()));
            try (DelegationWorker worker = new DelegationWorker(
                    ai.protomolt.proto.delegation.v1.AgentDelegationServiceGrpc
                            .newStub(channel), scripted)) {
                worker.start();
                assertThat(worker.awaitAdmission(Duration.ofSeconds(10))).isTrue();
                coordinator.offer(WORKER_ID, taskId, offerSpec, Duration.ofSeconds(30));
                awaitCandidate(coordinator, taskId, revision, expectAccepted);
                if (cancelAfterCandidate) coordinator.cancel(taskId, "launch test cancellation");
                var transcript = repository.load().orElseThrow();
                return new AcceptedRun(taskId, transcript, repository);
            } finally {
                channel.shutdownNow();
                server.shutdownNow();
            }
        }
    }

    private static void awaitCandidate(InProcessDelegationCoordinator coordinator,
                                       String taskId, int revision, boolean expectAccepted)
            throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (System.nanoTime() < deadline) {
            var task = coordinator.state().tasks().get(taskId);
            if (task != null && task.candidateRevision() == revision
                    && task.phase() == (expectAccepted ? DelegationReducer.Phase.ACCEPTED
                            : DelegationReducer.Phase.CANDIDATE)) return;
            Thread.sleep(10);
        }
        throw new AssertionError("candidate revision " + revision + " was not reached");
    }

    private static TranscriptRepository repository(Transcript value) {
        return new TranscriptRepository() {
            @Override public Optional<Transcript> load() { return Optional.of(value); }
            @Override public void save(Transcript ignored) { throw new UnsupportedOperationException(); }
        };
    }

    private record AcceptedRun(String taskId, Transcript transcript,
                               InMemoryTranscriptRepository repository) {
    }

    @Test void cleanAcceptedTranscriptYieldsHashesContextAuthoredResultAndAcceptedTime()
            throws Exception {
        AcceptedRun run = acceptedTranscript(CandidateReviewer.acceptAll(), candidate);
        var proof = WorkflowLaunchAcceptance.inspect(run.repository(), run.taskId());
        TranscriptEntry accepted = run.transcript().getEntriesList().stream()
                .filter(e -> e.hasCoordinatorFrame()
                        && e.getCoordinatorFrame().hasAccepted())
                .findFirst().orElseThrow();
        var identity = proof.identity();
        var completion = run.transcript().getEntriesList().stream()
                .filter(e -> e.hasWorkerFrame() && e.getWorkerFrame().hasCompletion())
                .map(e -> e.getWorkerFrame().getCompletion()).findFirst().orElseThrow();
        var offer = run.transcript().getEntriesList().stream()
                .filter(e -> e.hasCoordinatorFrame() && e.getCoordinatorFrame().hasOffer())
                .map(e -> e.getCoordinatorFrame().getOffer()).findFirst().orElseThrow();

        assertThat(identity.getTaskId()).isEqualTo(run.taskId());
        assertThat(identity.getAttempt()).isEqualTo(1);
        assertThat(identity.getRevision()).isEqualTo(1);
        assertThat(identity.getTaskSpecSha256()).isEqualTo(WorkflowLaunchValidation.sha256(offer.getSpec()));
        assertThat(identity.getCandidateSha256()).isEqualTo(WorkflowLaunchValidation.sha256(completion));
        assertThat(identity.getAcceptedEntrySha256()).isEqualTo(WorkflowLaunchValidation.sha256(accepted));
        assertThat(proof.context().taskId()).isEqualTo(run.taskId());
        assertThat(proof.context().workerId()).isEqualTo(WORKER_ID);
        assertThat(proof.context().spec()).isEqualTo(offer.getSpec());
        assertThat(proof.context().candidate()).isEqualTo(completion);
        assertThat(proof.authored()).isEqualTo(authored);
        assertThat(proof.acceptedAt()).isEqualTo(accepted.getCoordinatorFrame().getSentAt());
    }

    @Test void missingPendingRevisedCancelledAndWrongTaskTranscriptsAreRejected() throws Exception {
        assertThatThrownBy(() -> WorkflowLaunchAcceptance.inspect(
                new InMemoryTranscriptRepository(), TASK_ID)).isInstanceOf(IllegalArgumentException.class);

        AcceptedRun pending = transcript(CandidateReviewer.manual(), candidate, 1, false);
        assertThatThrownBy(() -> WorkflowLaunchAcceptance.inspect(pending.repository(), pending.taskId()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("not durably accepted");

        AtomicInteger reviews = new AtomicInteger();
        CandidateReviewer reviseThenWait = context -> reviews.incrementAndGet() == 1
                ? CandidateReviewer.ReviewDecision.revise("revise once", List.of(CHECK))
                : CandidateReviewer.ReviewDecision.pending();
        AcceptedRun revised = transcript(reviseThenWait, candidate, 2, false);
        assertThatThrownBy(() -> WorkflowLaunchAcceptance.inspect(revised.repository(), revised.taskId()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("not durably accepted");

        AcceptedRun cancelled = transcript(CandidateReviewer.manual(), candidate, 1, true);
        assertThatThrownBy(() -> WorkflowLaunchAcceptance.inspect(cancelled.repository(), cancelled.taskId()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("not durably accepted");
        assertThatThrownBy(() -> WorkflowLaunchAcceptance.inspect(pending.repository(),
                "00000000-0000-4000-8000-000000000099"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test void unknownTypedResultAndSelectedUnknownTranscriptFieldsAreRejected() throws Exception {
        AcceptedRun accepted = acceptedTranscript(CandidateReviewer.acceptAll(), candidate);
        int candidateIndex = -1;
        int offerIndex = -1;
        int acceptedIndex = -1;
        for (int i = 0; i < accepted.transcript().getEntriesCount(); i++) {
            var entry = accepted.transcript().getEntries(i);
            if (entry.hasWorkerFrame() && entry.getWorkerFrame().hasCompletion()) candidateIndex = i;
            if (entry.hasCoordinatorFrame() && entry.getCoordinatorFrame().hasOffer()) offerIndex = i;
            if (entry.hasCoordinatorFrame() && entry.getCoordinatorFrame().hasAccepted()) acceptedIndex = i;
        }
        var originalCandidateEntry = accepted.transcript().getEntries(candidateIndex);
        var unknownResult = originalCandidateEntry.getWorkerFrame().getCompletion().toBuilder()
                .setResult(Any.newBuilder().setTypeUrl("type.googleapis.com/launch.Missing")
                        .setValue(ByteString.copyFromUtf8("{}"))).build();
        Transcript unknownResultTranscript = accepted.transcript().toBuilder()
                .setEntries(candidateIndex, originalCandidateEntry.toBuilder()
                        .setWorkerFrame(originalCandidateEntry.getWorkerFrame().toBuilder()
                                .setCompletion(unknownResult)))
                .build();
        assertThatThrownBy(() -> WorkflowLaunchAcceptance.inspect(
                repository(unknownResultTranscript), accepted.taskId()))
                .isInstanceOf(IllegalArgumentException.class);

        var acceptedEntry = accepted.transcript().getEntries(acceptedIndex);
        var unknownEntry = acceptedEntry.toBuilder().setUnknownFields(
                UnknownFieldSet.newBuilder().addField(99,
                        UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build()).build();
        Transcript unknownSelectedEntry = accepted.transcript().toBuilder()
                .setEntries(acceptedIndex, unknownEntry).build();
        assertThatThrownBy(() -> WorkflowLaunchAcceptance.inspect(
                repository(unknownSelectedEntry), accepted.taskId()))
                .isInstanceOf(IllegalArgumentException.class);

        var unknown = UnknownFieldSet.newBuilder().addField(99,
                UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build();
        var offerEntry = accepted.transcript().getEntries(offerIndex);
        var unknownOffer = accepted.transcript().toBuilder().setEntries(offerIndex,
                offerEntry.toBuilder().setCoordinatorFrame(offerEntry.getCoordinatorFrame().toBuilder()
                        .setOffer(offerEntry.getCoordinatorFrame().getOffer().toBuilder()
                                .setUnknownFields(unknown)))).build();
        assertThatThrownBy(() -> WorkflowLaunchAcceptance.inspect(repository(unknownOffer), accepted.taskId()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("unknown fields");

        var unknownCandidateEnvelope = accepted.transcript().toBuilder().setEntries(candidateIndex,
                originalCandidateEntry.toBuilder().setWorkerFrame(
                        originalCandidateEntry.getWorkerFrame().toBuilder().setUnknownFields(unknown))).build();
        assertThatThrownBy(() -> WorkflowLaunchAcceptance.inspect(
                repository(unknownCandidateEnvelope), accepted.taskId()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("unknown fields");
    }

    @Test void selectedIdentityHashesTrackOnlyTheirSelectedMessages() throws Exception {
        AcceptedRun first = acceptedTranscript(CandidateReviewer.acceptAll(), candidate);
        CompletionCandidate altered = candidate.toBuilder().setSummary("different candidate").build();
        AcceptedRun second = acceptedTranscript(CandidateReviewer.acceptAll(), altered);
        var firstProof = WorkflowLaunchAcceptance.inspect(first.repository(), first.taskId());
        var secondProof = WorkflowLaunchAcceptance.inspect(second.repository(), second.taskId());

        assertThat(firstProof.identity().getTaskSpecSha256())
                .isEqualTo(secondProof.identity().getTaskSpecSha256());
        assertThat(firstProof.identity().getCandidateSha256())
                .isNotEqualTo(secondProof.identity().getCandidateSha256());
        assertThat(firstProof.identity().getAcceptedEntrySha256()).isNotBlank();
        assertThat(secondProof.identity().getAcceptedEntrySha256()).isNotBlank();

        TaskSpec changedOffer = spec.toBuilder().setObjective("A distinct accepted objective").build();
        AcceptedRun third = transcript(changedOffer, CandidateReviewer.acceptAll(), candidate,
                1, false, true);
        var thirdProof = WorkflowLaunchAcceptance.inspect(third.repository(), third.taskId());
        assertThat(firstProof.identity().getTaskSpecSha256())
                .isNotEqualTo(thirdProof.identity().getTaskSpecSha256());
        assertThat(firstProof.identity().getCandidateSha256())
                .isEqualTo(thirdProof.identity().getCandidateSha256());
    }
}
