package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.delegation.AdmissionPolicy;
import ai.protomolt.proto.delegation.CandidateReviewer;
import ai.protomolt.proto.delegation.DelegationReducer;
import ai.protomolt.proto.delegation.DelegationWorker;
import ai.protomolt.proto.delegation.InMemoryTranscriptRepository;
import ai.protomolt.proto.delegation.InProcessDelegationCoordinator;
import ai.protomolt.proto.delegation.ScriptedWorkerRunner;
import ai.protomolt.proto.delegation.TranscriptRepository;
import ai.protomolt.proto.delegation.WorkerRunner;
import ai.protomolt.proto.delegation.v1.AgentDelegationServiceGrpc;
import ai.protomolt.proto.delegation.v1.AcceptanceCheck;
import ai.protomolt.proto.delegation.v1.CheckEvidence;
import ai.protomolt.proto.delegation.v1.CheckVerdict;
import ai.protomolt.proto.delegation.v1.CompletionCandidate;
import ai.protomolt.proto.delegation.v1.DelegateRequest;
import ai.protomolt.proto.delegation.v1.DeliverableContract;
import ai.protomolt.proto.delegation.v1.TaskSpec;
import ai.protomolt.proto.delegation.v1.Transcript;
import ai.protomolt.proto.delegation.v1.TranscriptEntry;
import ai.protomolt.proto.delegation.v1.WorkerCapability;
import ai.protomolt.proto.delegation.v1.WorkerHello;
import ai.protomolt.proto.grpc.workflow.ArtifactRepository;
import ai.protomolt.proto.grpc.workflow.FileSystemArtifactRepository;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.receipt.WorkRecords;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringDeliverable;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringPolicy;
import ai.protomolt.proto.samples.starter.v1.WorkflowAcceptanceFixture;
import ai.protomolt.proto.samples.starter.v1.WorkflowPermittedCall;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowAuthorContextRequest;
import ai.protomolt.proto.workflow.authoring.v1.ReadWorkflowAuthorEventsRequest;
import ai.protomolt.proto.workflow.authoring.v1.ReadWorkflowAuthorEventsResponse;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.protobuf.UnknownFieldSet;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Adversarial read-boundary tests over transcripts emitted by the real coordinator. */
class WorkflowAuthorTaskReaderAdversarialTest {
    private static final String TASK = "00000000-0000-4000-8000-000000000501";
    private static final String AUTHOR = "author-one";
    private static final String SECOND = "author-two";
    private static final ArtifactReference POLICY = ArtifactReference.newBuilder()
            .setSha256("a".repeat(64)).setMediaType("application/x-protobuf").setSizeBytes(32).build();
    private static final Instant NOW = Instant.parse("2026-10-01T10:00:00Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);
    private static final TaskSpec SPEC = authorSpec();

    @TempDir Path temp;

    @Test
    void oldHolderCanReadOriginalAttemptAfterReassignmentButNewHolderCannotReadIt() throws Exception {
        try (Fixture fixture = new Fixture(AUTHOR, SECOND, 4)) {
            fixture.offer(AUTHOR, TASK);
            fixture.awaitPhase(TASK, DelegationReducer.Phase.LEASED, 1);
            fixture.coordinator.cancel(TASK, "close first author's attempt");
            fixture.awaitPhase(TASK, DelegationReducer.Phase.CANCELLED, 1);
            fixture.offer(SECOND, TASK);
            fixture.awaitPhase(TASK, DelegationReducer.Phase.LEASED, 2);

            var historical = reader(fixture.repo).events(request(TASK, 1, 0, 64), author(AUTHOR));
            assertThat(historical.getEventsList()).isNotEmpty();
            assertThat(historical.getEventsList()).allSatisfy(event -> {
                assertThat(event.getWorkerId()).isEqualTo(AUTHOR);
                assertThat(event.getTaskId()).isEqualTo(TASK);
                assertThat(attempt(event.getEntry())).isEqualTo(1);
            });
            assertThatThrownBy(() -> reader(fixture.repo).events(request(TASK, 1, 0, 64), author(SECOND)))
                    .isInstanceOfSatisfying(WorkflowPreparationException.class, error ->
                            assertThat(error.kind()).isEqualTo(WorkflowPreparationException.Kind.PERMISSION_DENIED));
            assertThatThrownBy(() -> reader(fixture.repo).events(request(TASK, 2, 0, 64), author(AUTHOR)))
                    .isInstanceOfSatisfying(WorkflowPreparationException.class, error ->
                            assertThat(error.kind()).isEqualTo(WorkflowPreparationException.Kind.PERMISSION_DENIED));
        }
    }

    @Test
    void taskMessagesAreAttemptZeroAndBatchLimitDoesNotSkipADeferredAuthorizedEvent() throws Exception {
        try (Fixture fixture = new Fixture(AUTHOR, null, 4)) {
            fixture.offer(AUTHOR, TASK);
            fixture.awaitPhase(TASK, DelegationReducer.Phase.LEASED, 1);
            fixture.coordinator.sendMessage(AUTHOR, TASK,
                    ai.protomolt.proto.delegation.v1.TaskMessageKind.TASK_MESSAGE_KIND_NOTE,
                    "attempt-zero message", "", List.of());
            var workerEvents = fixture.eventsFor(TASK);
            int beforeProgress = fixture.transcript().getEntriesCount();
            workerEvents.progress("authorized attempt-one progress");
            fixture.awaitEntries(beforeProgress + 1);

            WorkflowAuthorTaskReader reader = reader(fixture.repo);
            var first = reader.events(request(TASK, 1, 0, 1), author(AUTHOR));
            assertThat(first.getEventsCount()).isEqualTo(1);
            assertThat(first.getEvents(0).getEntry().getCoordinatorFrame().hasOffer()).isTrue();
            assertThat(first.getCursor()).isEqualTo(first.getEvents(0).getCursor());
            assertThat(first.getTruncated()).isTrue();

            var second = reader.events(request(TASK, 1, first.getCursor(), 1), author(AUTHOR));
            assertThat(second.getEventsCount()).isEqualTo(1);
            assertThat(second.getEvents(0).getEntry().getWorkerFrame().hasAccept()).isTrue();
            long progressCursor = fixture.cursorOfAttemptOneProgress();
            assertThat(second.getCursor()).isLessThan(progressCursor);

            // The second page can advance over the attempt-0 message, but it must stop before
            // the next authorized event. That next page returns the progress frame.
            var third = reader.events(request(TASK, 1, second.getCursor(), 1), author(AUTHOR));
            assertThat(third.getEventsList()).anySatisfy(event ->
                    assertThat(event.getEntry().hasWorkerFrame()
                            && event.getEntry().getWorkerFrame().hasProgress()).isTrue());
            assertThat(third.getEventsList()).noneMatch(event ->
                    event.getEntry().hasCoordinatorFrame()
                            && event.getEntry().getCoordinatorFrame().hasTaskMessage());
        }
    }

    @Test
    void scanWatermarkAdvancesAcrossMoreThan256UnrelatedEntriesAndEventuallyReturnsOwnEvent()
            throws Exception {
        final int unrelatedMessages = 270;
        try (Fixture fixture = new Fixture(AUTHOR, null, unrelatedMessages + 4)) {
            fixture.offer(AUTHOR, TASK);
            fixture.awaitPhase(TASK, DelegationReducer.Phase.LEASED, 1);
            String otherTask = "00000000-0000-4000-8000-000000000777";
            fixture.offer(AUTHOR, otherTask);
            for (int i = 0; i < unrelatedMessages; i++) {
                fixture.coordinator.sendMessage(AUTHOR, otherTask,
                        ai.protomolt.proto.delegation.v1.TaskMessageKind.TASK_MESSAGE_KIND_NOTE,
                        "unrelated message " + i, "", List.of());
            }
            int before = fixture.transcript().getEntriesCount();
            fixture.eventsFor(TASK).progress("late authorized attempt event");
            fixture.awaitEntries(before + 1);

            WorkflowAuthorTaskReader reader = reader(fixture.repo);
            long cursor = fixture.cursorOfAttemptOneAccept();
            var first = reader.events(request(TASK, 1, cursor, 64), author(AUTHOR));
            assertThat(first.getEventsCount()).isZero();
            assertThat(first.getCursor()).isGreaterThan(cursor);
            assertThat(first.getCursor()).isLessThanOrEqualTo(cursor + 256);
            assertThat(first.getTruncated()).isTrue();
            cursor = first.getCursor();

            ReadWorkflowAuthorEventsResponse page = first;
            int scans = 1;
            while (page.getEventsList().stream().noneMatch(event ->
                    event.getEntry().hasWorkerFrame() && event.getEntry().getWorkerFrame().hasProgress())
                    && page.getTruncated() && scans < 6) {
                page = reader.events(request(TASK, 1, cursor, 64), author(AUTHOR));
                assertThat(page.getCursor()).isGreaterThan(cursor);
                assertThat(page.getCursor()).isLessThanOrEqualTo(cursor + 256);
                cursor = page.getCursor();
                scans++;
            }
            assertThat(page.getEventsList()).anySatisfy(event ->
                    assertThat(event.getEntry().hasWorkerFrame()
                            && event.getEntry().getWorkerFrame().hasProgress()).isTrue());
            assertThat(scans).isGreaterThan(1);
        }
    }

    @Test
    void unknownFieldsAndCorruptSequenceInTrustedTranscriptFailClosed() throws Exception {
        try (Fixture fixture = new Fixture(AUTHOR, null, 4)) {
            fixture.offer(AUTHOR, TASK);
            fixture.awaitPhase(TASK, DelegationReducer.Phase.LEASED, 1);
            Transcript valid = fixture.transcript();
            TranscriptEntry offer = valid.getEntriesList().stream()
                    .filter(TranscriptEntry::hasCoordinatorFrame)
                    .filter(entry -> entry.getCoordinatorFrame().hasOffer()).findFirst().orElseThrow();
            int offerPosition = valid.getEntriesList().indexOf(offer);
            Transcript withUnknown = valid.toBuilder().setEntries(offerPosition,
                    offer.toBuilder().setUnknownFields(UnknownFieldSet.newBuilder().addField(99,
                            UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build())).build();
            fixture.repo.replace(withUnknown);
            assertCorrupt(reader(fixture.repo), request(TASK, 1, 0, 64));

            TranscriptEntry accepted = valid.getEntriesList().stream()
                    .filter(TranscriptEntry::hasWorkerFrame)
                    .filter(entry -> entry.getWorkerFrame().hasAccept()).findFirst().orElseThrow();
            int acceptedPosition = valid.getEntriesList().indexOf(accepted);
            Transcript corruptSequence = valid.toBuilder().setEntries(acceptedPosition,
                    accepted.toBuilder().setWorkerFrame(accepted.getWorkerFrame().toBuilder().setSeq(99))).build();
            fixture.repo.replace(corruptSequence);
            assertCorrupt(reader(fixture.repo), request(TASK, 1, 0, 64));
        }
    }

    @Test
    void contextRefusesPinnedDescriptorWithUnresolvedType() throws Exception {
        ArtifactRepository artifacts = new FileSystemArtifactRepository(temp.resolve("context-artifacts"));
        var brokenFile = FileDescriptorProto.newBuilder().setName("broken.proto")
                .setPackage("author.reader")
                .addMessageType(com.google.protobuf.DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("Input").addField(
                                com.google.protobuf.DescriptorProtos.FieldDescriptorProto.newBuilder()
                                        .setName("missing").setNumber(1)
                                        .setType(com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE)
                                        .setTypeName(".author.reader.DoesNotExist"))).build();
        byte[] brokenSet = FileDescriptorSet.newBuilder().addFile(brokenFile).build().toByteArray();
        ArtifactReference descriptors = artifacts.save(brokenSet, "application/x-protobuf", false);
        var policy = WorkflowAuthoringPolicy.newBuilder().setDescriptors(descriptors)
                .addFixtures(WorkflowAcceptanceFixture.newBuilder().setName("fixture-valid")
                        .setInput(POLICY).setExpectedOutput(POLICY))
                .addPermittedCalls(WorkflowPermittedCall.newBuilder()
                        .setTarget("fixture:9090").setMethod("author.reader.Echo/Run"))
                .build();
        ArtifactReference policyReference = artifacts.save(policy.toByteArray(),
                "application/x-protobuf", false);
        TaskSpec pinned = SPEC.toBuilder().clearContext().addContext(policyReference).build();
        try (Fixture fixture = new Fixture(AUTHOR, null, 4)) {
            fixture.offer(AUTHOR, TASK, pinned);
            fixture.awaitPhase(TASK, DelegationReducer.Phase.LEASED, 1);
            var reader = new WorkflowAuthorTaskReader(fixture.repo, artifacts, policyReference, CLOCK);
            var request = GetWorkflowAuthorContextRequest.newBuilder().setTaskId(TASK)
                    .setAttempt(1).build();
            assertThatThrownBy(() -> reader.context(request, author(AUTHOR)))
                    .isInstanceOfSatisfying(WorkflowPreparationException.class, error ->
                            assertThat(error.kind()).isEqualTo(
                                    WorkflowPreparationException.Kind.CORRUPT_EVIDENCE));
        }
    }

    @Test
    void eventReadRejectsCandidateAllowedByWeakerOfferButInvalidForNativeDeliverable() throws Exception {
        var fake = FileDescriptorProto.newBuilder().setName("weak-author.proto")
                .setPackage("ai.protomolt.proto.samples.starter.v1")
                .addMessageType(com.google.protobuf.DescriptorProtos.DescriptorProto.newBuilder()
                        .setName("WorkflowAuthoringDeliverable")).build();
        TaskSpec weak = SPEC.toBuilder().setContract(DeliverableContract.newBuilder()
                .setTypeName(WorkflowAuthoringDeliverable.getDescriptor().getFullName())
                .setDescriptorSet(FileDescriptorSet.newBuilder().addFile(fake).build().toByteString()))
                .build();
        try (Fixture fixture = new Fixture(AUTHOR, null, 4)) {
            fixture.offer(AUTHOR, TASK, weak);
            fixture.awaitPhase(TASK, DelegationReducer.Phase.LEASED, 1);
            Transcript transcript = fixture.transcript();
            long nextSeq = transcript.getEntriesList().stream().filter(TranscriptEntry::hasWorkerFrame)
                    .filter(entry -> entry.getWorkerId().equals(AUTHOR)
                            && entry.getWorkerFrame().getTaskId().equals(TASK))
                    .mapToLong(entry -> entry.getWorkerFrame().getSeq()).max().orElse(0) + 1;
            CompletionCandidate.Builder candidate = CompletionCandidate.newBuilder()
                    .setAttempt(1).setRevision(1).setSummary("claimed completion")
                    .addArtifacts(POLICY)
                    .setResult(Any.newBuilder().setTypeUrl("type.googleapis.com/"
                            + WorkflowAuthoringDeliverable.getDescriptor().getFullName())
                            .setValue(ByteString.EMPTY));
            for (String check : WorkflowAuthoringReviewer.REQUIRED_CHECKS) {
                candidate.addEvidence(CheckEvidence.newBuilder().setCheckName(check)
                        .setVerdict(CheckVerdict.CHECK_VERDICT_PASSED)
                        .setRanAt(Timestamp.newBuilder().setSeconds(NOW.getEpochSecond()))
                        .addArtifacts(POLICY));
            }
            var frame = DelegateRequest.newBuilder().setFrameId(UUID.randomUUID().toString())
                    .setTaskId(TASK).setSeq(nextSeq)
                    .setSentAt(Timestamp.newBuilder().setSeconds(NOW.getEpochSecond()))
                    .setCompletion(candidate).build();
            Transcript altered = transcript.toBuilder().addEntries(TranscriptEntry.newBuilder()
                    .setWorkerId(AUTHOR)
                    .setLane(ai.protomolt.proto.delegation.v1.Lane.LANE_WORKER)
                    .setWorkerFrame(frame)).build();
            assertThat(new DelegationReducer().reduce(altered).clean()).isTrue();
            fixture.repo.replace(altered);
            assertCorrupt(reader(fixture.repo), request(TASK, 1, 0, 64));
        }
    }

    private WorkflowAuthorTaskReader reader(TranscriptRepository repo) throws IOException {
        ArtifactRepository artifacts = new FileSystemArtifactRepository(temp.resolve("artifacts"));
        return new WorkflowAuthorTaskReader(repo, artifacts, POLICY, CLOCK);
    }

    private static ReadWorkflowAuthorEventsRequest request(String task, int attempt, long cursor, int max) {
        return ReadWorkflowAuthorEventsRequest.newBuilder().setTaskId(task).setAttempt(attempt)
                .setAfterCursor(cursor).setMaxEvents(max).build();
    }

    private static Caller author(String name) {
        return Caller.scoped(name, Set.of(Scopes.WORKFLOW_AUTHOR));
    }

    private static void assertCorrupt(WorkflowAuthorTaskReader reader,
            ReadWorkflowAuthorEventsRequest request) {
        assertThatThrownBy(() -> reader.events(request, author(AUTHOR)))
                .isInstanceOfSatisfying(WorkflowPreparationException.class, error ->
                        assertThat(error.kind()).isEqualTo(WorkflowPreparationException.Kind.CORRUPT_EVIDENCE));
    }

    private static int attempt(TranscriptEntry entry) {
        if (entry.hasWorkerFrame()) {
            var frame = entry.getWorkerFrame();
            return switch (frame.getPayloadCase()) {
                case ACCEPT -> frame.getAccept().getAttempt();
                case PROGRESS -> frame.getProgress().getAttempt();
                case HEARTBEAT -> frame.getHeartbeat().getAttempt();
                case CHECKPOINT -> frame.getCheckpoint().getAttempt();
                case BLOCKED -> frame.getBlocked().getAttempt();
                case FAILED -> frame.getFailed().getAttempt();
                case CANCELLED -> frame.getCancelled().getAttempt();
                case COMPLETION -> frame.getCompletion().getAttempt();
                default -> 0;
            };
        }
        var frame = entry.getCoordinatorFrame();
        return switch (frame.getPayloadCase()) {
            case OFFER -> frame.getOffer().getAttempt();
            case RENEWAL -> frame.getRenewal().getAttempt();
            case CANCELLATION -> frame.getCancellation().getAttempt();
            case EXPIRED -> frame.getExpired().getAttempt();
            case REVISION_REQUESTED -> frame.getRevisionRequested().getAttempt();
            case ACCEPTED -> frame.getAccepted().getAttempt();
            default -> 0;
        };
    }

    private static TaskSpec authorSpec() {
        try {
            Map<String, FileDescriptor> closure = new LinkedHashMap<>();
            collect(WorkflowAuthoringDeliverable.getDescriptor().getFile(), closure);
            FileDescriptorSet.Builder descriptors = FileDescriptorSet.newBuilder();
            closure.values().forEach(item -> descriptors.addFile(item.toProto()));
            var spec = TaskSpec.newBuilder().setObjective("Prepare a workflow candidate")
                    .setContract(DeliverableContract.newBuilder()
                            .setTypeName(WorkflowAuthoringDeliverable.getDescriptor().getFullName())
                            .setDescriptorSet(descriptors.build().toByteString()))
                    .addContext(POLICY);
            WorkflowAuthoringReviewer.REQUIRED_CHECKS.forEach(check -> spec.addRequiredChecks(
                    AcceptanceCheck.newBuilder().setName(check).setDescription("verify " + check)));
            return spec.build();
        } catch (Exception failure) {
            throw new ExceptionInInitializerError(failure);
        }
    }

    private static void collect(FileDescriptor file, Map<String, FileDescriptor> closure) {
        if (closure.containsKey(file.getName())) return;
        file.getDependencies().forEach(dependency -> collect(dependency, closure));
        closure.put(file.getName(), file);
    }

    private static final class Repo implements TranscriptRepository {
        private final InMemoryTranscriptRepository delegate = new InMemoryTranscriptRepository();
        private volatile Transcript replacement;
        @Override public Optional<Transcript> load() {
            Transcript current = replacement;
            return current == null ? delegate.load() : Optional.of(current);
        }
        @Override public void save(Transcript transcript) {
            delegate.save(transcript);
            replacement = null;
        }
        void replace(Transcript transcript) { replacement = transcript; }
    }

    private static final class Fixture implements AutoCloseable {
        private final Repo repo = new Repo();
        private final InProcessDelegationCoordinator coordinator = new InProcessDelegationCoordinator(
                AdmissionPolicy.allowAll(), CandidateReviewer.manual(), CLOCK, repo);
        private final String serverName = InProcessServerBuilder.generateName();
        private final Server server;
        private final ManagedChannel channel;
        private final Map<String, DelegationWorker> workers = new LinkedHashMap<>();
        private final Map<String, CountDownLatch> releases = new LinkedHashMap<>();
        private final Map<String, AtomicReference<WorkerRunner.WorkerEvents>> taskEvents = new LinkedHashMap<>();

        Fixture(String primary, String secondary, int scriptsPerWorker) throws Exception {
            server = InProcessServerBuilder.forName(serverName).addService(coordinator).build().start();
            channel = InProcessChannelBuilder.forName(serverName).build();
            addWorker(primary, scriptsPerWorker);
            if (secondary != null) addWorker(secondary, scriptsPerWorker);
        }

        private void addWorker(String workerId, int scripts) throws Exception {
            CountDownLatch release = new CountDownLatch(1);
            AtomicReference<WorkerRunner.WorkerEvents> captured = new AtomicReference<>();
            ScriptedWorkerRunner.Step step = (task, events) -> {
                if (task.taskId().equals(TASK)) captured.set(events);
                if (!release.await(20, TimeUnit.SECONDS)) throw new AssertionError("scripted runner timed out");
                throw new InterruptedException("fixture worker released during cleanup");
            };
            var hello = WorkerHello.newBuilder().setWorkerId(workerId).setProtocolVersion(1)
                    .setProvider("reader-test").setModel("scripted-reader")
                    .addCapabilities(WorkerCapability.newBuilder().setName("workflow-authoring")).build();
            var worker = new DelegationWorker(AgentDelegationServiceGrpc.newStub(channel),
                    new ScriptedWorkerRunner(hello, java.util.Collections.nCopies(scripts, step)));
            workers.put(workerId, worker);
            releases.put(workerId, release);
            taskEvents.put(workerId, captured);
            worker.start();
            if (!worker.awaitAdmission(java.time.Duration.ofSeconds(5))) {
                throw new AssertionError("worker was not admitted: " + workerId);
            }
        }

        void offer(String worker, String task) {
            offer(worker, task, SPEC);
        }

        void offer(String worker, String task, TaskSpec spec) {
            coordinator.offer(worker, task, spec, java.time.Duration.ofMinutes(30));
        }

        void awaitPhase(String task, DelegationReducer.Phase expected, int attempt) throws Exception {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (System.nanoTime() < deadline) {
                var state = coordinator.state().tasks().get(task);
                if (state != null && state.phase() == expected && state.attempt() == attempt) return;
                Thread.sleep(5);
            }
            throw new AssertionError("task did not reach " + expected + " attempt " + attempt
                    + ": " + coordinator.state().tasks().get(task));
        }

        WorkerRunner.WorkerEvents eventsFor(String task) throws Exception {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (System.nanoTime() < deadline) {
                for (AtomicReference<WorkerRunner.WorkerEvents> reference : taskEvents.values()) {
                    WorkerRunner.WorkerEvents events = reference.get();
                    if (events != null) return events;
                }
                Thread.sleep(5);
            }
            throw new AssertionError("no worker events for task " + task);
        }

        Transcript transcript() { return repo.load().orElseThrow(); }
        int cursorOfAttemptOneProgress() {
            return (int) transcript().getEntriesList().stream().filter(TranscriptEntry::hasWorkerFrame)
                    .filter(entry -> entry.getWorkerFrame().hasProgress()
                            && entry.getWorkerFrame().getTaskId().equals(TASK))
                    .mapToLong(entry -> transcript().getEntriesList().indexOf(entry) + 1L)
                    .findFirst().orElseThrow();
        }
        long cursorOfAttemptOneAccept() {
            return transcript().getEntriesList().stream().filter(TranscriptEntry::hasWorkerFrame)
                    .filter(entry -> entry.getWorkerFrame().hasAccept()
                            && entry.getWorkerFrame().getTaskId().equals(TASK))
                    .mapToLong(entry -> transcript().getEntriesList().indexOf(entry) + 1L)
                    .findFirst().orElseThrow();
        }
        void awaitEntries(int count) throws Exception {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (System.nanoTime() < deadline) {
                if (transcript().getEntriesCount() >= count) return;
                Thread.sleep(5);
            }
            throw new AssertionError("transcript did not reach " + count + " entries");
        }

        @Override public void close() throws Exception {
            releases.values().forEach(CountDownLatch::countDown);
            workers.values().forEach(DelegationWorker::close);
            channel.shutdownNow();
            channel.awaitTermination(5, TimeUnit.SECONDS);
            coordinator.close();
            server.shutdownNow();
            server.awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}
