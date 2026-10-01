package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.delegation.AdmissionPolicy;
import ai.protomolt.proto.delegation.CandidateReviewer;
import ai.protomolt.proto.delegation.DelegationBridge;
import ai.protomolt.proto.delegation.DelegationReducer;
import ai.protomolt.proto.delegation.InMemoryTranscriptRepository;
import ai.protomolt.proto.delegation.InProcessDelegationCoordinator;
import ai.protomolt.proto.delegation.TranscriptRepository;
import ai.protomolt.proto.delegation.v1.AcceptanceCheck;
import ai.protomolt.proto.delegation.v1.DeliverableContract;
import ai.protomolt.proto.delegation.v1.TaskSpec;
import ai.protomolt.proto.delegation.v1.Transcript;
import ai.protomolt.proto.delegation.v1.TranscriptEntry;
import ai.protomolt.proto.delegation.v1.WorkerCapability;
import ai.protomolt.proto.delegation.v1.WorkerHello;
import ai.protomolt.proto.grpc.workflow.ArtifactRepository;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.samples.starter.v1.WorkflowAuthoringDeliverable;
import ai.protomolt.proto.workflow.authoring.v1.ReadWorkflowAuthorAssignmentsRequest;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.FileDescriptor;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Discovery boundaries over real coordinator offers and frozen transcript snapshots. */
class WorkflowAuthorAssignmentReaderTest {
    private static final String AUTHOR = "author-one";
    private static final String GENERIC_TASK = "00000000-0000-4000-8000-000000000601";
    private static final String AUTHOR_TASK = "00000000-0000-4000-8000-000000000602";
    private static final ArtifactReference POLICY = ArtifactReference.newBuilder()
            .setSha256("a".repeat(64)).setMediaType("application/x-protobuf").setSizeBytes(32).build();
    private static final Caller CALLER = Caller.scoped(AUTHOR, Set.of(Scopes.WORKFLOW_AUTHOR));

    @Test
    void genericOfferIsFilteredWithoutPolicyArtifactReads() throws Exception {
        Transcript snapshot = offeredTranscript();
        var result = reader(readOnly(snapshot)).assignments(request(0), CALLER);
        assertThat(result.getAssignmentsList()).singleElement().satisfies(assignment -> {
            assertThat(assignment.getTaskId()).isEqualTo(AUTHOR_TASK);
            assertThat(assignment.getAttempt()).isEqualTo(1);
            assertThat(assignment.getOfferEntrySha256()).isEqualTo(WorkflowLaunchValidation.sha256(
                    snapshot.getEntries((int) assignment.getCursor() - 1)));
        });
        assertThat(result.getCursor()).isEqualTo(snapshot.getEntriesCount());
        assertThat(result.getTruncated()).isFalse();
    }

    @Test
    void emptyPageAdvancesExactly256NewPositionsThenFindsNextOffer() throws Exception {
        Transcript original = offeredTranscript();
        TranscriptEntry generic = offer(original, GENERIC_TASK);
        int authorIndex = indexOfOffer(original, AUTHOR_TASK);
        var expanded = Transcript.newBuilder();
        expanded.addAllEntries(original.getEntriesList().subList(0, authorIndex));
        for (int index = 0; index < 256; index++) expanded.addEntries(generic);
        expanded.addAllEntries(original.getEntriesList().subList(authorIndex, original.getEntriesCount()));
        Transcript snapshot = expanded.build();
        assertThat(new DelegationReducer().reduce(snapshot).clean()).isTrue();

        var reader = reader(readOnly(snapshot));
        var first = reader.assignments(request(authorIndex), CALLER);
        assertThat(first.getAssignmentsList()).isEmpty();
        assertThat(first.getCursor()).isEqualTo(authorIndex + 256L);
        assertThat(first.getTruncated()).isTrue();
        var second = reader.assignments(request(first.getCursor()), CALLER);
        assertThat(second.getAssignmentsList()).singleElement()
                .satisfies(assignment -> assertThat(assignment.getTaskId()).isEqualTo(AUTHOR_TASK));
    }

    @Test
    void duplicateOfferAfterCursorDoesNotProduceAnotherAssignment() throws Exception {
        Transcript original = offeredTranscript();
        TranscriptEntry authorOffer = offer(original, AUTHOR_TASK);
        long afterOriginal = indexOfOffer(original, AUTHOR_TASK) + 1L;
        Transcript duplicate = original.toBuilder().addEntries(authorOffer).build();
        assertThat(new DelegationReducer().reduce(duplicate).clean()).isTrue();
        var result = reader(readOnly(duplicate)).assignments(request(afterOriginal), CALLER);
        assertThat(result.getAssignmentsList()).isEmpty();
        assertThat(result.getCursor()).isEqualTo(duplicate.getEntriesCount());
    }

    @Test
    void malformedHistoricalAuthorOfferFailsClosed() {
        Transcript original = offeredTranscript();
        int index = indexOfOffer(original, AUTHOR_TASK);
        TranscriptEntry entry = original.getEntries(index);
        var malformedSpec = entry.getCoordinatorFrame().getOffer().getSpec().toBuilder()
                .clearRequiredChecks().addRequiredChecks(entry.getCoordinatorFrame().getOffer()
                        .getSpec().getRequiredChecks(0)).build();
        var changedOffer = entry.getCoordinatorFrame().getOffer().toBuilder().setSpec(malformedSpec).build();
        Transcript corrupt = original.toBuilder().setEntries(index, entry.toBuilder()
                .setCoordinatorFrame(entry.getCoordinatorFrame().toBuilder().setOffer(changedOffer))).build();
        assertThat(new DelegationReducer().reduce(corrupt).clean()).isTrue();
        assertThatThrownBy(() -> reader(readOnly(corrupt)).assignments(request(0), CALLER))
                .isInstanceOfSatisfying(WorkflowPreparationException.class, failure ->
                        assertThat(failure.kind()).isEqualTo(WorkflowPreparationException.Kind.CORRUPT_EVIDENCE));
    }

    @Test
    void transcriptStorageOutageIsUnavailable() {
        TranscriptRepository unavailable = new TranscriptRepository() {
            @Override public Optional<Transcript> load() { throw new IllegalStateException("storage outage"); }
            @Override public void save(Transcript transcript) { throw new AssertionError("discovery must not write"); }
        };
        assertThatThrownBy(() -> reader(unavailable).assignments(request(0), CALLER))
                .isInstanceOfSatisfying(WorkflowPreparationException.class, failure ->
                        assertThat(failure.kind()).isEqualTo(WorkflowPreparationException.Kind.UNAVAILABLE));
    }

    private static Transcript offeredTranscript() {
        var repository = new InMemoryTranscriptRepository();
        try (var coordinator = new InProcessDelegationCoordinator(
                AdmissionPolicy.allowAll(), CandidateReviewer.manual(), Clock.systemUTC(), repository);
             var bridge = new DelegationBridge(coordinator)) {
            bridge.registerWorker(WorkerHello.newBuilder().setWorkerId(AUTHOR).setProtocolVersion(1)
                    .setProvider("assignment-test")
                    .addCapabilities(WorkerCapability.newBuilder().setName("workflow-authoring")).build());
            coordinator.offer(AUTHOR, GENERIC_TASK, TaskSpec.newBuilder().setObjective("generic task")
                    .addRequiredChecks(AcceptanceCheck.newBuilder().setName("check")
                            .setDescription("generic check")).build(), Duration.ofMinutes(5));
            coordinator.offer(AUTHOR, AUTHOR_TASK, authorSpec(), Duration.ofMinutes(5));
            return coordinator.transcript();
        }
    }

    private static TaskSpec authorSpec() {
        Map<String, FileDescriptor> closure = new LinkedHashMap<>();
        collect(WorkflowAuthoringDeliverable.getDescriptor().getFile(), closure);
        var descriptors = FileDescriptorSet.newBuilder();
        closure.values().forEach(file -> descriptors.addFile(file.toProto()));
        var spec = TaskSpec.newBuilder().setObjective("Prepare workflow")
                .setContract(DeliverableContract.newBuilder()
                        .setTypeName(WorkflowAuthoringDeliverable.getDescriptor().getFullName())
                        .setDescriptorSet(descriptors.build().toByteString()))
                .addContext(POLICY);
        WorkflowAuthoringReviewer.REQUIRED_CHECKS.forEach(check -> spec.addRequiredChecks(
                AcceptanceCheck.newBuilder().setName(check).setDescription("verify " + check)));
        return spec.build();
    }

    private static void collect(FileDescriptor file, Map<String, FileDescriptor> closure) {
        if (closure.containsKey(file.getName())) return;
        file.getDependencies().forEach(dependency -> collect(dependency, closure));
        closure.put(file.getName(), file);
    }

    private static TranscriptEntry offer(Transcript transcript, String task) {
        return transcript.getEntries(indexOfOffer(transcript, task));
    }

    private static int indexOfOffer(Transcript transcript, String task) {
        for (int index = 0; index < transcript.getEntriesCount(); index++) {
            TranscriptEntry entry = transcript.getEntries(index);
            if (entry.hasCoordinatorFrame() && entry.getCoordinatorFrame().hasOffer()
                    && entry.getCoordinatorFrame().getTaskId().equals(task)) return index;
        }
        throw new AssertionError("missing coordinator offer: " + task);
    }

    private static ReadWorkflowAuthorAssignmentsRequest request(long cursor) {
        return ReadWorkflowAuthorAssignmentsRequest.newBuilder()
                .setAfterCursor(cursor).setMaxAssignments(64).build();
    }

    private static TranscriptRepository readOnly(Transcript snapshot) {
        return new TranscriptRepository() {
            @Override public Optional<Transcript> load() { return Optional.of(snapshot); }
            @Override public void save(Transcript transcript) { throw new AssertionError("discovery must not write"); }
        };
    }

    private static WorkflowAuthorTaskReader reader(TranscriptRepository transcripts) {
        ArtifactRepository artifacts = new ArtifactRepository() {
            @Override public ArtifactReference save(byte[] content, String mediaType,
                    boolean redacted) throws IOException { throw new AssertionError("discovery must not save artifacts"); }
            @Override public Optional<StoredArtifact> find(String sha256) throws IOException {
                throw new AssertionError("historical discovery must not read current policy artifacts");
            }
        };
        return new WorkflowAuthorTaskReader(transcripts, artifacts, POLICY, Clock.systemUTC());
    }
}
