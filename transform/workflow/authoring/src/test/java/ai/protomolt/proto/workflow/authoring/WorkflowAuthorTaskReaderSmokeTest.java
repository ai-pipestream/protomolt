package ai.protomolt.proto.workflow.authoring;

import ai.protomolt.proto.actions.Caller;
import ai.protomolt.proto.actions.Scopes;
import ai.protomolt.proto.delegation.InMemoryTranscriptRepository;
import ai.protomolt.proto.grpc.workflow.ArtifactRepository;
import ai.protomolt.proto.grpc.workflow.v1.ArtifactReference;
import ai.protomolt.proto.workflow.authoring.v1.GetWorkflowAuthorContextRequest;
import ai.protomolt.proto.workflow.authoring.v1.ReadWorkflowAuthorEventsRequest;
import java.io.IOException;
import java.time.Clock;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkflowAuthorTaskReaderSmokeTest {
    private static final String TASK = "00000000-0000-4000-8000-000000000001";
    private final Caller author = Caller.scoped("author", Set.of(Scopes.WORKFLOW_AUTHOR));
    private final WorkflowAuthorTaskReader reader = new WorkflowAuthorTaskReader(
            new InMemoryTranscriptRepository(), new ArtifactRepository() {
                @Override public ArtifactReference save(byte[] content, String mediaType,
                        boolean redacted) throws IOException {
                    throw new AssertionError("invalid or absent task must not access artifacts");
                }
                @Override public Optional<StoredArtifact> find(String sha256) throws IOException {
                    throw new AssertionError("invalid or absent task must not access artifacts");
                }
            }, ArtifactReference.newBuilder().setSha256("a".repeat(64))
                    .setSizeBytes(1).setMediaType("application/x-protobuf").build(),
            Clock.systemUTC());

    @Test
    void malformedRequestIsRejectedBeforeTranscriptAccess() {
        var invalid = GetWorkflowAuthorContextRequest.newBuilder().setTaskId(TASK).build();
        assertThatThrownBy(() -> reader.context(invalid, author))
                .isInstanceOfSatisfying(WorkflowPreparationException.class,
                        failure -> assertThat(failure.kind())
                                .isEqualTo(WorkflowPreparationException.Kind.INVALID_INPUT));
    }

    @Test
    void absentTranscriptIsInactiveForBothReadMethods() {
        var context = GetWorkflowAuthorContextRequest.newBuilder().setTaskId(TASK)
                .setAttempt(1).build();
        var events = ReadWorkflowAuthorEventsRequest.newBuilder().setTaskId(TASK)
                .setAttempt(1).setMaxEvents(4).build();
        assertThatThrownBy(() -> reader.context(context, author))
                .isInstanceOfSatisfying(WorkflowPreparationException.class,
                        failure -> assertThat(failure.kind())
                                .isEqualTo(WorkflowPreparationException.Kind.INACTIVE));
        assertThatThrownBy(() -> reader.events(events, author))
                .isInstanceOfSatisfying(WorkflowPreparationException.class,
                        failure -> assertThat(failure.kind())
                                .isEqualTo(WorkflowPreparationException.Kind.INACTIVE));
    }
}
