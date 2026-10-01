package ai.protomolt.proto.samples;

import ai.protomolt.proto.samples.authoring.v1.AuthoringWorkerPending;
import ai.protomolt.proto.samples.authoring.v1.AuthoringWorkerState;
import ai.protomolt.proto.samples.authoring.v1.WriteRecordRequest;
import ai.protomolt.proto.workflow.authoring.v1.PrepareWorkflowCandidateRequest;
import ai.protomolt.proto.workflow.authoring.v1.WorkflowAuthorAssignment;
import com.google.protobuf.ByteString;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Filesystem recovery-boundary checks; no network effect or process-crash claim. */
class AuthoringWorkerStateStoreTest {
    private static final String COORDINATOR = "grpc://coordinator.example:8443";
    private static final String FIXTURE = "grpc://fixture.example:9443";
    private static final String WORKER = "workflow-author";
    private static final String TASK = "00000000-0000-4000-8000-0000000000a1";
    private static final String TASK_B = "00000000-0000-4000-8000-0000000000b2";
    private static final String OFFER_HASH = "a".repeat(64);

    @TempDir Path temp;

    @Test
    void initialSnapshotAndQueuedIntentSurviveReopenWithStableDerivedIds() throws Exception {
        Path directory = temp.resolve("worker");
        WorkflowAuthorAssignment assignment = assignment(1, TASK, 1).build();
        String probeId = AuthoringWorkerStateStore.probeOperationId(assignment, WORKER);
        String preparationId = AuthoringWorkerStateStore.preparationId(assignment, WORKER);

        try (var store = open(directory)) {
            assertThat(store.state()).isEqualTo(initial());
            AuthoringWorkerPending pending = AuthoringWorkerPending.newBuilder().setAssignment(assignment)
                    .setProbe(WriteRecordRequest.newBuilder().setOperationId(probeId)
                            .setContent(AuthoringWorkerStateStore.PROBE_CONTENT))
                    .setPreparation(PrepareWorkflowCandidateRequest.newBuilder().setTaskId(TASK)
                            .setAttempt(1).setRevision(1).setPreparationId(preparationId)
                            .setExecutableSourceJson(ByteString.copyFromUtf8("{}")))
                    .build();
            store.commit(initial().toBuilder().setDiscoveryCursor(1).addPending(pending).build());
            assertThat(store.state().getPending(0).getProbe().getOperationId()).isEqualTo(probeId);
            assertThat(store.state().getPending(0).getPreparation().getPreparationId()).isEqualTo(preparationId);
        }

        try (var reopened = open(directory)) {
            assertThat(reopened.state().getDiscoveryCursor()).isEqualTo(1);
            assertThat(reopened.state().getPending(0).getAssignment()).isEqualTo(assignment);
            assertThat(reopened.state().getPending(0).getProbe().getOperationId()).isEqualTo(probeId);
            assertThat(reopened.state().getPending(0).getPreparation().getPreparationId()).isEqualTo(preparationId);
            assertThat(AuthoringWorkerStateStore.probeOperationId(assignment, WORKER)).isEqualTo(probeId);
            assertThat(AuthoringWorkerStateStore.preparationId(assignment, WORKER)).isEqualTo(preparationId);
            assertThat(AuthoringWorkerStateStore.probeOperationId(assignment(1, TASK_B, 1).build(), WORKER))
                    .isNotEqualTo(probeId);
        }
    }

    @Test
    void movingAnAssignmentToALaterCursorCannotDiscardSavedIntent() throws Exception {
        try (var store = open(temp.resolve("relocated"))) {
            var original = pending(1, TASK).build();
            var probe = WriteRecordRequest.newBuilder()
                    .setOperationId(AuthoringWorkerStateStore.probeOperationId(original.getAssignment(), WORKER))
                    .setContent(AuthoringWorkerStateStore.PROBE_CONTENT).build();
            var saved = initial().toBuilder().setDiscoveryCursor(1)
                    .addPending(original.toBuilder().setProbe(probe)).build();
            store.commit(saved);
            var relocated = saved.toBuilder().setDiscoveryCursor(2).clearPending()
                    .addPending(pending(2, TASK)).build();
            assertThatThrownBy(() -> store.commit(relocated))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("assignment identity");
            assertThat(store.state()).isEqualTo(saved);
        }
        try (var reopened = open(temp.resolve("relocated"))) {
            assertThat(reopened.state().getPending(0).hasProbe()).isTrue();
            assertThat(reopened.state().getDiscoveryCursor()).isEqualTo(1);
        }
    }

    @Test
    void directoryLockAndConfiguredIdentityAreEnforced() throws Exception {
        Path directory = temp.resolve("locked");
        try (var first = open(directory)) {
            assertThatThrownBy(() -> open(directory)).isInstanceOf(IOException.class);
            assertThat(first.state().getWorkerId()).isEqualTo(WORKER);
        }
        assertThatThrownBy(() -> AuthoringWorkerStateStore.open(directory, COORDINATOR, FIXTURE, "other-worker"))
                .isInstanceOf(IOException.class);
        try (var reopened = open(directory)) {
            assertThat(reopened.state().getWorkerId()).isEqualTo(WORKER);
        }
        assertThatThrownBy(() -> AuthoringWorkerStateStore.open(temp.resolve("invalid"), "", FIXTURE, WORKER))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void invalidCommitsDoNotAdvanceMemoryOrReplaceTheDurableSnapshot() throws Exception {
        Path directory = temp.resolve("transition");
        try (var store = open(directory)) {
            AuthoringWorkerState before = store.state();
            AuthoringWorkerState invalid = before.toBuilder().setDiscoveryCursor(-1).build();
            assertThatThrownBy(() -> store.commit(invalid)).isInstanceOf(IllegalArgumentException.class);
            assertThat(store.state()).isEqualTo(before);

            AuthoringWorkerState valid = before.toBuilder().setDiscoveryCursor(2).build();
            store.commit(valid);
            assertThat(store.state()).isEqualTo(valid);

            AuthoringWorkerState regressed = valid.toBuilder().setDiscoveryCursor(1).build();
            assertThatThrownBy(() -> store.commit(regressed)).isInstanceOf(IllegalArgumentException.class);
            assertThat(store.state()).isEqualTo(valid);

            var first = pending(3, TASK).setReviewCursor(3).build();
            AuthoringWorkerState queued = valid.toBuilder().setDiscoveryCursor(3).addPending(first).build();
            store.commit(queued);
            var cursorRegressed = queued.toBuilder().setPending(0, first.toBuilder().setReviewCursor(2)).build();
            assertThatThrownBy(() -> store.commit(cursorRegressed)).isInstanceOf(IllegalArgumentException.class);
            assertThat(store.state()).isEqualTo(queued);
        }
        try (var reopened = open(directory)) {
            assertThat(reopened.state().getPending(0).getReviewCursor()).isEqualTo(3);
        }
    }

    @Test
    void duplicateUnorderedAndForgedDeterministicIntentsAreRejectedWithoutMutation() throws Exception {
        Path directory = temp.resolve("identity");
        try (var store = open(directory)) {
            AuthoringWorkerState initial = store.state();
            var first = pending(2, TASK).build();
            var second = pending(1, TASK_B).build();
            assertThatThrownBy(() -> store.commit(initial.toBuilder().setDiscoveryCursor(2)
                    .addPending(first).addPending(second).build())).isInstanceOf(IllegalArgumentException.class);

            var duplicate = pending(3, TASK).setAssignment(assignment(3, TASK, 1)).build();
            assertThatThrownBy(() -> store.commit(initial.toBuilder().setDiscoveryCursor(2)
                    .addPending(first).setDiscoveryCursor(3).addPending(duplicate).build()))
                    .isInstanceOf(IllegalArgumentException.class);

            String expectedProbe = AuthoringWorkerStateStore.probeOperationId(first.getAssignment(), WORKER);
            var forgedProbe = first.toBuilder().setProbe(WriteRecordRequest.newBuilder()
                    .setOperationId("00000000-0000-4000-8000-0000000000c3")
                    .setContent(AuthoringWorkerStateStore.PROBE_CONTENT)).build();
            assertThatThrownBy(() -> store.commit(initial.toBuilder().setDiscoveryCursor(2)
                    .addPending(forgedProbe).build())).isInstanceOf(IllegalArgumentException.class);

            var forgedPreparation = first.toBuilder().setProbe(WriteRecordRequest.newBuilder()
                    .setOperationId(expectedProbe).setContent(AuthoringWorkerStateStore.PROBE_CONTENT))
                    .setPreparation(PrepareWorkflowCandidateRequest.newBuilder().setTaskId(TASK).setAttempt(1)
                            .setRevision(1).setPreparationId("00000000-0000-4000-8000-0000000000c3")
                            .setExecutableSourceJson(ByteString.copyFromUtf8("{}"))).build();
            assertThatThrownBy(() -> store.commit(initial.toBuilder().setDiscoveryCursor(2)
                    .addPending(forgedPreparation).build())).isInstanceOf(IllegalArgumentException.class);
            assertThat(store.state()).isEqualTo(initial);
        }
    }

    @Test
    void corruptUnknownNoncanonicalAndOversizedFilesFailClosed() throws Exception {
        Path directory = temp.resolve("corrupt");
        try (var store = open(directory)) {
            assertThat(store.state()).isEqualTo(initial());
        }
        Path file = directory.resolve("authoring-worker-state.pb");
        byte[] canonical = Files.readAllBytes(file);

        Files.write(file, new byte[] { (byte) 0xff, (byte) 0xff });
        assertThatThrownBy(() -> open(directory)).isInstanceOf(IOException.class);

        Files.write(file, appendUnknownVarint(canonical));
        assertThatThrownBy(() -> open(directory)).isInstanceOf(IOException.class);

        // Duplicate format_version=1 parses to the same message but is not canonical.
        Files.write(file, appendVarintField(canonical, 1, 1));
        assertThatThrownBy(() -> open(directory)).isInstanceOf(IOException.class);

        Files.write(file, new byte[AuthoringWorkerStateStore.MAX_BYTES + 1]);
        assertThatThrownBy(() -> open(directory)).isInstanceOf(IOException.class);
    }

    @Test
    void failedAtomicPublishPoisonsThisInstanceAndFreshOpenResolvesTheCommitBoundary() throws Exception {
        Path beforeRenameDirectory = temp.resolve("before-rename");
        try (var initialized = open(beforeRenameDirectory)) {
            assertThat(initialized.state()).isEqualTo(initial());
        }
        var beforeRename = AuthoringWorkerStateStore.open(beforeRenameDirectory, COORDINATOR, FIXTURE, WORKER,
                (source, target) -> {
                    throw new AtomicMoveNotSupportedException(source.toString(), target.toString(), "injected");
                }, () -> {});
        AuthoringWorkerState proposed = initial().toBuilder().setDiscoveryCursor(7).build();
        assertThatThrownBy(() -> beforeRename.commit(proposed)).isInstanceOf(AtomicMoveNotSupportedException.class);
        assertThatThrownBy(beforeRename::state).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> beforeRename.commit(proposed)).isInstanceOf(IllegalStateException.class);
        beforeRename.close();
        try (var recovered = open(beforeRenameDirectory)) {
            assertThat(recovered.state()).isEqualTo(initial());
        }

        Path afterRenameDirectory = temp.resolve("after-rename");
        try (var initialized = open(afterRenameDirectory)) {
            assertThat(initialized.state()).isEqualTo(initial());
        }
        var afterRename = AuthoringWorkerStateStore.open(afterRenameDirectory, COORDINATOR, FIXTURE, WORKER,
                (source, target) -> Files.move(source, target, java.nio.file.StandardCopyOption.ATOMIC_MOVE,
                        java.nio.file.StandardCopyOption.REPLACE_EXISTING),
                () -> { throw new IOException("injected after rename"); });
        assertThatThrownBy(() -> afterRename.commit(proposed)).isInstanceOf(IOException.class)
                .hasMessage("injected after rename");
        assertThatThrownBy(afterRename::state).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> afterRename.commit(initial())).isInstanceOf(IllegalStateException.class);
        afterRename.close();
        try (var recovered = open(afterRenameDirectory)) {
            assertThat(recovered.state()).isEqualTo(proposed);
        }
    }

    private static AuthoringWorkerStateStore open(Path directory) throws IOException {
        return AuthoringWorkerStateStore.open(directory, COORDINATOR, FIXTURE, WORKER);
    }

    private static AuthoringWorkerState initial() {
        return AuthoringWorkerState.newBuilder().setFormatVersion(1).setCoordinator(COORDINATOR)
                .setFixture(FIXTURE).setWorkerId(WORKER).build();
    }

    private static WorkflowAuthorAssignment.Builder assignment(long cursor, String taskId, int attempt) {
        return WorkflowAuthorAssignment.newBuilder().setCursor(cursor).setTaskId(taskId)
                .setAttempt(attempt).setOfferEntrySha256(OFFER_HASH);
    }

    private static AuthoringWorkerPending.Builder pending(long cursor, String taskId) {
        var assignment = assignment(cursor, taskId, 1).build();
        return AuthoringWorkerPending.newBuilder().setAssignment(assignment)
                .setProbe(WriteRecordRequest.newBuilder()
                        .setOperationId(AuthoringWorkerStateStore.probeOperationId(assignment, WORKER))
                        .setContent(AuthoringWorkerStateStore.PROBE_CONTENT));
    }

    private static byte[] appendUnknownVarint(byte[] bytes) {
        return appendVarintField(bytes, 99, 1);
    }

    private static byte[] appendVarintField(byte[] bytes, int fieldNumber, int value) {
        byte[] tag = varint((fieldNumber << 3));
        byte[] encodedValue = varint(value);
        byte[] result = java.util.Arrays.copyOf(bytes, bytes.length + tag.length + encodedValue.length);
        System.arraycopy(tag, 0, result, bytes.length, tag.length);
        System.arraycopy(encodedValue, 0, result, bytes.length + tag.length, encodedValue.length);
        return result;
    }

    private static byte[] varint(int value) {
        byte[] result = new byte[5];
        int size = 0;
        while ((value & ~0x7f) != 0) {
            result[size++] = (byte) ((value & 0x7f) | 0x80);
            value >>>= 7;
        }
        result[size++] = (byte) value;
        return java.util.Arrays.copyOf(result, size);
    }
}
