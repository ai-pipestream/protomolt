package ai.pipestream.proto.acquire.confluence;

import ai.pipestream.proto.acquire.confluence.v1.ChangeOperation;
import ai.pipestream.proto.acquire.confluence.v1.ConfluenceChange;
import ai.pipestream.proto.acquire.confluence.v1.ConfluenceSnapshot;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The collection contract of {@link InMemoryChangeSink}. */
class InMemoryChangeSinkTest {

    @Test
    void collectsChangesAndSnapshotsInEmissionOrder() {
        InMemoryChangeSink sink = new InMemoryChangeSink();
        ConfluenceChange first = ConfluenceChange.newBuilder().setChangeId("c1").build();
        ConfluenceChange second = ConfluenceChange.newBuilder().setChangeId("c2")
                .setOperation(ChangeOperation.CHANGE_OPERATION_DELETE).build();
        ConfluenceSnapshot snapshot = ConfluenceSnapshot.newBuilder().setSnapshotId("s1").build();

        sink.emit(first);
        sink.emit(second);
        sink.snapshot(snapshot);

        assertThat(sink.changes()).containsExactly(first, second);
        assertThat(sink.snapshots()).containsExactly(snapshot);
    }

    @Test
    void returnedListsAreDefensiveCopies() {
        InMemoryChangeSink sink = new InMemoryChangeSink();
        sink.emit(ConfluenceChange.newBuilder().setChangeId("c1").build());
        sink.snapshot(ConfluenceSnapshot.newBuilder().setSnapshotId("s1").build());

        // Draining the returned list must not drain the sink.
        assertThatThrownBy(() -> sink.changes().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThatThrownBy(() -> sink.snapshots().clear())
                .isInstanceOf(UnsupportedOperationException.class);
        assertThat(sink.changes()).hasSize(1);
        assertThat(sink.snapshots()).hasSize(1);
    }

    @Test
    void clearEmptiesBothCollections() {
        InMemoryChangeSink sink = new InMemoryChangeSink();
        sink.emit(ConfluenceChange.getDefaultInstance());
        sink.snapshot(ConfluenceSnapshot.getDefaultInstance());

        sink.clear();

        assertThat(sink.changes()).isEmpty();
        assertThat(sink.snapshots()).isEmpty();
    }

    @Test
    void acceptsDefaultInstances() {
        InMemoryChangeSink sink = new InMemoryChangeSink();

        sink.emit(ConfluenceChange.getDefaultInstance());
        sink.snapshot(ConfluenceSnapshot.getDefaultInstance());

        assertThat(sink.changes()).containsExactly(ConfluenceChange.getDefaultInstance());
        assertThat(sink.snapshots()).containsExactly(ConfluenceSnapshot.getDefaultInstance());
    }
}
