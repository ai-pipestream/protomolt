package ai.protomolt.proto.acquire.confluence;

import ai.protomolt.proto.acquire.confluence.v1.ChangeOperation;
import ai.protomolt.proto.acquire.confluence.v1.ConfluenceChange;
import ai.protomolt.proto.acquire.confluence.v1.ConfluenceSnapshot;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Fan-out semantics of {@link CompositeChangeSink}. */
class CompositeChangeSinkTest {

    @Test
    void everySinkSeesEveryEmission() {
        InMemoryChangeSink first = new InMemoryChangeSink();
        InMemoryChangeSink second = new InMemoryChangeSink();
        CompositeChangeSink composite = new CompositeChangeSink(List.of(first, second));

        ConfluenceChange change = ConfluenceChange.newBuilder()
                .setChangeId("c1")
                .setOperation(ChangeOperation.CHANGE_OPERATION_DELETE)
                .build();
        ConfluenceSnapshot snapshot = ConfluenceSnapshot.newBuilder()
                .setSnapshotId("s1")
                .build();
        composite.emit(change);
        composite.snapshot(snapshot);

        assertThat(first.changes()).containsExactly(change);
        assertThat(second.changes()).containsExactly(change);
        assertThat(first.snapshots()).containsExactly(snapshot);
        assertThat(second.snapshots()).containsExactly(snapshot);
    }

    @Test
    void anEarlyFailureSkipsLaterSinksAndPropagates() {
        ChangeSink failing = new ChangeSink() {
            @Override
            public void emit(ConfluenceChange change) {
                throw new IllegalStateException("boom");
            }

            @Override
            public void snapshot(ConfluenceSnapshot snapshot) {
            }
        };
        InMemoryChangeSink later = new InMemoryChangeSink();
        CompositeChangeSink composite = new CompositeChangeSink(List.of(failing, later));

        assertThatThrownBy(() -> composite.emit(ConfluenceChange.getDefaultInstance()))
                .isInstanceOf(IllegalStateException.class);
        assertThat(later.changes()).isEmpty();
    }
}
