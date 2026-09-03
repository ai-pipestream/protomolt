package ai.protomolt.proto.acquire.confluence;

import ai.protomolt.proto.acquire.confluence.v1.ConfluenceChange;
import ai.protomolt.proto.acquire.confluence.v1.ConfluenceSnapshot;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A {@link ChangeSink} that collects everything in memory, for tests and for
 * callers that want to batch-drain a crawl. Thread-safe by construction.
 */
public final class InMemoryChangeSink implements ChangeSink {

    private final List<ConfluenceChange> changes = new CopyOnWriteArrayList<>();
    private final List<ConfluenceSnapshot> snapshots = new CopyOnWriteArrayList<>();

    @Override
    public void emit(ConfluenceChange change) {
        changes.add(change);
    }

    @Override
    public void snapshot(ConfluenceSnapshot snapshot) {
        snapshots.add(snapshot);
    }

    /** The changes collected so far, in emission order. */
    public List<ConfluenceChange> changes() {
        return List.copyOf(changes);
    }

    /** The snapshots collected so far, in emission order. */
    public List<ConfluenceSnapshot> snapshots() {
        return List.copyOf(snapshots);
    }

    public void clear() {
        changes.clear();
        snapshots.clear();
    }
}
