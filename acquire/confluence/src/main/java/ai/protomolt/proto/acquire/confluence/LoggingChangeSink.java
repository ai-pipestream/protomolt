package ai.protomolt.proto.acquire.confluence;

import ai.protomolt.proto.acquire.confluence.v1.ConfluenceChange;
import ai.protomolt.proto.acquire.confluence.v1.ConfluenceSnapshot;

/**
 * A {@link ChangeSink} that logs one line per change and per snapshot through
 * {@link System.Logger} - no logging dependency, and the change payloads
 * carry no credentials, so there is nothing to redact.
 */
public final class LoggingChangeSink implements ChangeSink {

    private static final System.Logger LOG = System.getLogger(LoggingChangeSink.class.getName());

    @Override
    public void emit(ConfluenceChange change) {
        LOG.log(System.Logger.Level.INFO, "confluence change id={0} op={1} source={2} entity={3}",
                change.getChangeId(), change.getOperation(), change.getSource(),
                change.getEntity().getEntityId());
    }

    @Override
    public void snapshot(ConfluenceSnapshot snapshot) {
        LOG.log(System.Logger.Level.INFO,
                "confluence snapshot id={0} space={1} counts={2} cursor={3}",
                snapshot.getSnapshotId(), snapshot.getSpaceKey(), snapshot.getEntityCountsMap(),
                snapshot.getCursor());
    }
}
