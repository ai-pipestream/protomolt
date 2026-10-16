package ai.protomolt.proto.repo.accession;

import ai.protomolt.proto.repo.catalog.Catalog;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v2.DocumentEvent;
import java.util.HashSet;
import java.util.Objects;

/**
 * Drains a snapshot of the catalog outbox in commit order. A failed publish holds
 * back later events for the same full document address during that pass; other
 * documents may continue. Each event is published, the
 * publish is acknowledged, then the row is stamped relayed. A crash or failure
 * between the two republishes the same event, with the same event_id, on the
 * next pass: delivery is at-least-once and the catalog keeps one event per
 * accession. The Kafka publisher is a host adapter; this slice ships none.
 */
public final class OutboxRelay {
    /** Publishes one event and returns once the broker has acknowledged it. */
    @FunctionalInterface public interface Publisher {
        void publish(String key, DocumentEvent event) throws Exception;
    }

    private final Catalog catalog;
    private final Publisher publisher;

    public OutboxRelay(Catalog catalog, Publisher publisher) {
        this.catalog = Objects.requireNonNull(catalog);
        this.publisher = Objects.requireNonNull(publisher);
    }

    /** Relays up to {@code limit} pending events; returns how many were stamped relayed. */
    public int relayOnce(int limit) throws InterruptedException {
        int relayed = 0;
        var blocked = new HashSet<NodeAddress>();
        for (var pending : catalog.pendingEvents(limit)) {
            if (blocked.contains(pending.event().getAddress())) continue;
            try {
                publisher.publish(pending.key(), pending.event());
            } catch (InterruptedException interrupted) {
                throw interrupted;
            } catch (Exception failure) {
                blocked.add(pending.event().getAddress());
                catalog.markAttempt(pending.eventId());
                continue;
            }
            if (catalog.markRelayed(pending.eventId())) relayed++;
        }
        return relayed;
    }
}
