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
 * accession. The Kafka publisher is a host adapter; the test tree qualifies
 * one against a real broker and keeps the production runtime Kafka-free.
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
        var relayed = new java.util.ArrayList<String>();
        var attempted = new java.util.ArrayList<String>();
        var blocked = new HashSet<NodeAddress>();
        int count = 0;
        try {
            for (var pending : catalog.pendingEvents(limit)) {
                if (blocked.contains(pending.event().getAddress())) continue;
                try {
                    publisher.publish(pending.key(), pending.event());
                } catch (InterruptedException interrupted) {
                    throw interrupted;
                } catch (Exception failure) {
                    blocked.add(pending.event().getAddress());
                    attempted.add(pending.eventId());
                    continue;
                }
                relayed.add(pending.eventId());
            }
        } finally {
            RuntimeException attemptFailure = null;
            if (!attempted.isEmpty()) {
                try {
                    catalog.markAttempts(attempted);
                } catch (RuntimeException failure) {
                    attemptFailure = failure;
                }
            }
            if (!relayed.isEmpty()) {
                count = catalog.markRelayed(relayed);
            }
            if (attemptFailure != null) {
                throw attemptFailure;
            }
        }
        return count;
    }
}
