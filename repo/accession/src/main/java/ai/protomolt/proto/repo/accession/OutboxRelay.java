package ai.protomolt.proto.repo.accession;

import ai.protomolt.proto.repo.catalog.PostgresCatalog;
import ai.protomolt.proto.repo.v2.DocumentEvent;
import java.util.Objects;

/**
 * Drains the catalog outbox in commit order. Each event is published, the
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

    private final PostgresCatalog catalog;
    private final Publisher publisher;

    public OutboxRelay(PostgresCatalog catalog, Publisher publisher) {
        this.catalog = Objects.requireNonNull(catalog);
        this.publisher = Objects.requireNonNull(publisher);
    }

    /** Relays up to {@code limit} pending events; returns how many were stamped relayed. */
    public int relayOnce(int limit) throws InterruptedException {
        int relayed = 0;
        for (var pending : catalog.pendingEvents(limit)) {
            try {
                publisher.publish(pending.key(), pending.event());
            } catch (InterruptedException interrupted) {
                throw interrupted;
            } catch (Exception failure) {
                catalog.markAttempt(pending.eventId());
                continue;
            }
            if (catalog.markRelayed(pending.eventId())) relayed++;
        }
        return relayed;
    }
}
