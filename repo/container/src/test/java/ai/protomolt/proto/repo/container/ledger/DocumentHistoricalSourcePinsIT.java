package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.container.blob.DocumentIds;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** Real SQL pin identities; publication provider observations are synthetic fixture data. */
@Testcontainers
class DocumentHistoricalSourcePinsIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller ADMIN = new RepositoryCaller("pin-reader", true);

    @Test void selectsOnlyBorrowedObjectsAndBindsTheExactCaptureLifetime() throws Exception {
        try (var c = context(POSTGRES)) {
            var prepared = prepare(c, 1);
            var published = publish(c, prepared, Fault.NONE, em -> {});
            var revision = UUID.fromString(published.getMembers(0).getRevisionId());
            var address = prepared.command().intent().getMembers(0).getDestination().getAddress();
            var reader = UUID.randomUUID();
            var ledger = new DocumentReadLedger(c.tx(), reader);
            var first = ledger.captureHistorical(ADMIN, address, revision);
            var second = ledger.captureHistorical(ADMIN, address, revision);
            try (var use = first.use(); var other = second.use()) {
                var entry = use.plan().entries().getFirst();
                assertThat(use.plan().entries()).hasSize(2);
                var selected = first.selectedPins(use, List.of(entry, entry), RepositoryReadControl.NONE);
                assertThat(selected).hasSize(1);
                var pin = selected.getFirst();
                assertThat(pin.reader()).isEqualTo(reader);
                assertThat(pin.object()).isEqualTo(entry.objectId());
                assertThat(pin.node()).isEqualTo(DocumentIds.nodeId(address));
                assertThat(pin.revision()).isEqualTo(revision);
                assertThat(pin.publicationRevision()).isEqualTo(use.plan().publicationRevision());
                long exact = c.tx().readOnly(em -> ((Number) em.createNativeQuery("""
                        SELECT count(*) FROM document_read_pins WHERE pin_id=:pin AND reader_incarnation=:reader
                        AND object_id=:object AND source_node=:node AND source_revision=:revision
                        AND publication_revision=:publication AND read_scope='HISTORICAL'
                        """).setParameter("pin", pin.pin()).setParameter("reader", pin.reader())
                        .setParameter("object", pin.object()).setParameter("node", pin.node())
                        .setParameter("revision", pin.revision()).setParameter("publication", pin.publicationRevision())
                        .getSingleResult()).longValue());
                assertThat(exact).isEqualTo(1);
                assertThat(count(c, "document_read_pins")).isEqualTo(4);
                var otherPin = second.selectedPins(other, List.of(other.plan().entries().getFirst()), RepositoryReadControl.NONE).getFirst();
                assertThat(otherPin.pin()).isNotEqualTo(pin.pin());
                assertThat(otherPin.object()).isEqualTo(pin.object());
                assertThatThrownBy(() -> first.selectedPins(other, List.of(entry), RepositoryReadControl.NONE))
                        .hasMessageContaining("another historical capture");
                var forged = new DocumentHistoricalReadPlan.Entry(entry.revisionOrdinal(), UUID.randomUUID(), entry.part());
                assertThatThrownBy(() -> first.selectedPins(use, List.of(forged), RepositoryReadControl.NONE))
                        .hasMessageContaining("differs from captured");
                assertThatThrownBy(() -> first.selectedPins(use, List.of(), RepositoryReadControl.NONE))
                        .hasMessageContaining("count exceeds bounds");
                assertThatThrownBy(() -> selected.clear()).isInstanceOf(UnsupportedOperationException.class);
                var cancelled = new RepositoryReadControl() {
                    public boolean isCancelled() { return true; }
                    public long remainingNanos() { return Long.MAX_VALUE; }
                };
                assertThatThrownBy(() -> first.selectedPins(use, List.of(entry), cancelled))
                        .isInstanceOfSatisfying(ai.protomolt.proto.repo.spi.RepositoryException.class,
                                failure -> assertThat(failure.code())
                                        .isEqualTo(ai.protomolt.proto.repo.spi.RepositoryException.Code.CANCELLED));
                first.close();
                assertThat(first.isDrained()).isFalse();
                assertThat(first.selectedPins(use, List.of(entry), RepositoryReadControl.NONE)).isEqualTo(selected);
                use.close();
                assertThatThrownBy(() -> first.selectedPins(use, List.of(entry), RepositoryReadControl.NONE))
                        .isInstanceOf(IllegalStateException.class);
            } finally {
                first.close(); second.close();
                assertThat(first.awaitDrained(Duration.ofSeconds(1))).isTrue();
                assertThat(second.awaitDrained(Duration.ofSeconds(1))).isTrue();
                first.release(); second.release();
                ledger.fence(); ledger.attestLocalQuiescence();
            }
            assertThat(count(c, "document_read_pins")).isZero();
        }
    }
}
