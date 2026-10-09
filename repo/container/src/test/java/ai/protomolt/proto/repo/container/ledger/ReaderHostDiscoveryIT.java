package ai.protomolt.proto.repo.container.ledger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.context;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
class ReaderHostDiscoveryIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void exactReceiptPagesAllStatesWithoutCrossingHostOrLocalBindings() throws Exception {
        try (var c = context(POSTGRES); var child = new ReaderHostTerminationIT.ManagedChild()) {
            var host = child.identity.execution();
            ReaderHostExecutions.register(c.tx(), host, child.identity.host(), child.identity.boot());
            // UUIDs straddle Java's signed comparison boundary; SQL determines cursor ordering.
            var ids = List.of(UUID.fromString("00000000-0000-0000-0000-000000000001"),
                    UUID.fromString("80000000-0000-0000-0000-000000000001"),
                    UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff"));
            for (var id : ids) ReaderRegistration.register(c.tx(), id, host);
            ReaderRegistration.register(c.tx(), UUID.randomUUID());
            var otherHost = UUID.randomUUID();
            ReaderHostExecutions.register(c.tx(), otherHost, "other", "boot");
            ReaderRegistration.register(c.tx(), UUID.randomUUID(), otherHost);
            c.tx().inTransaction(em -> {
                em.createNativeQuery("SELECT fence_repository_reader(:id)").setParameter("id", ids.get(1)).getSingleResult();
                em.createNativeQuery("SELECT fence_repository_reader(:id)").setParameter("id", ids.get(2)).getSingleResult();
                em.createNativeQuery("SELECT attest_local_reader_quiescence(:id)").setParameter("id", ids.get(2)).getSingleResult();
            });
            var discovery = new ReaderHostDiscovery(c.tx());
            assertThatThrownBy(() -> discovery.discover(host, UUID.randomUUID(), Optional.empty(), 1))
                    .hasMessageContaining("verified host termination");
            ReaderHostExecutions.fence(c.tx(), host); child.stop();
            var receipt = new ReaderHostTermination(c.tx(), Map.of("managed-child", child::verify)).record(child.identity, child.proof());
            assertThatThrownBy(() -> discovery.discover(otherHost, receipt.id(), Optional.empty(), 1))
                    .hasMessageContaining("verified host termination");
            assertThatThrownBy(() -> discovery.discover(host, UUID.randomUUID(), Optional.of(ids.getLast()), 1))
                    .hasMessageContaining("verified host termination");
            for (int invalid : new int[]{0, -1, 1001})
                assertThatThrownBy(() -> discovery.discover(host, receipt.id(), Optional.empty(), invalid))
                        .isInstanceOf(IllegalArgumentException.class);
            Optional<UUID> cursor = Optional.empty();
            var states = List.of(ReaderHostDiscovery.State.ACTIVE, ReaderHostDiscovery.State.FENCED, ReaderHostDiscovery.State.QUIESCED);
            for (int i = 0; i < ids.size(); i++) {
                var page = discovery.discover(host, receipt.id(), cursor, 1);
                assertThat(page.readers()).hasSize(1);
                var reader = page.readers().getFirst();
                assertThat(reader.incarnation()).isEqualTo(ids.get(i));
                assertThat(reader.state()).isEqualTo(states.get(i));
                assertThat(reader.registrationNonce()).isNotNull();
                if (i < 2) {
                    new ReaderExternalQuiescence(c.tx()).quiesce(reader.incarnation(), reader.registrationNonce(), host, receipt.id());
                } else assertThat(reader.quiescence()).isEqualTo(ReaderHostDiscovery.Quiescence.LOCAL_DRAIN);
                cursor = page.nextCursor();
            }
            var empty = discovery.discover(host, receipt.id(), cursor, 1);
            assertThat(empty.readers()).isEmpty(); assertThat(empty.nextCursor()).isEmpty();
            var retry = discovery.discover(host, receipt.id(), Optional.empty(), 1000);
            assertThat(retry.readers()).extracting(ReaderHostDiscovery.Reader::incarnation).containsExactlyElementsOf(ids);
            assertThat(retry.readers()).allMatch(reader -> reader.state() == ReaderHostDiscovery.State.QUIESCED);
            assertThat(retry.readers().getFirst().quiescence()).isEqualTo(ReaderHostDiscovery.Quiescence.HOST_TERMINATION);
            // A failed constructor can reserve a terminal tombstone behind the completed cursor.
            var late = UUID.fromString("00000000-0000-0000-0000-000000000000");
            assertThatThrownBy(() -> ReaderRegistration.register(c.tx(), late, host))
                    .isInstanceOfSatisfying(ReaderRegistration.Failure.class,
                            failure -> assertThat(failure.cleanup()).isEqualTo(ReaderRegistration.Cleanup.QUIESCED));
            assertThat(discovery.discover(host, receipt.id(), cursor, 1).readers()).isEmpty();
            var restarted = discovery.discover(host, receipt.id(), Optional.empty(), 1);
            assertThat(restarted.readers()).hasSize(1);
            assertThat(restarted.readers().getFirst().incarnation()).isEqualTo(late);
            assertThat(restarted.readers().getFirst().quiescence()).isEqualTo(ReaderHostDiscovery.Quiescence.LOCAL_DRAIN);
        }
    }
}
