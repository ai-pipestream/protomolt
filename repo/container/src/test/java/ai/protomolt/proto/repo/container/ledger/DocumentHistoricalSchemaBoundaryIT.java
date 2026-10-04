package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.container.blob.DocumentIds;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import com.google.protobuf.ByteString;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** Real SQL authorization race; physical observations are synthetic, not a provider-read test. */
@Testcontainers
class DocumentHistoricalSchemaBoundaryIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @ParameterizedTest @ValueSource(booleans = {true, false})
    void revocationAfterSnapshotSuppressesBothProofAndValidationDetails(boolean validFragments) throws Exception {
        try (var c = context(POSTGRES)) {
            var f = DocumentSchemaRetentionFixture.prepare(c);
            new DocumentSchemaPolicies(c.tx()).activate(f.batch().policy().policy(), 0, () -> {});
            var revision = DocumentSchemaRetentionFixture.publishBound(c, f, (em, candidate) -> {},
                    (em, id, manifest) -> f.retention().write(em, f.owner(), id, () -> {}));
            var original = f.batch().proofs().get("member");
            var address = original.member().getDestination().getAddress();
            var node = DocumentIds.nodeId(address);
            c.tx().inTransaction(em -> { em.createNativeQuery("UPDATE documents SET security=CAST(:policy AS jsonb) WHERE node_id=:node")
                    .setParameter("node", node).setParameter("policy",
                            "{\"permissions\":[{\"identityType\":\"public\",\"identity\":\"public\",\"access\":\"ACCESS_READ\"}]}")
                    .executeUpdate(); });
            var caller = new RepositoryCaller("historical-reader", false, Set.of("account"), Set.of());
            var fragments = new HashMap<>(original.fragments());
            if (!validFragments) fragments.put(fragments.keySet().iterator().next(), ByteString.copyFromUtf8("wrong fragment"));
            var observedCaptureLock = new AtomicBoolean();
            var revoked = new AtomicBoolean();
            long key = node.getMostSignificantBits() ^ node.getLeastSignificantBits();
            Runnable control = () -> {
                if (revoked.get()) return;
                // Probe a separate connection without blocking. This follows the real lock lifetime,
                // not an assumed callback count or a production test hook.
                c.tx().inTransaction(em -> {
                    boolean free = (Boolean) em.createNativeQuery("SELECT pg_try_advisory_xact_lock(:key)")
                            .setParameter("key", key).getSingleResult();
                    if (!free) observedCaptureLock.set(true);
                    else if (observedCaptureLock.get()) {
                        em.createNativeQuery("UPDATE documents SET security=CAST('{}' AS jsonb) WHERE node_id=:node")
                                .setParameter("node", node).executeUpdate();
                        revoked.set(true);
                    }
                });
            };
            assertThatThrownBy(() -> new DocumentHistoricalSchemas(c.tx()).check(caller, address, revision, fragments, control))
                    .isInstanceOfSatisfying(RepositoryException.class,
                            failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.NOT_FOUND))
                    .hasMessage("Document is unavailable");
            assertThat(observedCaptureLock).isTrue();
            assertThat(revoked).isTrue();
        }
    }
}
