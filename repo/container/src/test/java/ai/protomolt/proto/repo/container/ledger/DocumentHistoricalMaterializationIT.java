package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentSchemaMaterialization;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.container.blob.DocumentIds;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.v1.DocumentRootSchemaEvidence;
import ai.protomolt.proto.repo.v1.NodeAddress;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** Real PostgreSQL revision/ACL/pin behavior; provider observations remain synthetic in publication fixtures. */
@Testcontainers
class DocumentHistoricalMaterializationIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller ADMIN = new RepositoryCaller("historical-reader", true);
    private static final DocumentSchemaMaterialization.Limits LIMITS =
            new DocumentSchemaMaterialization.Limits(4_000_000, 4_000_000, 16_000_000, 64, 4_000_000, 64);

    @Test void returnedContentOwnsAPinUseAndReservationsUntilClose() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = fixture(c); var ledger = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var history = ledger.captureHistorical(ADMIN, f.address, f.revision); var budget = new PayloadBudget(32L * 1024 * 1024);
            var peak = new java.util.concurrent.atomic.AtomicLong();
            var control = new RepositoryReadControl() {
                public long remainingNanos() { return Long.MAX_VALUE; }
                public boolean isCancelled() { peak.accumulateAndGet(budget.reservedBytes(), Math::max); return false; }
            };
            var result = history.materializeFragment(f.ordinal, f.fragment, f.selection, LIMITS, budget, control);
            long pins = c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM document_read_pins").getSingleResult()).longValue());
            assertThat(pins).isPositive();
            assertThat(budget.reservedBytes()).isLessThan(peak.get()); // Complete SQL snapshot and scratch already released.
            history.close();
            assertThat(history.awaitDrained(Duration.ZERO)).isFalse();
            assertThatThrownBy(history::release).isInstanceOf(IllegalStateException.class);
            assertThat(budget.reservedBytes()).isPositive();
            var view = result.view(RepositoryReadControl.NONE);
            assertThat(view.original()).isEqualTo(f.original);
            assertThat(view.value().getDescriptorForType().getFullName()).isEqualTo(f.schemaType);
            assertThat(view.occurrence().ordinal()).isEqualTo(f.ordinal);
            result.close(); result.close();
            assertThat(budget.reservedBytes()).isZero();
            assertThatThrownBy(() -> result.view(RepositoryReadControl.NONE)).isInstanceOf(IllegalStateException.class);
            release(ledger, history);
            assertThat(c.tx().<Long>readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM document_read_pins").getSingleResult()).longValue())).isZero();
        }
    }

    @Test void unknownRootPathAndOrdinalAreNotFoundRatherThanCorruption() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = fixture(c); var ledger = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var history = ledger.captureHistorical(ADMIN, f.address, f.revision); var budget = new PayloadBudget(32L * 1024 * 1024);
            for (var selected : java.util.List.of(new DocumentSchemaMaterialization.Selection("a".repeat(64), f.selection.pathSha256()),
                    new DocumentSchemaMaterialization.Selection(f.selection.rootSha256(), "a".repeat(64)))) {
                assertCode(() -> history.materializeFragment(f.ordinal, f.fragment, selected, LIMITS, budget, RepositoryReadControl.NONE), RepositoryException.Code.NOT_FOUND);
                assertThat(budget.reservedBytes()).isZero();
            }
            assertCode(() -> history.materializeFragment(9999, f.fragment, f.selection, LIMITS, budget, RepositoryReadControl.NONE), RepositoryException.Code.NOT_FOUND);
            assertThat(budget.reservedBytes()).isZero();
            release(ledger, history);
        }
    }

    @Test void badFragmentCapacityAndCancellationReleaseTheirPinUsesAndBytes() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = fixture(c); var ledger = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var history = ledger.captureHistorical(ADMIN, f.address, f.revision); var budget = new PayloadBudget(32L * 1024 * 1024);
            assertCode(() -> history.materializeFragment(f.ordinal, ByteString.copyFromUtf8("wrong"), f.selection, LIMITS, budget,
                    RepositoryReadControl.NONE), RepositoryException.Code.DATA_LOSS);
            assertThat(budget.reservedBytes()).isZero();
            var tiny = new PayloadBudget(1);
            assertCode(() -> history.materializeFragment(f.ordinal, f.fragment, f.selection, LIMITS, tiny,
                    RepositoryReadControl.NONE), RepositoryException.Code.RESOURCE_EXHAUSTED);
            assertThat(tiny.reservedBytes()).isZero();
            var cancellation = new RepositoryReadControl() {
                public boolean isCancelled() { return budget.reservedBytes() > 0; }
                public long remainingNanos() { return Long.MAX_VALUE; }
            };
            assertCode(() -> history.materializeFragment(f.ordinal, f.fragment, f.selection, LIMITS, budget,
                    cancellation), RepositoryException.Code.CANCELLED);
            assertThat(budget.reservedBytes()).isZero();
            release(ledger, history);
        }
    }

    @Test void revocationAfterDecodeSuppressesDeliveryAndDoesNotReleaseOwnershipEarly() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = fixture(c); grant(c, f.address, true);
            var reader = new RepositoryCaller("reader", false, Set.of("account"), Set.of());
            var ledger = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var history = ledger.captureHistorical(reader, f.address, f.revision); var budget = new PayloadBudget(32L * 1024 * 1024);
            try (var result = history.materializeFragment(f.ordinal, f.fragment, f.selection, LIMITS, budget, RepositoryReadControl.NONE)) {
                assertThat(result.view(RepositoryReadControl.NONE).original()).isEqualTo(f.original);
                grant(c, f.address, false);
                assertCode(() -> result.view(RepositoryReadControl.NONE), RepositoryException.Code.NOT_FOUND);
                assertThat(budget.reservedBytes()).isPositive();
                history.close(); assertThat(history.isDrained()).isFalse();
            }
            assertThat(budget.reservedBytes()).isZero(); release(ledger, history);
        }
    }

    @ParameterizedTest @ValueSource(booleans = {true, false})
    void revocationDuringDecodeSuppressesSuccessAndCorruptionDetails(boolean valid) throws Exception {
        try (var c = context(POSTGRES)) {
            var f = fixture(c); grant(c, f.address, true);
            var reader = new RepositoryCaller("reader", false, Set.of("account"), Set.of());
            var ledger = new DocumentReadLedger(c.tx(), UUID.randomUUID());
            var history = ledger.captureHistorical(reader, f.address, f.revision); var budget = new PayloadBudget(32L * 1024 * 1024);
            var observedLock = new AtomicBoolean(); var revoked = new AtomicBoolean();
            var node = DocumentIds.nodeId(f.address); long key = node.getMostSignificantBits() ^ node.getLeastSignificantBits();
            var control = new RepositoryReadControl() {
                public long remainingNanos() { return Long.MAX_VALUE; }
                public boolean isCancelled() {
                    if (!revoked.get()) c.tx().inTransaction(em -> {
                        boolean free = (Boolean) em.createNativeQuery("SELECT pg_try_advisory_xact_lock(:key)").setParameter("key", key).getSingleResult();
                        if (!free) observedLock.set(true);
                        else if (observedLock.get()) {
                            em.createNativeQuery("UPDATE documents SET security=CAST('{}' AS jsonb) WHERE node_id=:node")
                                    .setParameter("node", node).executeUpdate(); revoked.set(true);
                        }
                    });
                    return false;
                }
            };
            assertCode(() -> history.materializeFragment(f.ordinal, valid ? f.fragment : ByteString.copyFromUtf8("wrong"),
                    f.selection, LIMITS, budget, control), RepositoryException.Code.NOT_FOUND);
            assertThat(observedLock).isTrue(); assertThat(revoked).isTrue();
            assertThat(budget.reservedBytes()).isZero(); release(ledger, history);
        }
    }

    @Test void wrongAccountCannotCaptureOrMaterializeExistingOrRandomHistory() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = fixture(c); var reader = new RepositoryCaller("other", false, Set.of("elsewhere"), Set.of());
            var ledger = new DocumentReadLedger(c.tx(), UUID.randomUUID()); var budget = new PayloadBudget(32L * 1024 * 1024);
            for (var revision : java.util.List.of(f.revision, UUID.randomUUID())) {
                assertCode(() -> ledger.captureHistorical(reader, f.address, revision), RepositoryException.Code.NOT_FOUND);
                assertCode(() -> new DocumentHistoricalMaterializer(c.tx()).read(reader, f.address, revision,
                        f.ordinal, f.fragment, f.selection, LIMITS, budget, RepositoryReadControl.NONE), RepositoryException.Code.NOT_FOUND);
                assertThat(budget.reservedBytes()).isZero();
            }
            ledger.fence(); ledger.attestLocalQuiescence();
        }
    }

    private static Fixture fixture(Context c) throws Exception {
        var f = DocumentSchemaRetentionFixture.prepare(c);
        new DocumentSchemaPolicies(c.tx()).activate(f.batch().policy().policy(), 0, () -> {});
        var revision = DocumentSchemaRetentionFixture.publishBound(c, f, (em, candidate) -> {},
                (em, id, manifest) -> f.retention().write(em, f.owner(), id, () -> {}));
        var proof = f.batch().proofs().get("member"); var root = proof.roots().getFirst();
        var evidence = DocumentRootSchemaEvidence.parseFrom(root.encoded().bytes()); var path = evidence.getOccurrences(0);
        return new Fixture(proof.member().getDestination().getAddress(), revision, root.ordinal(), proof.fragments().get(root.ordinal()),
                new DocumentSchemaMaterialization.Selection(root.locatorSha256(), DocumentPartCodec.sha256Hex(path.toByteArray())),
                proof.document().getStructuredData(), path.getSteps(0).getAnyBoundary().getResolved().getSchema().getTypeName());
    }
    private static void grant(Context c, NodeAddress address, boolean allowed) {
        c.tx().inTransaction(em -> { em.createNativeQuery("UPDATE documents SET security=CAST(:policy AS jsonb) WHERE node_id=:node")
                .setParameter("node", DocumentIds.nodeId(address)).setParameter("policy", allowed
                        ? "{\"permissions\":[{\"identityType\":\"public\",\"identity\":\"public\",\"access\":\"ACCESS_READ\"}]}" : "{}")
                .executeUpdate(); });
    }
    private static void release(DocumentReadLedger ledger, DocumentReadLedger.PinnedHistory history) throws Exception {
        history.close(); assertThat(history.awaitDrained(Duration.ofSeconds(1))).isTrue(); history.release();
        ledger.fence(); ledger.attestLocalQuiescence();
    }
    private static void assertCode(org.assertj.core.api.ThrowableAssert.ThrowingCallable action, RepositoryException.Code code) {
        assertThatThrownBy(action).isInstanceOfSatisfying(RepositoryException.class, failure -> assertThat(failure.code()).isEqualTo(code));
    }
    private record Fixture(NodeAddress address, UUID revision, int ordinal, ByteString fragment,
            DocumentSchemaMaterialization.Selection selection, com.google.protobuf.Any original, String schemaType) {}
}
