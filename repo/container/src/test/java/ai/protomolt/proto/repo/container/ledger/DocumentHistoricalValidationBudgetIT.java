package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentAdmissionPolicy;
import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.v1.DocumentSchemaPolicyMode;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** Byte reservations belong to historical validation until its returned proof is closed. */
@Testcontainers
class DocumentHistoricalValidationBudgetIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller ADMIN = new RepositoryCaller("historical-reader", true);

    @Test void holdsEveryReservationUntilValidationCloseAndUsesRetainedPolicy() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = DocumentSchemaRetentionFixture.prepare(c);
            activate(c, f);
            var revision = publish(c, f);

            // A newer live policy cannot replace the exact policy retained by this revision.
            var current = DocumentAdmissionPolicy.of(f.batch().policy().policy().definition().toBuilder()
                    .setMode(DocumentSchemaPolicyMode.DOCUMENT_SCHEMA_POLICY_MODE_OPAQUE_ALLOWED).build(), () -> {});
            new DocumentSchemaPolicies(c.tx()).activate(current, 1, () -> {});

            var ledger = new DocumentReadLedger(new Tx(c.emf()), UUID.randomUUID());
            var history = ledger.captureHistorical(ADMIN, address(f), revision);
            var budget = new PayloadBudget(32L * 1024 * 1024);
            try (var use = history.use()) {
                var validation = history.validateFragments(f.batch().proofs().get("member").fragments(), budget,
                        RepositoryReadControl.NONE);
                assertThat(budget.reservedBytes()).isPositive();
                assertThat(validation.document()).isEqualTo(f.batch().proofs().get("member").document());
                assertThat(validation.validationProfile()).isEqualTo(f.batch().proofs().get("member").validationProfile());
                assertThat(validation.policySha256()).isEqualTo(f.batch().policy().policy().sha256());
                assertThat(validation.commandSha256()).isEqualTo(f.batch().proofs().get("member").commandSha256());
                validation.close();
                assertThat(budget.reservedBytes()).isZero();
                assertThatThrownBy(validation::document).isInstanceOf(IllegalStateException.class);
            }
            release(ledger, history);
        }
    }

    @Test void tinyCapacityRefusesBeforeReturningProofAndReleasesAllLeases() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = DocumentSchemaRetentionFixture.prepare(c); activate(c, f);
            var revision = publish(c, f);
            var ledger = new DocumentReadLedger(new Tx(c.emf()), UUID.randomUUID());
            var history = ledger.captureHistorical(ADMIN, address(f), revision);
            var budget = new PayloadBudget(1);
            try (var use = history.use()) {
                assertThatThrownBy(() -> history.validateFragments(f.batch().proofs().get("member").fragments(),
                        budget, RepositoryReadControl.NONE))
                        .isInstanceOfSatisfying(RepositoryException.class,
                                error -> assertThat(error.code()).isEqualTo(RepositoryException.Code.RESOURCE_EXHAUSTED));
                assertThat(budget.reservedBytes()).isZero();
            }
            release(ledger, history);
        }
    }

    @Test void artifactAdmissionFailureReleasesEarlierCommandAndPolicyReservations() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = DocumentSchemaRetentionFixture.prepare(c); activate(c, f);
            var revision = publish(c, f);
            var ledger = new DocumentReadLedger(new Tx(c.emf()), UUID.randomUUID());
            var history = ledger.captureHistorical(ADMIN, address(f), revision);
            long commandAndPolicyReservation = c.tx().readOnly(em -> {
                Number size = (Number) em.createNativeQuery("""
                        SELECT 2 * (octet_length(o.command) + octet_length(p.policy_bytes))
                        FROM repository_operations o JOIN document_revision_commits c
                          ON c.account_id=o.account_id AND c.principal=o.principal AND c.operation_id=o.operation_id
                        JOIN document_revision_schema_admissions a ON a.revision_id=c.revision_id
                        JOIN document_schema_policies p ON p.account_id=a.account_id AND p.policy_sha256=a.policy_sha256
                        WHERE c.revision_id=:revision
                        """).setParameter("revision", revision).getSingleResult();
                return size.longValue();
            });
            // The next reservation is for retained artifacts, so this admits the command/policy copies only.
            var budget = new PayloadBudget(commandAndPolicyReservation + 1);
            var peak = new java.util.concurrent.atomic.AtomicLong();
            var control = new RepositoryReadControl() {
                @Override public boolean isCancelled() { peak.accumulateAndGet(budget.reservedBytes(), Math::max); return false; }
                @Override public long remainingNanos() { return Long.MAX_VALUE; }
            };
            try (var use = history.use()) {
                assertThatThrownBy(() -> history.validateFragments(f.batch().proofs().get("member").fragments(),
                        budget, control))
                        .isInstanceOfSatisfying(RepositoryException.class,
                                error -> assertThat(error.code()).isEqualTo(RepositoryException.Code.RESOURCE_EXHAUSTED));
                assertThat(peak.get()).isEqualTo(commandAndPolicyReservation);
                assertThat(budget.reservedBytes()).isZero();
            }
            release(ledger, history);
        }
    }

    @Test void cancellationAfterReservationReleasesBytesAndReturnsNoValidation() throws Exception {
        try (var c = context(POSTGRES)) {
            var f = DocumentSchemaRetentionFixture.prepare(c); activate(c, f);
            var revision = publish(c, f);
            var ledger = new DocumentReadLedger(new Tx(c.emf()), UUID.randomUUID());
            var history = ledger.captureHistorical(ADMIN, address(f), revision);
            var budget = new PayloadBudget(32L * 1024 * 1024);
            RepositoryReadControl control = new RepositoryReadControl() {
                @Override public boolean isCancelled() { return budget.reservedBytes() > 0; }
                @Override public long remainingNanos() { return Long.MAX_VALUE; }
            };
            try (var use = history.use()) {
                assertThatThrownBy(() -> history.validateFragments(f.batch().proofs().get("member").fragments(), budget, control))
                        .isInstanceOfSatisfying(RepositoryException.class,
                                error -> assertThat(error.code()).isEqualTo(RepositoryException.Code.CANCELLED));
                assertThat(budget.reservedBytes()).isZero();
            }
            release(ledger, history);
        }
    }

    private static void activate(Context c, DocumentSchemaRetentionFixture.Fixture f) {
        new DocumentSchemaPolicies(c.tx()).activate(f.batch().policy().policy(), 0, () -> {});
    }

    private static UUID publish(Context c, DocumentSchemaRetentionFixture.Fixture f) {
        return DocumentSchemaRetentionFixture.publishBound(c, f, (em, candidate) -> {},
                (em, id, manifest) -> f.retention().write(em, f.owner(), id, () -> {}));
    }

    private static ai.protomolt.proto.repo.v1.NodeAddress address(DocumentSchemaRetentionFixture.Fixture f) {
        return f.command().intent().getMembers(0).getDestination().getAddress();
    }

    private static void release(DocumentReadLedger ledger, DocumentReadLedger.PinnedHistory history) throws Exception {
        history.close();
        assertThat(history.awaitDrained(Duration.ofSeconds(1))).isTrue();
        history.release();
        ledger.fence();
        ledger.attestLocalQuiescence();
    }
}
