package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentAdmissionPolicy;
import ai.protomolt.proto.repo.v1.*;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.flywaydb.core.Flyway;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** PostgreSQL policy-catalog boundaries; these tests do not authorize policy administration. */
@Testcontainers
class DocumentSchemaPoliciesIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void activatesReadsAndAdvancesImmutableSnapshots() {
        try (var c = context(POSTGRES)) {
            var catalog = new DocumentSchemaPolicies(c.tx());
            var first = catalog.activate(policy("account", 1), 0, () -> {});
            assertThat(first.revision()).isEqualTo(1);
            assertSelection(catalog.read("account", () -> {}), first);
            var second = catalog.activate(policy("account", 2), 1, () -> {});
            assertThat(second.revision()).isEqualTo(2);
            assertSelection(catalog.read("account", () -> {}), second);
            assertThat(count(c, "document_schema_policies")).isEqualTo(2);
            assertThat(count(c, "document_schema_policy_current")).isEqualTo(1);
        }
    }

    @Test void pointerRulesAndLockingRefuseInvalidRevisionsAndSnapshotIsolation() {
        try (var c = context(POSTGRES)) {
            var selected = catalog(c).activate(policy("account", 1), 0, () -> {});
            for (String update : List.of("UPDATE document_schema_policy_current SET policy_revision=policy_revision",
                    "UPDATE document_schema_policy_current SET account_id='other',policy_revision=policy_revision+1")) {
                assertThatThrownBy(() -> c.tx().inTransaction(em -> { em.createNativeQuery(update).executeUpdate(); }))
                        .hasStackTraceContaining("Policy update requires the next revision");
            }
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                em.createNativeQuery("INSERT INTO document_schema_policy_current VALUES('other',2,:sha)")
                        .setParameter("sha", new byte[32]).executeUpdate();
            })).hasStackTraceContaining("Initial policy revision must be one");
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                em.createNativeQuery("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ").executeUpdate();
                DocumentSchemaPolicies.lockCurrent(em, selected, () -> {});
            })).hasStackTraceContaining("requires READ COMMITTED isolation");
        }
    }

    @Test void staleCompareAndSetRollsBackTheNewSnapshot() {
        try (var c = context(POSTGRES)) {
            var catalog = new DocumentSchemaPolicies(c.tx());
            catalog.activate(policy("account", 1), 0, () -> {});
            var candidate = policy("account", 3);
            assertThatThrownBy(() -> catalog.activate(candidate, 0, () -> {}))
                    .isInstanceOf(DocumentSchemaPolicies.StalePolicy.class);
            assertThat(count(c, "document_schema_policies")).isEqualTo(1);
            assertThat(catalog.read("account", () -> {}).policy().sha256()).isEqualTo(policy("account", 1).sha256());
            catalog.activate(policy("account", 2), 1, () -> {});
            assertThatThrownBy(() -> catalog.activate(candidate, 1, () -> {}))
                    .isInstanceOf(DocumentSchemaPolicies.StalePolicy.class);
            assertThat(count(c, "document_schema_policies")).isEqualTo(2);
        }
    }

    @Test void abaContentDoesNotMakeAnOldRevisionCurrentAgain() {
        try (var c = context(POSTGRES)) {
            var catalog = new DocumentSchemaPolicies(c.tx());
            var p1 = catalog.activate(policy("account", 1), 0, () -> {});
            catalog.activate(policy("account", 2), 1, () -> {});
            var p1again = catalog.activate(policy("account", 1), 2, () -> {});
            assertThat(p1again.policy().sha256()).isEqualTo(p1.policy().sha256());
            assertThat(p1again.revision()).isEqualTo(3);
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                DocumentSchemaPolicies.lockCurrent(em, p1, () -> {});
            })).isInstanceOf(DocumentSchemaPolicies.StalePolicy.class)
                    .hasMessageContaining("no longer active");
        }
    }

    @Test void policyPointersAreAccountScopedAndMissingPoliciesAreExplicit() {
        try (var c = context(POSTGRES)) {
            var catalog = new DocumentSchemaPolicies(c.tx());
            var accountA = catalog.activate(policy("account-a", 1), 0, () -> {});
            var accountB = catalog.activate(policy("account-b", 2), 0, () -> {});
            assertSelection(catalog.read("account-a", () -> {}), accountA);
            assertSelection(catalog.read("account-b", () -> {}), accountB);
            assertThatThrownBy(() -> catalog.read("account-missing", () -> {}))
                    .isInstanceOf(DocumentSchemaPolicies.StalePolicy.class).hasMessageContaining("No active schema policy");
            assertThatThrownBy(() -> catalog.activate(policy("account-a", 3), 0, () -> {}))
                    .isInstanceOf(DocumentSchemaPolicies.StalePolicy.class);
            assertSelection(catalog.read("account-b", () -> {}), accountB);
        }
    }

    @Test void snapshotsAndPointersRejectMutationSkippedRevisionAndBrokenReferences() {
        try (var c = context(POSTGRES)) {
            var selected = new DocumentSchemaPolicies(c.tx()).activate(policy("account", 1), 0, () -> {});
            for (String sql : List.of("UPDATE document_schema_policies SET policy_bytes=policy_bytes",
                    "DELETE FROM document_schema_policies")) {
                assertThatThrownBy(() -> c.tx().inTransaction(em -> { em.createNativeQuery(sql).executeUpdate(); }))
                        .hasStackTraceContaining("Document schema policy snapshots are immutable");
            }
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                em.createNativeQuery("DELETE FROM document_schema_policy_current").executeUpdate();
            })).hasStackTraceContaining("Document schema policy pointers cannot be deleted");
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                em.createNativeQuery("UPDATE document_schema_policy_current SET policy_revision=policy_revision+2")
                        .executeUpdate();
            })).hasStackTraceContaining("Policy update requires the next revision");
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                em.createNativeQuery("INSERT INTO document_schema_policy_current(account_id,policy_revision,policy_sha256) " +
                        "VALUES('orphan',1,:sha)").setParameter("sha", new byte[32]).executeUpdate();
            })).hasStackTraceContaining("document_schema_policy_current_snapshot");
            assertSelection(catalog(c).read("account", () -> {}), selected);
            assertThat(count(c, "document_schema_policies")).isEqualTo(1);
            assertThat(count(c, "document_schema_policy_current")).isEqualTo(1);
        }
    }

    @Test void cancellationAfterSnapshotInsertRollsBackBothSnapshotAndPointer() {
        for (int checkpoint : List.of(3, 4)) try (var c = context(POSTGRES)) {
            var calls = new AtomicInteger();
            var cancelled = new CancellationException("cancel policy activation");
            assertThatThrownBy(() -> new DocumentSchemaPolicies(c.tx()).activate(policy("account", 1), 0, () -> {
                if (calls.incrementAndGet() == checkpoint) throw cancelled;
            })).isSameAs(cancelled);
            assertThat(calls.get()).isEqualTo(checkpoint);
            assertThat(count(c, "document_schema_policies")).isZero();
            assertThat(count(c, "document_schema_policy_current")).isZero();
        }
    }

    @Test void mismatchedAccountContentCannotBecomeAnAuthoritativeSelection() {
        try (var c = context(POSTGRES)) {
            var other = policy("other", 1);
            c.tx().inTransaction(em -> {
                // SQL enforces bytes and references; only the real decoder can
                // establish that the protobuf account matches its catalog scope.
                em.createNativeQuery("INSERT INTO document_schema_policies VALUES('account',:sha,:codec,1,:bytes)")
                        .setParameter("sha", java.util.HexFormat.of().parseHex(other.sha256()))
                        .setParameter("codec", DocumentAdmissionPolicy.CODEC).setParameter("bytes", other.bytes().toByteArray()).executeUpdate();
                em.createNativeQuery("INSERT INTO document_schema_policy_current VALUES('account',1,:sha)")
                        .setParameter("sha", java.util.HexFormat.of().parseHex(other.sha256())).executeUpdate();
            });
            assertThatThrownBy(() -> catalog(c).read("account", () -> {}))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Invalid account policy selection");
        }
    }

    @Test void migrationPreservesPopulatedV57PublicationsWithoutInventingPolicies() {
        try (var c = context(POSTGRES, "57")) {
            var publication = prepare(c, 1);
            publish(c, publication, Fault.NONE, em -> {});
            var before = c.tx().readOnly(em -> em.createNativeQuery(
                    "SELECT row_to_json(r)::text FROM document_revision_publications r ORDER BY revision_id").getResultList());
            Flyway.configure().dataSource(c.pool()).schemas(c.pool().getSchema()).defaultSchema(c.pool().getSchema())
                    .locations("classpath:db/migration/repo").load().migrate();
            var after = c.tx().readOnly(em -> em.createNativeQuery(
                    "SELECT row_to_json(r)::text FROM document_revision_publications r ORDER BY revision_id").getResultList());
            assertThat(after).isEqualTo(before);
            assertThat(count(c, "document_schema_policies")).isZero();
            assertThat(count(c, "document_schema_policy_current")).isZero();
        }
    }

    private static void assertSelection(DocumentSchemaPolicies.Selection actual, DocumentSchemaPolicies.Selection expected) {
        assertThat(actual.account()).isEqualTo(expected.account());
        assertThat(actual.revision()).isEqualTo(expected.revision());
        assertThat(actual.policy().bytes()).isEqualTo(expected.policy().bytes());
        assertThat(actual.policy().sha256()).isEqualTo(expected.policy().sha256());
    }

    private static DocumentSchemaPolicies catalog(Context c) { return new DocumentSchemaPolicies(c.tx()); }

    private static DocumentAdmissionPolicy policy(String account, int maxFragments) {
        var definition = DocumentSchemaPolicy.newBuilder().setEncodingVersion(1).setAccountId(account)
                .setValidationProfile("protomolt-retained-schema-admission/v1")
                .setMode(DocumentSchemaPolicyMode.DOCUMENT_SCHEMA_POLICY_MODE_TYPED_REQUIRED)
                .setAnyResolvedSchema(true).setLimits(DocumentSchemaPolicyLimits.newBuilder()
                        .setMaxFragments(maxFragments).setMaxFragmentBytes(1024).setMaxRoots(16)
                        .setMaxEvidenceBytes(1024).setMaxBindings(8).setMaxRetainedBytes(4096)
                        .setMaxDecodedBytes(4096)).build();
        return DocumentAdmissionPolicy.of(definition, () -> {});
    }
}
