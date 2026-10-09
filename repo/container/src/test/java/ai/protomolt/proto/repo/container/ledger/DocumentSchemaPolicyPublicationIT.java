package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentAdmissionPolicy;
import ai.protomolt.proto.repo.blob.spi.BackendIdentity;
import ai.protomolt.proto.repo.v1.*;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** SQL fence tests for publication attempts that have no schema-policy binding path yet. */
@Testcontainers
class DocumentSchemaPolicyPublicationIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void configuredPolicyRejectsNativePublicationAndPreservesExistingRevisionRows() {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1);
            var before = revisionRows(c);
            new DocumentSchemaPolicies(c.tx()).activate(policy("account", true), 0, () -> {});
            assertThatThrownBy(() -> publish(c, p, Fault.NONE, em -> {}))
                    .hasStackTraceContaining("Publication requires an explicit schema policy binding");
            assertThat(revisionRows(c)).isEqualTo(before);
            assertThat(count(c, "document_revision_commits")).isZero();
            assertThat(count(c, "repository_operation_success")).isZero();
        }
    }

    @Test void configuredPolicyRejectsLegacyManagedDocumentPublication() {
        try (var c = context(POSTGRES)) {
            var profile = new ManagedBackendLedger.Profile(new BackendIdentity("test-location", "test-location/v1",
                    Map.of("endpoint", "synthetic")), "legacy-policy-test");
            new ManagedBackendLedger(c.tx()).bind("legacy-policy-test", profile);
            var drive = drive(c);
            var address = NodeAddress.newBuilder().setAccountId("account").setDocId("legacy-doc")
                    .setGraphId("graph").setGraphAddressId("node").build();
            new DocumentSchemaPolicies(c.tx()).activate(policy("account", true), 0, () -> {});
            var before = revisionRows(c);
            assertThatThrownBy(() -> ManagedDocumentFixture.publish(c.tx(), drive, "legacy-policy-test", profile,
                    address, DocumentSecurity.getDefaultInstance(), 2, 5, "synthetic-version"))
                    .hasStackTraceContaining("Publication requires an explicit schema policy binding");
            assertThat(revisionRows(c)).isEqualTo(before);
            assertThat(count(c, "documents")).isZero();
        }
    }

    @Test void noConfiguredPolicyKeepsTheExistingNativePublicationPathAvailable() {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1);
            var result = publish(c, p, Fault.NONE, em -> {});
            assertThat(result.getMembersCount()).isEqualTo(1);
            assertThat(count(c, "document_revision_commits")).isEqualTo(1);
            assertThat(count(c, "repository_operation_success")).isEqualTo(1);
        }
    }

    @Test void opaquePermittedPolicyStillRejectsAnOlderUnboundPublisher() {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1);
            new DocumentSchemaPolicies(c.tx()).activate(policy("account", false), 0, () -> {});
            assertThatThrownBy(() -> publish(c, p, Fault.NONE, em -> {}))
                    .hasStackTraceContaining("Publication requires an explicit schema policy binding");
            assertThat(count(c, "document_revision_commits")).isZero();
            assertThat(count(c, "repository_operation_success")).isZero();
        }
    }

    @Test void unmanagedDocumentBodiesCannotBypassPolicyByOmittingRevisionPublication() {
        try (var c = context(POSTGRES)) {
            var ledger = new DocumentLedger(c.tx());
            var row = new DocumentRecord();
            row.nodeId = UUID.randomUUID(); row.docId = "unmanaged"; row.accountId = "account";
            row.graphId = "graph"; row.graphAddressId = "node"; row.rowKind = DocumentRowKind.PIPELINE;
            row.datasourceId = "source"; row.objectKey = "old-unmanaged-object";
            row.checksum = "fixture-checksum"; row.driveName = "fixture-drive";
            row.etag = "fixture-etag"; row.sizeBytes = 1L;
            var stored = ledger.save(row);
            new DocumentSchemaPolicies(c.tx()).activate(policy("account", true), 0, () -> {});
            stored.objectKey = "changed-unmanaged-object";
            assertThatThrownBy(() -> ledger.save(stored))
                    .hasStackTraceContaining("Publication requires an explicit schema policy binding");
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                em.createNativeQuery("UPDATE documents SET object_key='direct-sql-object' WHERE node_id=:id")
                        .setParameter("id", row.nodeId).executeUpdate();
            })).hasStackTraceContaining("Publication requires an explicit schema policy binding");
            assertThat(ledger.findByNodeId(row.nodeId).orElseThrow().objectKey).isEqualTo("old-unmanaged-object");
            var bookkeeping = ledger.markReprocessed(row.nodeId, java.time.Instant.now()).orElseThrow();
            assertThat(bookkeeping.reprocessCount).isEqualTo(1);
            assertThat(bookkeeping.objectKey).isEqualTo("old-unmanaged-object");
            row.nodeId = UUID.randomUUID(); row.docId = "unmanaged-new";
            assertThatThrownBy(() -> ledger.save(row))
                    .hasStackTraceContaining("Publication requires an explicit schema policy binding");
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { em.persist(row); em.flush(); }))
                    .hasStackTraceContaining("Publication requires an explicit schema policy binding");
            assertThat(count(c, "documents")).isEqualTo(1);
        }
    }

    @Test void accountIdentityCannotBeRetaggedToEscapePolicy() {
        try (var c = context(POSTGRES)) {
            var p = prepare(c, 1);
            new DocumentSchemaPolicies(c.tx()).activate(policy("account", true), 0, () -> {});
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                em.createNativeQuery("UPDATE documents SET account_id='other' WHERE node_id=:id")
                        .setParameter("id", p.sources().getFirst().row().nodeId).executeUpdate();
            })).hasStackTraceContaining("Document account identity is immutable");
            assertThat(new DocumentLedger(c.tx()).findByNodeId(p.sources().getFirst().row().nodeId).orElseThrow().accountId)
                    .isEqualTo("account");
        }
    }

    private static Object revisionRows(Context c) {
        return c.tx().readOnly(em -> em.createNativeQuery("""
                SELECT row_to_json(r)::text FROM document_revision_publications r ORDER BY revision_id
                """).getResultList());
    }

    private static DriveRecord drive(Context c) {
        var drive = new DriveRecord(); drive.driveId = UUID.randomUUID(); drive.accountId = "account";
        drive.name = "policy-test"; drive.bucket = "bucket"; drive.prefix = "root";
        drive.provider = "test-location"; drive.driveType = "CUSTOM"; drive.status = "ACTIVE";
        new DriveLedger(c.tx()).insert(drive);
        return drive;
    }

    private static DocumentAdmissionPolicy policy(String account, boolean typed) {
        return DocumentAdmissionPolicy.of(DocumentSchemaPolicy.newBuilder().setEncodingVersion(1).setAccountId(account)
                .setValidationProfile("protomolt-retained-schema-admission/v1")
                .setMode(typed ? DocumentSchemaPolicyMode.DOCUMENT_SCHEMA_POLICY_MODE_TYPED_REQUIRED
                        : DocumentSchemaPolicyMode.DOCUMENT_SCHEMA_POLICY_MODE_OPAQUE_ALLOWED)
                .setAnyResolvedSchema(true).setLimits(DocumentSchemaPolicyLimits.newBuilder()
                        .setMaxFragments(100).setMaxFragmentBytes(4_000_000).setMaxRoots(100)
                        .setMaxEvidenceBytes(4_000_000).setMaxBindings(20).setMaxRetainedBytes(16_000_000)
                        .setMaxDecodedBytes(1_000_000)).build(), () -> {});
    }
}
