package ai.pipestream.proto.repo.container.ledger;

import ai.pipestream.proto.repo.v1.Access;
import ai.pipestream.proto.repo.v1.AccessRule;
import ai.pipestream.proto.repo.v1.DocumentManifest;
import ai.pipestream.proto.repo.v1.DocumentPart;
import ai.pipestream.proto.repo.v1.DocumentSecurity;
import ai.pipestream.proto.repo.v1.NodeAddress;
import ai.pipestream.proto.repo.v1.PartManifestEntry;
import ai.pipestream.proto.repo.v1.PartState;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link DocumentRecord}'s JSON column helpers (manifest and security
 * round-trips through the claim-check codec, null clearing, parse-failure
 * types) and the {@code @PrePersist} timestamp defaulting that must never
 * become a {@code @PreUpdate} (the staleness guard depends on it).
 */
class DocumentRecordTest {

    @Test
    void manifestRoundTripsThroughTheJsonColumn() {
        DocumentRecord record = new DocumentRecord();
        DocumentManifest manifest = DocumentManifest.newBuilder()
                .setAddress(NodeAddress.newBuilder()
                        .setDocId("d1").setGraphAddressId("ds-1")
                        .setAccountId("acct-1").setGraphId("intake:acct-1"))
                .setDocVersion(4)
                .addParts(PartManifestEntry.newBuilder()
                        .setPart(DocumentPart.DOCUMENT_PART_CORE)
                        .setState(PartState.PART_STATE_PRESENT)
                        .setObjectKey("p/core.pb").setSha256("ab").setSizeBytes(10))
                .build();

        record.writeManifest(manifest);

        assertThat(record.partManifest).isNotBlank();
        assertThat(record.readManifest()).isEqualTo(manifest);
    }

    @Test
    void aNullManifestWritesNullAndReadsNull() {
        DocumentRecord record = new DocumentRecord();

        assertThat(record.readManifest()).isNull();

        record.writeManifest(DocumentManifest.newBuilder().setDocVersion(1).build());
        assertThat(record.readManifest()).isNotNull();
        record.writeManifest(null); // null clears the column
        assertThat(record.partManifest).isNull();
        assertThat(record.readManifest()).isNull();
    }

    @Test
    void anUnparseableManifestSurfacesAsIllegalState() {
        DocumentRecord record = new DocumentRecord();
        record.nodeId = UUID.randomUUID();
        record.partManifest = "{ broken json";

        // The codec owns manifest (de)serialization; its parse failure type
        // is IllegalStateException, not LedgerException.
        assertThatThrownBy(record::readManifest)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Manifest JSON parse failed");
    }

    @Test
    void securityRoundTripsThroughTheJsonColumn() {
        DocumentRecord record = new DocumentRecord();
        DocumentSecurity security = DocumentSecurity.newBuilder()
                .setInheritanceEnabled(true)
                .addPermissions(AccessRule.newBuilder()
                        .setIdentity("alice").setIdentityType("cmis-user")
                        .setDisplayName("Alice").setAccess(Access.ACCESS_READ))
                .addPermissions(AccessRule.newBuilder()
                        .setIdentity("bob").setIdentityType("public")
                        .setAccess(Access.ACCESS_DENY))
                .build();

        record.writeSecurity(security);

        assertThat(record.security).isNotBlank();
        assertThat(record.readSecurity()).isEqualTo(security);
    }

    @Test
    void aNullSecurityWritesNullAndReadsNull() {
        DocumentRecord record = new DocumentRecord();

        assertThat(record.readSecurity()).isNull();

        record.writeSecurity(DocumentSecurity.newBuilder().setInheritanceEnabled(false).build());
        assertThat(record.readSecurity()).isNotNull();
        record.writeSecurity(null);
        assertThat(record.security).isNull();
        assertThat(record.readSecurity()).isNull();
    }

    @Test
    void anUnparseableSecuritySurfacesAsLedgerExceptionNamingTheRow() {
        DocumentRecord record = new DocumentRecord();
        record.nodeId = UUID.randomUUID();
        record.security = "{ broken json";

        assertThatThrownBy(record::readSecurity)
                .isInstanceOf(LedgerException.class)
                .hasMessageContaining(record.nodeId.toString());
    }

    @Test
    void prePersistDefaultsBothAuditTimestamps() {
        DocumentRecord record = new DocumentRecord();

        record.onPrePersist();

        assertThat(record.createdAt).isNotNull();
        assertThat(record.updatedAt).isNotNull();
    }

    @Test
    void prePersistNeverOverwritesExplicitTimestamps() {
        DocumentRecord record = new DocumentRecord();
        Instant created = Instant.parse("2026-01-01T00:00:00Z");
        Instant updated = Instant.parse("2026-02-02T00:00:00Z");
        record.createdAt = created;
        record.updatedAt = updated;

        record.onPrePersist();

        assertThat(record.createdAt).isEqualTo(created);
        assertThat(record.updatedAt).isEqualTo(updated);
    }

    @Test
    void newRowsDefaultToAvailableWithNoReprocessing() {
        DocumentRecord record = new DocumentRecord();

        assertThat(record.status).isEqualTo(DocumentStatus.AVAILABLE);
        assertThat(record.reprocessCount).isZero();
        assertThat(record.lastReprocessedAt).isNull();
        assertThat(record.deleteSourceBlobsOnSettle).isFalse();
    }
}
