package ai.pipestream.proto.repo.container.ledger;

import ai.pipestream.proto.repo.v1.DocumentManifest;
import ai.pipestream.proto.repo.v1.DocumentPart;
import ai.pipestream.proto.repo.v1.NodeAddress;
import ai.pipestream.proto.repo.v1.PartManifestEntry;
import ai.pipestream.proto.repo.v1.PartState;
import ai.pipestream.proto.repo.v1.WriteProvenance;
import jakarta.persistence.PersistenceException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Document-ledger behavior against a real PostgreSQL: round-trips through
 * the codec-serialized manifest, the identity constraints, and the
 * updated_at staleness guard.
 */
@Testcontainers
class DocumentLedgerIT {

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17-alpine");

    private static final String ACCOUNT = "acct-1";

    private static DocumentLedger ledger;

    @BeforeAll
    static void boot() {
        LedgerConfig config = new LedgerConfig(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        LedgerDatabase database = new LedgerDatabase(config);
        ledger = new DocumentLedger(new Tx(database.entityManagerFactory()));
        // Deliberately never closed: the container dies with the JVM and the
        // shared pool/EMF serve every test in the class.
    }

    private static DocumentRecord intakeRow(UUID nodeId, String docId, String datasourceId) {
        DocumentRecord record = new DocumentRecord();
        record.nodeId = nodeId;
        record.docId = docId;
        record.graphAddressId = datasourceId;
        record.graphId = "intake:" + ACCOUNT;
        record.rowKind = DocumentRowKind.INTAKE;
        record.accountId = ACCOUNT;
        record.datasourceId = datasourceId;
        record.connectorId = "s3";
        record.checksum = "sha256:" + UUID.randomUUID();
        record.driveName = "intake-drive";
        record.objectKey = "intake/" + docId;
        record.etag = "etag-" + docId;
        record.sizeBytes = 1234L;
        record.contentType = "application/pdf";
        record.filename = docId + ".pdf";
        return record;
    }

    private static DocumentManifest manifestFor(DocumentRecord record) {
        return DocumentManifest.newBuilder()
                .setAddress(addressOf(record))
                .setDocVersion(1)
                .addParts(PartManifestEntry.newBuilder()
                        .setPart(DocumentPart.DOCUMENT_PART_CORE)
                        .setState(PartState.PART_STATE_PRESENT)
                        .setObjectKey(record.objectKey + "/core.bin")
                        .setSha256(record.checksum)
                        .setSizeBytes(record.sizeBytes)
                        .setWrittenBy(WriteProvenance.newBuilder()
                                .setModuleId("intake")
                                .setNodeId(record.graphAddressId)
                                .setGraphId(record.graphId)
                                .setGraphVersion(1)))
                .build();
    }

    private static NodeAddress addressOf(DocumentRecord record) {
        return NodeAddress.newBuilder()
                .setDocId(record.docId)
                .setGraphAddressId(record.graphAddressId)
                .setAccountId(record.accountId)
                .setGraphId(record.graphId)
                .build();
    }

    @Test
    void saveAndFindRoundTripWithManifest() {
        DocumentRecord record = intakeRow(UUID.randomUUID(), "doc-roundtrip", "ds-1");
        DocumentManifest manifest = manifestFor(record);
        record.writeManifest(manifest);
        record.writeSecurity(ai.pipestream.proto.repo.v1.DocumentSecurity.newBuilder()
                .setInheritanceEnabled(true)
                .build());

        DocumentRecord saved = ledger.save(record);
        assertThat(saved.createdAt).isNotNull();
        assertThat(saved.updatedAt).isNotNull();
        assertThat(saved.status).isEqualTo(DocumentStatus.AVAILABLE);

        Optional<DocumentRecord> byNodeId = ledger.findByNodeId(record.nodeId);
        assertThat(byNodeId).isPresent();
        assertThat(byNodeId.get().readManifest()).isEqualTo(manifest);
        assertThat(byNodeId.get().partManifest).isNotBlank();
        assertThat(byNodeId.get().readSecurity().getInheritanceEnabled()).isTrue();

        Optional<DocumentRecord> byReference = ledger.findByReference(addressOf(record));
        assertThat(byReference).isPresent();
        assertThat(byReference.get().nodeId).isEqualTo(record.nodeId);

        Optional<DocumentRecord> locked = ledger.findByReferenceForUpdate(addressOf(record));
        assertThat(locked).isPresent();
        assertThat(locked.get().checksum).isEqualTo(record.checksum);
    }

    @Test
    void duplicateStorageIdentityIsRejected() {
        String docId = "doc-dupe";
        DocumentRecord first = intakeRow(UUID.randomUUID(), docId, "ds-dupe");
        ledger.save(first);

        // Different node_id, SAME four-segment storage identity: the unique
        // constraint is the arbiter.
        DocumentRecord second = intakeRow(UUID.randomUUID(), docId, "ds-dupe");
        assertThatThrownBy(() -> ledger.save(second))
                .isInstanceOf(PersistenceException.class);
    }

    @Test
    void rowKindCheckRejectsMisShapedGraphId() {
        // INTAKE row carrying a pipeline graph id: unrepresentable per the
        // identity rule, enforced by chk_documents_row_kind.
        DocumentRecord bad = intakeRow(UUID.randomUUID(), "doc-badkind", "ds-badkind");
        bad.graphId = "some-pipeline-graph";
        assertThatThrownBy(() -> ledger.save(bad))
                .isInstanceOf(PersistenceException.class);

        // PIPELINE row with a cluster and a real graph id: representable.
        DocumentRecord good = intakeRow(UUID.randomUUID(), "doc-goodkind", "ds-goodkind");
        good.rowKind = DocumentRowKind.PIPELINE;
        good.graphId = "some-pipeline-graph";
        good.clusterId = "cluster-a";
        assertThat(ledger.save(good).nodeId).isEqualTo(good.nodeId);
    }

    @Test
    void markReprocessedBumpsCountButNotUpdatedAt() {
        DocumentRecord record = intakeRow(UUID.randomUUID(), "doc-reprocess", "ds-reprocess");
        ledger.save(record);
        Instant updatedAtAfterSave = ledger.findByNodeId(record.nodeId).orElseThrow().updatedAt;

        Instant dedupeHit = Instant.now();
        Optional<DocumentRecord> marked = ledger.markReprocessed(record.nodeId, dedupeHit);

        assertThat(marked).isPresent();
        assertThat(marked.get().reprocessCount).isEqualTo(1);
        assertThat(marked.get().lastReprocessedAt).isNotNull();

        DocumentRecord reloaded = ledger.findByNodeId(record.nodeId).orElseThrow();
        assertThat(reloaded.reprocessCount).isEqualTo(1);
        // The staleness guard: bookkeeping must NOT move updated_at.
        assertThat(reloaded.updatedAt).isEqualTo(updatedAtAfterSave);

        ledger.markReprocessed(record.nodeId, Instant.now());
        assertThat(ledger.findByNodeId(record.nodeId).orElseThrow().reprocessCount).isEqualTo(2);
    }

    @Test
    void tombstoneFlipsStatusButNotUpdatedAt() {
        DocumentRecord record = intakeRow(UUID.randomUUID(), "doc-tombstone", "ds-tombstone");
        ledger.save(record);
        Instant updatedAtAfterSave = ledger.findByNodeId(record.nodeId).orElseThrow().updatedAt;

        Optional<DocumentRecord> tombstoned = ledger.tombstone(record.nodeId);

        assertThat(tombstoned).isPresent();
        assertThat(tombstoned.get().status).isEqualTo(DocumentStatus.PENDING_PURGE);

        DocumentRecord reloaded = ledger.findByNodeId(record.nodeId).orElseThrow();
        assertThat(reloaded.status).isEqualTo(DocumentStatus.PENDING_PURGE);
        // The staleness guard: a status-only transition must NOT move updated_at.
        assertThat(reloaded.updatedAt).isEqualTo(updatedAtAfterSave);
    }

    @Test
    void listFiltersAndPaginates() {
        String docPrefix = "doc-list-";
        for (int i = 0; i < 5; i++) {
            DocumentRecord record = intakeRow(UUID.randomUUID(), docPrefix + i, "ds-list");
            record.driveName = i < 3 ? "drive-a" : "drive-b";
            record.connectorId = i % 2 == 0 ? "con-x" : "con-y";
            record.crawlId = i < 4 ? "crawl-1" : null;
            ledger.save(record);
        }

        ListDocumentsResult all = ledger.list(new ListDocumentsFilter(
                null, null, null, ACCOUNT, 100, 0));
        assertThat(all.totalCount()).isGreaterThanOrEqualTo(5);

        ListDocumentsResult byDrive = ledger.list(new ListDocumentsFilter(
                "drive-a", null, null, ACCOUNT, 100, 0));
        assertThat(byDrive.rows()).hasSize(3);
        assertThat(byDrive.totalCount()).isEqualTo(3);

        ListDocumentsResult byDriveAndConnector = ledger.list(new ListDocumentsFilter(
                "drive-a", "con-x", null, ACCOUNT, 100, 0));
        assertThat(byDriveAndConnector.rows())
                .allSatisfy(r -> {
                    assertThat(r.driveName).isEqualTo("drive-a");
                    assertThat(r.connectorId).isEqualTo("con-x");
                })
                .hasSize(2);

        ListDocumentsResult byCrawl = ledger.list(new ListDocumentsFilter(
                null, null, "crawl-1", ACCOUNT, 100, 0));
        assertThat(byCrawl.rows()).hasSize(4);

        // Pagination: two offset pages of 2 over the crawl-1 set tile it
        // exactly, in stable (created_at, node_id) order.
        ListDocumentsResult page1 = ledger.list(new ListDocumentsFilter(
                null, null, "crawl-1", ACCOUNT, 2, 0));
        ListDocumentsResult page2 = ledger.list(new ListDocumentsFilter(
                null, null, "crawl-1", ACCOUNT, 2, 2));
        assertThat(page1.rows()).hasSize(2);
        assertThat(page2.rows()).hasSize(2);
        assertThat(page1.totalCount()).isEqualTo(4);
        List<UUID> paged = new java.util.ArrayList<>();
        page1.rows().forEach(r -> paged.add(r.nodeId));
        page2.rows().forEach(r -> paged.add(r.nodeId));
        assertThat(paged).containsExactlyElementsOf(
                byCrawl.rows().stream().map(r -> r.nodeId).toList());
    }

    @Test
    void deleteLogicalRemovesExactlyThatIdentityAndReturnsRows() {
        String docId = "doc-delete";
        String datasourceId = "ds-delete";
        DocumentRecord intake = intakeRow(UUID.randomUUID(), docId, datasourceId);
        ledger.save(intake);
        // A pipeline copy of the same logical document (same datasource).
        DocumentRecord pipeline = intakeRow(UUID.randomUUID(), docId, datasourceId);
        pipeline.rowKind = DocumentRowKind.PIPELINE;
        pipeline.graphAddressId = "parser-node";
        pipeline.graphId = "graph-9";
        ledger.save(pipeline);
        // A different document that must survive.
        DocumentRecord other = intakeRow(UUID.randomUUID(), "doc-other", datasourceId);
        ledger.save(other);

        List<DocumentRecord> removed = ledger.deleteLogical(docId, ACCOUNT, datasourceId);

        assertThat(removed)
                .extracting(r -> r.nodeId)
                .containsExactlyInAnyOrder(intake.nodeId, pipeline.nodeId);
        assertThat(removed).allSatisfy(r -> assertThat(r.objectKey).isNotBlank());
        assertThat(ledger.findByNodeId(intake.nodeId)).isEmpty();
        assertThat(ledger.findByNodeId(pipeline.nodeId)).isEmpty();
        assertThat(ledger.findByNodeId(other.nodeId)).isPresent();

        // deleteByReference removes exactly one storage identity.
        DocumentRecord victim = intakeRow(UUID.randomUUID(), "doc-victim", "ds-victim");
        ledger.save(victim);
        Optional<DocumentRecord> byRef = ledger.deleteByReference(addressOf(victim));
        assertThat(byRef).isPresent();
        assertThat(ledger.findByNodeId(victim.nodeId)).isEmpty();

        // deleteByNodeId on a missing row is a clean empty.
        assertThat(ledger.deleteByNodeId(UUID.randomUUID())).isEmpty();
    }
}
