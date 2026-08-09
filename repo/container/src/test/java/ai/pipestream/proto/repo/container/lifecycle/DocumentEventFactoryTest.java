package ai.pipestream.proto.repo.container.lifecycle;

import ai.pipestream.proto.repo.container.ledger.DocumentEventRecord;
import ai.pipestream.proto.repo.container.ledger.DocumentPurgeRecord;
import ai.pipestream.proto.repo.container.ledger.DocumentRecord;
import ai.pipestream.proto.repo.v1.DocumentEvent;
import ai.pipestream.proto.repo.v1.DocumentManifest;
import ai.pipestream.proto.repo.v1.NodeAddress;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link DocumentEventFactory}: each commit point's outbox row carries the
 * right event type, the doc_id Kafka key, the event-id echo, and the exact
 * commit instant — and the packaged descriptor set really contains
 * document_events.proto and its transitive imports.
 */
class DocumentEventFactoryTest {

    private static final Instant WHEN = Instant.parse("2026-08-09T12:34:56.123456789Z");

    @Test
    void savedBuildsADocumentSavedOutboxRow() throws Exception {
        DocumentRecord row = row();
        row.writeManifest(DocumentManifest.newBuilder().setDocVersion(7).build());

        DocumentEventRecord record = DocumentEventFactory.saved(row, WHEN);

        assertThat(record.eventId).isNotNull();
        assertThat(record.eventType).isEqualTo(DocumentEventRecord.TYPE_SAVED);
        assertThat(record.kafkaKey).isEqualTo(row.docId); // partition-ordered per document
        assertThat(record.createdAt).isEqualTo(WHEN);
        assertThat(record.status).isEqualTo(DocumentEventRecord.STATUS_PENDING);
        assertThat(record.attempts).isZero();

        DocumentEvent event = DocumentEvent.parseFrom(record.payload);
        assertThat(event.getEventId()).isEqualTo(record.eventId.toString()); // the dedupe key
        assertThat(event.hasSaved()).isTrue();
        assertThat(event.getSaved().getAddress()).isEqualTo(addressOf(row));
        assertThat(event.getSaved().getChecksum()).isEqualTo(row.checksum);
        assertThat(event.getSaved().getDocVersion()).isEqualTo(7);
        assertThat(event.getSaved().getSizeBytes()).isEqualTo(1234L);
        assertThat(event.getSaved().getSavedAt().getSeconds()).isEqualTo(WHEN.getEpochSecond());
        assertThat(event.getSaved().getSavedAt().getNanos()).isEqualTo(WHEN.getNano());
    }

    @Test
    void savedDefaultsDocVersionAndSizeWhenTheRowCarriesNeither() throws Exception {
        DocumentRecord row = row();
        row.partManifest = null; // no manifest: version 0
        row.sizeBytes = null;

        DocumentEvent event = DocumentEvent.parseFrom(
                DocumentEventFactory.saved(row, WHEN).payload);

        assertThat(event.getSaved().getDocVersion()).isZero();
        assertThat(event.getSaved().getSizeBytes()).isZero();
    }

    @Test
    void savedSurvivesAnUnparseableManifestWithDocVersionZero() throws Exception {
        DocumentRecord row = row();
        row.partManifest = "{ broken json"; // docVersion() deliberately swallows this

        DocumentEvent event = DocumentEvent.parseFrom(
                DocumentEventFactory.saved(row, WHEN).payload);

        assertThat(event.getSaved().getDocVersion()).isZero();
    }

    @Test
    void deletedBuildsADocumentDeletedOutboxRow() throws Exception {
        DocumentRecord row = row();

        DocumentEventRecord record = DocumentEventFactory.deleted(row, WHEN);

        assertThat(record.eventType).isEqualTo(DocumentEventRecord.TYPE_DELETED);
        assertThat(record.kafkaKey).isEqualTo(row.docId);
        DocumentEvent event = DocumentEvent.parseFrom(record.payload);
        assertThat(event.getEventId()).isEqualTo(record.eventId.toString());
        assertThat(event.hasDeleted()).isTrue();
        assertThat(event.getDeleted().getAddress()).isEqualTo(addressOf(row));
        assertThat(event.getDeleted().getChecksum()).isEqualTo(row.checksum);
        assertThat(event.getDeleted().getDeletedAt().getSeconds()).isEqualTo(WHEN.getEpochSecond());
        assertThat(event.getDeleted().getDeletedAt().getNanos()).isEqualTo(WHEN.getNano());
    }

    @Test
    void deletedMapsANullChecksumToEmpty() throws Exception {
        DocumentRecord row = row();
        row.checksum = null;

        DocumentEvent event = DocumentEvent.parseFrom(
                DocumentEventFactory.deleted(row, WHEN).payload);

        assertThat(event.getDeleted().getChecksum()).isEmpty();
    }

    @Test
    void purgeRequestedBuildsAPurgeRequestedOutboxRow() throws Exception {
        DocumentPurgeRecord purge = purge();

        DocumentEventRecord record = DocumentEventFactory.purgeRequested(purge, "sha256:root", WHEN);

        assertThat(record.eventType).isEqualTo(DocumentEventRecord.TYPE_PURGE_REQUESTED);
        assertThat(record.kafkaKey).isEqualTo(purge.docId);
        assertThat(record.createdAt).isEqualTo(WHEN);
        DocumentEvent event = DocumentEvent.parseFrom(record.payload);
        assertThat(event.hasPurgeRequested()).isTrue();
        assertThat(event.getPurgeRequested().getPurgeId()).isEqualTo(purge.purgeId.toString());
        assertThat(event.getPurgeRequested().getAddress().getDocId()).isEqualTo(purge.docId);
        assertThat(event.getPurgeRequested().getAddress().getGraphAddressId())
                .isEqualTo(purge.graphAddressId);
        assertThat(event.getPurgeRequested().getAddress().getAccountId()).isEqualTo(purge.accountId);
        assertThat(event.getPurgeRequested().getAddress().getGraphId()).isEqualTo(purge.graphId);
        assertThat(event.getPurgeRequested().getChecksum()).isEqualTo("sha256:root");
        assertThat(event.getPurgeRequested().getRequestedAt().getNanos()).isEqualTo(WHEN.getNano());
    }

    @Test
    void purgedBuildsADocumentPurgedOutboxRowAndMapsANullChecksumToEmpty() throws Exception {
        DocumentPurgeRecord purge = purge();

        DocumentEventRecord record = DocumentEventFactory.purged(purge, null, WHEN);

        assertThat(record.eventType).isEqualTo(DocumentEventRecord.TYPE_PURGED);
        assertThat(record.kafkaKey).isEqualTo(purge.docId);
        DocumentEvent event = DocumentEvent.parseFrom(record.payload);
        assertThat(event.hasPurged()).isTrue();
        assertThat(event.getPurged().getPurgeId()).isEqualTo(purge.purgeId.toString());
        assertThat(event.getPurged().getChecksum()).isEmpty(); // row already gone: unknown
        assertThat(event.getPurged().getPurgedAt().getSeconds()).isEqualTo(WHEN.getEpochSecond());
    }

    @Test
    void eachEventGetsItsOwnId() {
        DocumentRecord row = row();

        DocumentEventRecord a = DocumentEventFactory.saved(row, WHEN);
        DocumentEventRecord b = DocumentEventFactory.saved(row, WHEN);

        assertThat(a.eventId).isNotEqualTo(b.eventId);
        assertThat(a.payload).isNotEqualTo(b.payload); // the id rides the payload
    }

    @Test
    void theDescriptorSetContainsTheContractAndItsTransitiveImports() throws Exception {
        FileDescriptorSet set = FileDescriptorSet.parseFrom(
                Base64.getDecoder().decode(DocumentEventFactory.descriptorSetBase64()));
        List<String> files = set.getFileList().stream().map(f -> f.getName()).toList();

        assertThat(files).contains(
                "ai/pipestream/proto/repo/v1/document_events.proto",
                "ai/pipestream/proto/repo/v1/address.proto",
                "google/protobuf/timestamp.proto");
    }

    private static DocumentRecord row() {
        DocumentRecord row = new DocumentRecord();
        row.nodeId = UUID.randomUUID();
        row.docId = "doc-1";
        row.graphAddressId = "ds-1";
        row.accountId = "acct-1";
        row.graphId = "intake:acct-1";
        row.checksum = "sha256:abc";
        row.sizeBytes = 1234L;
        return row;
    }

    private static DocumentPurgeRecord purge() {
        DocumentPurgeRecord purge = new DocumentPurgeRecord();
        purge.purgeId = UUID.randomUUID();
        purge.nodeId = UUID.randomUUID();
        purge.docId = "doc-1";
        purge.graphAddressId = "ds-1";
        purge.accountId = "acct-1";
        purge.graphId = "intake:acct-1";
        purge.driveName = "docs";
        purge.writeObjectKeys(List.of("k/1"));
        purge.requestedAt = WHEN;
        return purge;
    }

    private static NodeAddress addressOf(DocumentRecord row) {
        return NodeAddress.newBuilder()
                .setDocId(row.docId)
                .setGraphAddressId(row.graphAddressId)
                .setAccountId(row.accountId)
                .setGraphId(row.graphId)
                .build();
    }
}
