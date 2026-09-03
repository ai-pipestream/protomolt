package ai.protomolt.proto.repo.container.lifecycle;

import ai.protomolt.proto.repo.container.blob.DocumentIds;
import ai.protomolt.proto.repo.container.blob.PartStorage;
import ai.protomolt.proto.repo.container.codec.PartLayouts;
import ai.protomolt.proto.repo.container.ledger.DocumentRecord;
import ai.protomolt.proto.repo.container.ledger.DocumentRowKind;
import ai.protomolt.proto.repo.container.ledger.DocumentStatus;
import ai.protomolt.proto.repo.container.ledger.DriveRecord;
import ai.protomolt.proto.repo.v1.Blob;
import ai.protomolt.proto.repo.v1.BlobBag;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.OwnershipContext;
import ai.protomolt.proto.repo.v1.ParseStatus;
import ai.protomolt.proto.repo.v1.ParserDocument;
import ai.protomolt.proto.repo.v1.ParserResult;
import ai.protomolt.proto.repo.v1.PartManifestEntry;
import ai.protomolt.proto.repo.v1.PartState;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.StringValue;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The coherence probe: a part object deleted out from under a manifest gets
 * its manifest entry tombstoned to DELETED/COHERENCE_PROBE — and the next
 * read assembles honestly from the remaining parts. Part objects are written
 * by the real {@link PartStorage}, so assembly sees real fragments.
 */
@Testcontainers(disabledWithoutDocker = true)
class CoherenceProbeIT extends AbstractLifecycleIT {

    @Test
    void missingPartObjectIsTombstonedInTheManifestAndReadsStayHonest() {
        String account = "acct-probe";
        DriveRecord drive = createDrive(account, "docs", "probe-it", "pfx");

        Document doc = Document.newBuilder()
                .setDocId("doc-probe")
                .setOwnership(OwnershipContext.newBuilder()
                        .setAccountId(account)
                        .setDatasourceId("ds-1"))
                .setBlobBag(BlobBag.newBuilder().setBlob(Blob.newBuilder()
                        .setBlobId("blob-1")
                        .setData(ByteString.copyFromUtf8("raw-bytes"))
                        .setMimeType("application/pdf")))
                .putParserResults("tika", ParserResult.newBuilder()
                        .setParserName("tika")
                        .setStatus(ParseStatus.PARSE_STATUS_OK)
                        .setDocument(ParserDocument.newBuilder()
                                .setShape(Any.pack(StringValue.of("tika-exhaust"))))
                        .build())
                .build();
        NodeAddress address = NodeAddress.newBuilder()
                .setDocId(doc.getDocId())
                .setGraphAddressId("ds-1")
                .setAccountId(account)
                .setGraphId("intake:" + account)
                .build();
        UUID nodeId = DocumentIds.nodeId(address);
        String basePrefix = "pfx/documents/" + account + "/" + nodeId;

        PartStorage partStorage = new PartStorage();
        PartStorage.WriteResult written = partStorage.writeParts(store, drive.bucket, basePrefix,
                doc, PartLayouts.document(), address, null, "application/x-protobuf", null, false, 1L);

        DocumentRecord row = new DocumentRecord();
        row.nodeId = nodeId;
        row.docId = doc.getDocId();
        row.graphAddressId = "ds-1";
        row.graphId = "intake:" + account;
        row.rowKind = DocumentRowKind.INTAKE;
        row.accountId = account;
        row.datasourceId = "ds-1";
        row.checksum = written.rootChecksum();
        row.driveName = "docs";
        row.objectKey = basePrefix;
        row.etag = written.coreEtag();
        row.sizeBytes = written.totalSizeBytes();
        row.writeManifest(written.manifest());
        documents.save(row);
        java.time.Instant updatedAtAfterSave = documents.findByNodeId(nodeId).orElseThrow().updatedAt;

        // The PARSED object vanishes out from under the manifest.
        PartManifestEntry parsedEntry = written.manifest().getPartsList().stream()
                .filter(e -> e.getPart() == ai.protomolt.proto.repo.v1.DocumentPart.DOCUMENT_PART_PARSED)
                .findFirst()
                .orElseThrow();
        assertThat(store.delete(drive.bucket, parsedEntry.getObjectKey())).isTrue();

        CoherenceProbe.ProbeReport report = probe.probe(store, 100);

        assertThat(report.rowsExamined()).isEqualTo(1);
        // CORE + BLOBS + PARSED were PRESENT and probed.
        assertThat(report.objectsChecked()).isEqualTo(3);
        assertThat(report.missingByPart())
                .containsExactlyEntriesOf(java.util.Map.of("DOCUMENT_PART_PARSED", 1));
        assertThat(report.totalMissing()).isEqualTo(1);

        // The manifest now says PARSED is gone (COHERENCE_PROBE); the row
        // stays AVAILABLE — the remaining parts are still valid.
        DocumentRecord after = documents.findByNodeId(nodeId).orElseThrow();
        assertThat(after.status).isEqualTo(DocumentStatus.AVAILABLE);
        assertThat(after.updatedAt).isEqualTo(updatedAtAfterSave);
        PartManifestEntry tombstoned = after.readManifest().getPartsList().stream()
                .filter(e -> e.getPart() == ai.protomolt.proto.repo.v1.DocumentPart.DOCUMENT_PART_PARSED)
                .findFirst()
                .orElseThrow();
        assertThat(tombstoned.getState()).isEqualTo(PartState.PART_STATE_DELETED);
        assertThat(tombstoned.getDeletedReason()).isEqualTo(CoherenceProbe.DELETED_REASON);

        // The next read assembles from the remaining parts — the parsed
        // metadata is honestly absent instead of a opaque failure.
        Document assembled = partStorage.readParts(store, drive.bucket, after.readManifest(),
                Set.of(), Set.of(), Document.getDefaultInstance());
        assertThat(assembled).isNotNull();
        assertThat(assembled.getDocId()).isEqualTo(doc.getDocId());
        assertThat(assembled.getParserResultsMap()).isEmpty();
        assertThat(assembled.getBlobBag().getBlob().getData())
                .isEqualTo(ByteString.copyFromUtf8("raw-bytes"));

        // Idempotent: a second probe finds nothing new to tombstone.
        CoherenceProbe.ProbeReport again = probe.probe(store, 100);
        assertThat(again.totalMissing()).isZero();
    }
}
