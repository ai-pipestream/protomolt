package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.*;
import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.v1.Blob;
import ai.protomolt.proto.repo.v1.BlobWrite;
import ai.protomolt.proto.repo.v1.BlobBag;
import ai.protomolt.proto.repo.v1.Blobs;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.FileStorageReference;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v2.*;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Raw blob claim checks (requirements §6, decision 45): each raw blob is its own
 * stack object bound to its blob ID, the server alone fills {@code storage_ref},
 * and carry-forward and part sources reuse the bound objects without re-upload.
 */
class RawBlobClaimCheckIT {
    private AccessionFixture f;
    private String account;

    @BeforeEach void open() throws Exception {
        f = new AccessionFixture();
        account = "acct-" + UUID.randomUUID();
    }

    @AfterEach void close() { f.close(); }

    @Test void twoRawBlobsAreTwoStackObjectsAndTwoBindings() throws Exception {
        var address = address(account, "doc-blobs");
        var document = twoBlobs(document(account, "doc-blobs", "v1"));
        f.stack.reset();
        f.saves.save(processAuthority(), create("op-1", address, document).build());
        assertThat(f.stack.puts.get()).as("two raw blobs plus CORE, BLOBS and PARSED").isEqualTo(5);

        var stored = f.catalog.manifest(address, 1).orElseThrow();
        var blobs = blobsEntry(stored);
        assertThat(blobs.getBlobRefsList()).extracting(BlobObjectRef::getBlobId).containsExactly("b1", "b2");
        assertThat(blobs.getBlobRefs(0).getRef().getKey()).isNotEqualTo(blobs.getBlobRefs(1).getRef().getKey());
        assertThat(f.blobBytes(stored, "b1")).isEqualTo("first raw file".getBytes());
        assertThat(f.blobBytes(stored, "b2")).isEqualTo("second raw file".getBytes());
        assertThat(blobRefRows(address)).containsExactlyInAnyOrder(
                row(1, "b1", blobs.getBlobRefs(0).getRef()), row(1, "b2", blobs.getBlobRefs(1).getRef()));

        // The stored BLOBS fragment holds metadata only; the read fills each claim check.
        try (var fragment = f.stack.open(blobs.getRef())) {
            var parsed = Document.parseFrom(fragment.readAllBytes());
            assertThat(parsed.getBlobBag().getBlobs().getBlobList()).allSatisfy(blob -> {
                assertThat(blob.getContentCase()).isEqualTo(Blob.ContentCase.CONTENT_NOT_SET);
                assertThat(blob.getMimeType()).isEqualTo("text/plain");
            });
        }
        var read = f.reads.getDocument(processAuthority(), GetDocumentRequest.newBuilder().setAddress(address).build());
        assertThat(read.getDocument()).isEqualTo(claimChecked(document, stored));
        assertThat(read.getDocument().getBlobBag().getBlobs().getBlob(1).getStorageRef()).isEqualTo(
                FileStorageReference.newBuilder().setDriveName(f.stack.stackId())
                        .setObjectKey(blobs.getBlobRefs(1).getRef().getKey()).build());
    }

    @Test void clientStorageReferencesAndBlobsWithoutDataAreRefusedUnrecorded() {
        var address = address(account, "doc-forged");
        var forged = document(account, "doc-forged", "v1").toBuilder().setBlobBag(BlobBag.newBuilder().setBlob(
                Blob.newBuilder().setBlobId("b1").setStorageRef(FileStorageReference.newBuilder()
                        .setDriveName(f.stack.stackId()).setObjectKey(f.prefix + "/someone-elses-object")))).build();
        var empty = document(account, "doc-forged", "v1").toBuilder().setBlobBag(BlobBag.newBuilder().setBlob(
                Blob.newBuilder().setBlobId("b1").setMimeType("text/plain"))).build();
        var duplicate = document(account, "doc-forged", "v1").toBuilder().setBlobBag(BlobBag.newBuilder().setBlobs(
                Blobs.newBuilder().addBlob(raw("b1", "one")).addBlob(raw("b1", "two")))).build();
        var blank = document(account, "doc-forged", "v1").toBuilder().setBlobBag(BlobBag.newBuilder().setBlob(
                raw(" ", "one"))).build();
        f.stack.reset();
        for (var document : List.of(forged, empty, duplicate, blank)) {
            assertThatThrownBy(() -> f.saves.save(processAuthority(), create("op-forged", address, document).build()))
                    .isInstanceOfSatisfying(AccessionException.class, failure -> {
                        assertThat(failure.code()).isEqualTo(Status.Code.INVALID_ARGUMENT);
                        assertThat(((SaveRejection) failure.detail().orElseThrow()).getReason())
                                .isEqualTo(RejectionReason.REJECTION_REASON_MALFORMED_REQUEST);
                    });
        }
        assertThat(f.stack.puts.get()).isZero();
        assertThat(f.catalog.outcome(account, "op-forged")).isEmpty();
        assertThat(f.catalog.current(address)).isEmpty();
    }

    @Test void carryForwardAndPartSourcesReuseTheBoundObjectsWithoutUpload() throws Exception {
        var address = address(account, "doc-shared");
        var document = twoBlobs(document(account, "doc-shared", "v1"));
        f.saves.save(processAuthority(), create("op-1", address, document).build());
        var first = blobsEntry(f.catalog.manifest(address, 1).orElseThrow());

        // Carry-forward: a CORE-only save keeps the BLOBS entry and its bindings.
        f.stack.reset();
        f.saves.save(processAuthority(), SaveDocumentRequest.newBuilder().setOperationId("op-2")
                .setTarget(DocumentTarget.newBuilder().setAddress(address)).setExpected(at(1))
                .setDocument(Document.newBuilder().setDocId("doc-shared").setOwnership(document.getOwnership())
                        .setSearchMetadata(document.getSearchMetadata().toBuilder().setTitle("v2")))
                .addWrites(put(DocumentPart.DOCUMENT_PART_CORE)).setOwner(intakeOwner()).build());
        assertThat(f.stack.puts.get()).as("CORE only").isEqualTo(1);
        var carried = blobsEntry(f.catalog.manifest(address, 2).orElseThrow());
        assertThat(carried).isEqualTo(first);

        // A part source: another document with the same doc_id reuses the bytes by reference.
        var other = NodeAddress.newBuilder().setDocId("doc-shared").setGraphAddressId("ds-2").setAccountId(account)
                .setGraphId("intake:" + account).build();
        var otherDocument = Document.newBuilder().setDocId("doc-shared").setOwnership(document.getOwnership().toBuilder()
                .setDatasourceId("ds-2")).setSearchMetadata(document.getSearchMetadata()).build();
        f.stack.reset();
        f.saves.save(processAuthority(), SaveDocumentRequest.newBuilder().setOperationId("op-3")
                .setTarget(DocumentTarget.newBuilder().setAddress(other)).setExpected(ExpectedRevision.newBuilder().setNone(true))
                .setDocument(otherDocument).addWrites(put(DocumentPart.DOCUMENT_PART_CORE))
                .addSources(PartSource.newBuilder().setSelector(PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_BLOBS))
                        .setFromAddress(address).setFromRevision(1))
                .setOwner(SaveOwner.newBuilder().setIntake(IntakeOrigin.newBuilder().setDatasourceId("ds-2"))).build());
        assertThat(f.stack.puts.get()).as("CORE only; the sourced BLOBS part and its raw blobs are reused").isEqualTo(1);
        var sourced = blobsEntry(f.catalog.manifest(other, 1).orElseThrow());
        assertThat(sourced.getRef()).isEqualTo(first.getRef());
        // The same objects, bound in the sourcing revision: a sourced part is written in
        // that revision, so its bindings carry that revision's stamp.
        var otherStored = f.catalog.manifest(other, 1).orElseThrow();
        assertThat(sourced.getEntry().getWrittenAt()).as("a sourced part is written in the sourcing revision")
                .isEqualTo(otherStored.getCommittedAt());
        assertThat(sourced.getBlobRefsList()).extracting(BlobObjectRef::getBlobId, BlobObjectRef::getRef)
                .containsExactlyElementsOf(first.getBlobRefsList().stream()
                        .map(ref -> tuple(ref.getBlobId(), ref.getRef())).toList());
        assertThat(sourced.getBlobRefsList()).allSatisfy(ref -> assertThat(ref.getWritten()).isEqualTo(
                BlobWrite.newBuilder().setRevision(1)
                        .setCommittedAt(otherStored.getCommittedAt()).build()));
        var read = f.reads.getDocument(processAuthority(), GetDocumentRequest.newBuilder().setAddress(other)
                .setSelection(PartSelection.newBuilder().addSelectors(PartSelector.newBuilder()
                        .setPart(DocumentPart.DOCUMENT_PART_BLOBS))).build());
        assertThat(read.getDocument().getBlobBag()).isEqualTo(claimChecked(document, otherStored).getBlobBag());

        // Both documents reference the same raw blob objects: the reference set a purge
        // of one document counts before deleting bytes (purge itself is #318).
        for (var ref : first.getBlobRefsList())
            assertThat(documentsReferencing(ref.getRef())).isEqualTo(2);
    }

    @Test void aPurgedRawBlobCannotBeCarriedForward() throws Exception {
        var address = address(account, "doc-purged");
        var document = twoBlobs(document(account, "doc-purged", "v1"));
        f.saves.save(processAuthority(), create("op-1", address, document).build());
        var blob = blobsEntry(f.catalog.manifest(address, 1).orElseThrow()).getBlobRefs(1).getRef();
        // Purge marking is #318's; mark the row as its first step would.
        try (var connection = DriverManager.getConnection(f.url, POSTGRES.getUsername(), POSTGRES.getPassword());
             var update = connection.prepareStatement(
                     "UPDATE catalog_objects SET purged = true WHERE stack_id = ? AND object_key = ?")) {
            update.setString(1, blob.getStackId());
            update.setString(2, blob.getKey());
            assertThat(update.executeUpdate()).isEqualTo(1);
        }
        var request = SaveDocumentRequest.newBuilder().setOperationId("op-2")
                .setTarget(DocumentTarget.newBuilder().setAddress(address)).setExpected(at(1))
                .setDocument(Document.newBuilder().setDocId("doc-purged").setOwnership(document.getOwnership())
                        .setSearchMetadata(document.getSearchMetadata().toBuilder().setTitle("v2")))
                .addWrites(put(DocumentPart.DOCUMENT_PART_CORE)).setOwner(intakeOwner()).build();
        assertThatThrownBy(() -> f.saves.save(processAuthority(), request))
                .isInstanceOfSatisfying(AccessionException.class, failure ->
                        assertThat(((SaveRejection) failure.detail().orElseThrow()).getReason())
                                .isEqualTo(RejectionReason.REJECTION_REASON_SOURCE_PURGED));
        assertThat(f.catalog.current(address).orElseThrow().revision()).isEqualTo(1);
    }

    // ------------------------------------------------------------------ helpers

    private static Document twoBlobs(Document document) {
        return document.toBuilder().setBlobBag(BlobBag.newBuilder().setBlobs(Blobs.newBuilder()
                .addBlob(raw("b1", "first raw file")).addBlob(raw("b2", "second raw file")))).build();
    }

    private static Blob raw(String id, String content) {
        return Blob.newBuilder().setBlobId(id).setMimeType("text/plain").setSizeBytes(content.length())
                .setData(ByteString.copyFromUtf8(content)).build();
    }

    private static StoredPartEntry blobsEntry(StoredManifest manifest) {
        return manifest.getPartsList().stream()
                .filter(part -> part.getEntry().getSelector().getPart() == DocumentPart.DOCUMENT_PART_BLOBS)
                .findFirst().orElseThrow();
    }

    private static String row(long revision, String blobId, StackRef ref) {
        return revision + "|" + blobId + "|" + ref.getStackId() + "|" + ref.getKey();
    }

    private List<String> blobRefRows(NodeAddress address) throws Exception {
        try (var connection = DriverManager.getConnection(f.url, POSTGRES.getUsername(), POSTGRES.getPassword());
             var select = connection.prepareStatement("""
                     SELECT b.revision, b.blob_id, b.stack_id, b.object_key FROM catalog_blob_refs b
                     JOIN catalog_documents d ON d.document_id = b.document_id
                     WHERE d.account_id = ? AND d.graph_address_id = ? AND d.doc_id = ?""")) {
            select.setString(1, address.getAccountId());
            select.setString(2, address.getGraphAddressId());
            select.setString(3, address.getDocId());
            var rows = new ArrayList<String>();
            try (var result = select.executeQuery()) {
                while (result.next()) rows.add(result.getLong(1) + "|" + result.getString(2) + "|" + result.getString(3)
                        + "|" + result.getString(4));
            }
            return rows;
        }
    }

    private int documentsReferencing(StackRef ref) throws Exception {
        try (var connection = DriverManager.getConnection(f.url, POSTGRES.getUsername(), POSTGRES.getPassword());
             var select = connection.prepareStatement("""
                     SELECT count(DISTINCT document_id) FROM catalog_blob_refs WHERE stack_id = ? AND object_key = ?""")) {
            select.setString(1, ref.getStackId());
            select.setString(2, ref.getKey());
            try (var result = select.executeQuery()) {
                result.next();
                return result.getInt(1);
            }
        }
    }
}
