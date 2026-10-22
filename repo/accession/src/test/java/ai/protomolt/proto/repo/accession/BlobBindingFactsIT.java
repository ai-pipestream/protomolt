package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.*;
import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.v1.Blob;
import ai.protomolt.proto.repo.v1.BlobBag;
import ai.protomolt.proto.repo.v1.BlobWrite;
import ai.protomolt.proto.repo.v1.Blobs;
import ai.protomolt.proto.repo.v1.ChecksumType;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v2.*;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.grpc.Status;
import java.io.ByteArrayInputStream;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The v2 prerequisites of the archive over v2 documents (archive design, "The
 * archive over v2 documents", §2): blob-level carry-forward in a BLOBS PUT,
 * server-filled blob facts with the write that bound each blob, the recorded
 * schema subject, and the accession's commit time on every manifest.
 */
class BlobBindingFactsIT {
    private static final String ORIGINAL = "the original rendition, large in production";
    private static final String MARKDOWN = "# the markdown rendition";

    private AccessionFixture f;
    private String account;

    @BeforeEach void open() throws Exception {
        f = new AccessionFixture();
        account = "acct-" + UUID.randomUUID();
    }

    @AfterEach void close() { f.close(); }

    @Test void aBlobNamedByIdIsCarriedForwardWithItsOriginalStampAndNoUpload() throws Exception {
        var address = address(account, "doc-carry");
        f.saves.save(processAuthority(), create("op-1", address, bag(account, "doc-carry",
                raw("original", ORIGINAL), raw("thumbnail", "a thumbnail"))).build());
        var first = f.catalog.manifest(address, 1).orElseThrow();
        var originalBinding = binding(first, "original");
        assertThat(originalBinding.getWritten()).isEqualTo(BlobWrite.newBuilder().setRevision(1)
                .setCommittedAt(first.getCommittedAt()).build());

        // Revision 2 names "original" by blob_id only, adds "markdown" inline and drops "thumbnail".
        f.stack.reset();
        var saved = f.saves.save(processAuthority(), blobsOnly("op-2", address, 1,
                carried("original").setFilename("report.pdf").build(), raw("markdown", MARKDOWN)));
        assertThat(f.stack.puts.get()).as("the markdown blob and the BLOBS fragment; the original is not sent again")
                .isEqualTo(2);

        var second = f.catalog.manifest(address, 2).orElseThrow();
        assertThat(blobsEntry(second).getBlobRefsList()).extracting(BlobObjectRef::getBlobId)
                .containsExactly("original", "markdown");
        assertThat(binding(second, "original")).as("the same object, the same stamp").isEqualTo(originalBinding);
        assertThat(binding(second, "markdown").getWritten()).isEqualTo(BlobWrite.newBuilder().setRevision(2)
                .setCommittedAt(second.getCommittedAt()).build());
        assertThat(saved.getAccession().getManifest().getCommittedAt()).isEqualTo(second.getCommittedAt());
        // Part entries record their write time the same way: CORE and PARSED were carried, BLOBS was written.
        assertThat(part(second, DocumentPart.DOCUMENT_PART_CORE).getWrittenAt()).isEqualTo(first.getCommittedAt());
        assertThat(part(second, DocumentPart.DOCUMENT_PART_PARSED).getWrittenAt()).isEqualTo(first.getCommittedAt());
        assertThat(part(second, DocumentPart.DOCUMENT_PART_BLOBS).getWrittenAt()).isEqualTo(second.getCommittedAt());
        assertThat(saved.getAccession().getManifest().getPartsList())
                .extracting(PartManifestEntry::getWrittenAt).containsOnly(first.getCommittedAt(), second.getCommittedAt());

        var read = f.reads.getDocument(processAuthority(), GetDocumentRequest.newBuilder().setAddress(address).build());
        var blobs = read.getDocument().getBlobBag().getBlobs().getBlobList();
        assertThat(blobs).extracting(Blob::getBlobId).containsExactly("original", "markdown");
        assertThat(blobs.get(0).getFilename()).isEqualTo("report.pdf");
        assertFacts(blobs.get(0), ORIGINAL, originalBinding.getWritten());
        assertFacts(blobs.get(1), MARKDOWN, binding(second, "markdown").getWritten());
        assertThat(f.blobBytes(second, "original")).isEqualTo(ORIGINAL.getBytes());
    }

    @Test void aStreamedBlobBesideACarriedBlobUploadsOnlyTheStreamedBytes() throws Exception {
        var address = address(account, "doc-carry-upload");
        f.saves.save(processAuthority(), create("op-1", address, bag(account, "doc-carry-upload",
                raw("original", ORIGINAL))).build());
        var originalBinding = binding(f.catalog.manifest(address, 1).orElseThrow(), "original");

        f.stack.reset();
        var payload = MARKDOWN.getBytes();
        var header = UploadHeader.newBuilder()
                .setSave(blobsOnly("op-2", address, 1, carried("original").build(),
                        Blob.newBuilder().setBlobId("markdown").setMimeType("text/markdown")
                                .setSizeBytes(payload.length).setChecksum(SaveIntents.sha256Hex(payload)).build()))
                .setBlobId("markdown").setDeclaredLength(payload.length).build();
        var uploaded = f.saves.upload(processAuthority(), header, new ByteArrayInputStream(payload));
        assertThat(f.stack.puts.get()).as("the streamed blob and the BLOBS fragment").isEqualTo(2);

        var second = f.catalog.manifest(address, 2).orElseThrow();
        assertThat(binding(second, "original")).isEqualTo(originalBinding);
        assertThat(binding(second, "markdown").getWritten().getRevision()).isEqualTo(2);
        assertThat(uploaded.getSaved().getAccession().getManifest().getCommittedAt()).isEqualTo(second.getCommittedAt());
    }

    @Test void aCarriedBlobWhoseObjectWasPurgedFailsTheCommitAsSourcePurged() throws Exception {
        var address = address(account, "doc-carry-purged");
        f.saves.save(processAuthority(), create("op-1", address, bag(account, "doc-carry-purged",
                raw("original", ORIGINAL))).build());
        var object = binding(f.catalog.manifest(address, 1).orElseThrow(), "original").getRef();
        // Purge marking is the purge path's first step; mark the object row as it would.
        try (var connection = DriverManager.getConnection(f.url, POSTGRES.getUsername(), POSTGRES.getPassword());
             var update = connection.prepareStatement(
                     "UPDATE catalog_objects SET purged = true WHERE stack_id = ? AND object_key = ?")) {
            update.setString(1, object.getStackId());
            update.setString(2, object.getKey());
            assertThat(update.executeUpdate()).isEqualTo(1);
        }
        var request = blobsOnly("op-2", address, 1, carried("original").build(), raw("markdown", MARKDOWN));
        assertThatThrownBy(() -> f.saves.save(processAuthority(), request))
                .isInstanceOfSatisfying(AccessionException.class, failure ->
                        assertThat(((SaveRejection) failure.detail().orElseThrow()).getReason())
                                .isEqualTo(RejectionReason.REJECTION_REASON_SOURCE_PURGED));
        assertThat(f.catalog.current(address).orElseThrow().revision()).isEqualTo(1);
    }

    @Test void aBlobWithoutDataMustNameABlobOfTheExpectedRevision() {
        var address = address(account, "doc-carry-unknown");
        // On a create there is no expected revision to carry from.
        assertMalformed(create("op-create", address, bag(account, "doc-carry-unknown",
                carried("original").build())).build(), "names no raw blob of the expected revision");
        assertThat(f.catalog.outcome(account, "op-create")).isEmpty();

        f.saves.save(processAuthority(), create("op-1", address, bag(account, "doc-carry-unknown",
                raw("original", ORIGINAL))).build());
        f.stack.reset();
        assertMalformed(blobsOnly("op-2", address, 1, carried("missing").build()),
                "names no raw blob of the expected revision");
        // A carried blob's declared facts must describe the bound object.
        assertMalformed(blobsOnly("op-3", address, 1, carried("original").setSizeBytes(ORIGINAL.length() + 1).build()),
                "declares size_bytes");
        assertMalformed(blobsOnly("op-4", address, 1, carried("original").setChecksum("0".repeat(64)).build()),
                "declares SHA-256");
        assertThat(f.stack.puts.get()).isZero();
        for (var operation : List.of("op-2", "op-3", "op-4")) assertThat(f.catalog.outcome(account, operation)).isEmpty();
        assertThat(f.catalog.current(address).orElseThrow().revision()).isEqualTo(1);

        // Declared facts that match are accepted.
        f.saves.save(processAuthority(), blobsOnly("op-5", address, 1, carried("original")
                .setSizeBytes(ORIGINAL.length()).setChecksum(SaveIntents.sha256Hex(ORIGINAL.getBytes()).toUpperCase())
                .setChecksumType(ChecksumType.CHECKSUM_TYPE_SHA256).build()));
        assertThat(f.catalog.current(address).orElseThrow().revision()).isEqualTo(2);
    }

    @Test void readsFillSizeChecksumWrittenAndTheRecordedSchemaSubject() throws Exception {
        var address = address(account, "doc-facts");
        var credential = UUID.randomUUID();
        f.catalog.registerCredential(ISSUER, credential, "parser");
        f.stepGrant(account, credential, "parser", true);
        var owner = stepOwner("parser");
        var subject = "ai.protomolt.proto.repo.v1.Document-value";
        var declared = Blob.newBuilder().setBlobId("declared").setMimeType("application/x-protobuf")
                .setSchemaSubject(subject).setData(ByteString.copyFromUtf8(ORIGINAL)).build();
        var silent = Blob.newBuilder().setBlobId("silent").setData(ByteString.copyFromUtf8(MARKDOWN)).build();
        var saved = f.saves.save(writer(account, "parser", credential, 1),
                create("op-1", address, bag(account, "doc-facts", declared, silent)).setOwner(owner).build());
        var stored = f.catalog.manifest(address, 1).orElseThrow();
        var stamp = BlobWrite.newBuilder().setRevision(1).setCommittedAt(stored.getCommittedAt())
                .setStep(owner.getStep()).build();

        var read = f.reads.getDocument(processAuthority(), GetDocumentRequest.newBuilder().setAddress(address).build());
        var blobs = read.getDocument().getBlobBag().getBlobs().getBlobList();
        assertFacts(blobs.get(0), ORIGINAL, stamp);
        assertThat(blobs.get(0).getSchemaSubject()).isEqualTo(subject);
        assertFacts(blobs.get(1), MARKDOWN, stamp);
        assertThat(blobs.get(1).hasSchemaSubject()).isFalse();

        // The streamed read that fills claim checks fills the same facts, and its manifest frame
        // carries the commit time.
        var frames = new ArrayList<ReadDocumentResponse>();
        f.reads.readDocument(processAuthority(), ReadDocumentRequest.newBuilder().setAddress(address).build(), frames::add);
        assertThat(frames.getFirst().getManifest().getCommittedAt()).isEqualTo(stored.getCommittedAt());
        var merged = Document.newBuilder();
        var buffered = new java.util.EnumMap<DocumentPart, ByteString>(DocumentPart.class);
        for (var frame : frames)
            if (frame.hasPartData())
                buffered.merge(frame.getPartData().getSelector().getPart(), frame.getPartData().getData(), ByteString::concat);
        for (var bytes : buffered.values()) merged.mergeFrom(bytes);
        assertThat(merged.build()).isEqualTo(read.getDocument());

        // committed_at is the accession's commit time on every save and read surface.
        var committedAt = stored.getCommittedAt();
        assertThat(saved.getAccession().getCommittedAt()).isEqualTo(committedAt);
        assertThat(saved.getAccession().getManifest().getCommittedAt()).isEqualTo(committedAt);
        assertThat(read.getManifest().getCommittedAt()).isEqualTo(committedAt);
        assertThat(f.reads.getManifest(processAuthority(), GetManifestRequest.newBuilder().setAddress(address).build())
                .getManifest().getCommittedAt()).isEqualTo(committedAt);
    }

    @Test void eachRevisionsManifestCarriesItsOwnCommitTime() {
        var address = address(account, "doc-times");
        f.saves.save(processAuthority(), create("op-1", address, bag(account, "doc-times", raw("original", ORIGINAL))).build());
        f.saves.save(processAuthority(), blobsOnly("op-2", address, 1, carried("original").build(), raw("markdown", MARKDOWN)));
        var first = f.reads.getManifest(processAuthority(), GetManifestRequest.newBuilder().setAddress(address)
                .setRevision(RevisionSelector.newBuilder().setRevision(1)).build()).getManifest();
        var second = f.reads.getManifest(processAuthority(), GetManifestRequest.newBuilder().setAddress(address)
                .setRevision(RevisionSelector.newBuilder().setRevision(2)).build()).getManifest();
        assertThat(first.getCommittedAt()).isEqualTo(f.catalog.manifest(address, 1).orElseThrow().getCommittedAt());
        assertThat(second.getCommittedAt()).isEqualTo(f.catalog.manifest(address, 2).orElseThrow().getCommittedAt());
        assertThat(timestampOrder(first.getCommittedAt(), second.getCommittedAt())).isLessThanOrEqualTo(0);
    }

    @Test void clientWrittenStampsAndMismatchedDeclaredFactsAreRefusedUnrecorded() {
        var address = address(account, "doc-refused");
        var stamp = BlobWrite.newBuilder().setRevision(1).setCommittedAt(Timestamp.newBuilder().setSeconds(1)).build();
        f.stack.reset();
        assertMalformed(create("op-written", address, bag(account, "doc-refused",
                raw("b1", ORIGINAL).toBuilder().setWritten(stamp).build())).build(), "names written");
        assertMalformed(create("op-size", address, bag(account, "doc-refused",
                raw("b1", ORIGINAL).toBuilder().setSizeBytes(ORIGINAL.length() - 1).build())).build(), "declares size_bytes");
        assertMalformed(create("op-sha", address, bag(account, "doc-refused",
                raw("b1", ORIGINAL).toBuilder().setChecksum(SaveIntents.sha256Hex(MARKDOWN.getBytes())).build())).build(),
                "declares SHA-256");
        assertMalformed(create("op-md5", address, bag(account, "doc-refused",
                raw("b1", ORIGINAL).toBuilder().setChecksum("d41d8cd98f00b204e9800998ecf8427e")
                        .setChecksumType(ChecksumType.CHECKSUM_TYPE_MD5).build())).build(), "SHA-256 only");
        assertMalformed(create("op-subject", address, bag(account, "doc-refused",
                raw("b1", ORIGINAL).toBuilder().setSchemaSubject(" ").build())).build(), "schema_subject");
        assertThat(f.stack.puts.get()).isZero();
        for (var operation : List.of("op-written", "op-size", "op-sha", "op-md5", "op-subject"))
            assertThat(f.catalog.outcome(account, operation)).isEmpty();
        assertThat(f.catalog.current(address)).isEmpty();
    }

    @Test void aStreamedBlobWhoseDeclaredFactsDisagreeIsARecordedUploadMismatch() {
        var address = address(account, "doc-streamed-mismatch");
        var payload = ORIGINAL.getBytes();
        var deletes = new AtomicInteger();
        f.stack.reset();
        f.stack.afterDelete = deletes::incrementAndGet;
        var cases = List.of(
                new Object[] {"op-size", Blob.newBuilder().setBlobId("b1").setSizeBytes(payload.length + 5).build()},
                new Object[] {"op-sha", Blob.newBuilder().setBlobId("b1").setChecksum("0".repeat(64)).build()});
        for (var item : cases) {
            var operation = (String) item[0];
            var header = UploadHeader.newBuilder().setSave(create(operation, address,
                    bag(account, "doc-streamed-mismatch", (Blob) item[1]))).setBlobId("b1").build();
            assertThatThrownBy(() -> f.saves.upload(processAuthority(), header, new ByteArrayInputStream(payload)))
                    .isInstanceOfSatisfying(AccessionException.class, failure -> {
                        assertThat(failure.code()).isEqualTo(Status.Code.INVALID_ARGUMENT);
                        assertThat(((SaveRejection) failure.detail().orElseThrow()).getReason())
                                .isEqualTo(RejectionReason.REJECTION_REASON_UPLOAD_MISMATCH);
                    });
            assertThat(f.catalog.outcome(account, operation).orElseThrow().outcome().getRejection().getReason())
                    .isEqualTo(RejectionReason.REJECTION_REASON_UPLOAD_MISMATCH);
        }
        assertThat(deletes.get()).as("each refused upload's object is deleted").isEqualTo(2);
        assertThat(f.catalog.current(address)).isEmpty();
    }

    // ------------------------------------------------------------------ helpers

    private static void assertFacts(Blob blob, String content, BlobWrite written) {
        assertThat(blob.getContentCase()).isEqualTo(Blob.ContentCase.STORAGE_REF);
        assertThat(blob.getSizeBytes()).isEqualTo(content.getBytes().length);
        assertThat(blob.getChecksum()).isEqualTo(SaveIntents.sha256Hex(content.getBytes()));
        assertThat(blob.getChecksumType()).isEqualTo(ChecksumType.CHECKSUM_TYPE_SHA256);
        assertThat(blob.getWritten()).isEqualTo(written);
    }

    private void assertMalformed(SaveDocumentRequest request, String message) {
        assertThatThrownBy(() -> f.saves.save(processAuthority(), request))
                .isInstanceOfSatisfying(AccessionException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(Status.Code.INVALID_ARGUMENT);
                    assertThat(failure.getMessage()).contains(message);
                    assertThat(((SaveRejection) failure.detail().orElseThrow()).getReason())
                            .isEqualTo(RejectionReason.REJECTION_REASON_MALFORMED_REQUEST);
                });
    }

    private static Document bag(String account, String doc, Blob... blobs) {
        return document(account, doc, "v1").toBuilder()
                .setBlobBag(BlobBag.newBuilder().setBlobs(Blobs.newBuilder().addAllBlob(List.of(blobs)))).build();
    }

    private static SaveDocumentRequest blobsOnly(String operation, NodeAddress address, long expected, Blob... blobs) {
        return SaveDocumentRequest.newBuilder().setOperationId(operation)
                .setTarget(DocumentTarget.newBuilder().setAddress(address)).setExpected(at(expected))
                .setDocument(Document.newBuilder().setDocId(address.getDocId())
                        .setBlobBag(BlobBag.newBuilder().setBlobs(Blobs.newBuilder().addAllBlob(List.of(blobs)))))
                .addWrites(put(DocumentPart.DOCUMENT_PART_BLOBS)).setOwner(intakeOwner()).build();
    }

    private static Blob raw(String id, String content) {
        return Blob.newBuilder().setBlobId(id).setMimeType("text/plain").setSizeBytes(content.getBytes().length)
                .setData(ByteString.copyFromUtf8(content)).build();
    }

    private static Blob.Builder carried(String id) {
        return Blob.newBuilder().setBlobId(id).setMimeType("text/plain");
    }

    private static StoredPartEntry blobsEntry(StoredManifest manifest) {
        return manifest.getPartsList().stream()
                .filter(part -> part.getEntry().getSelector().getPart() == DocumentPart.DOCUMENT_PART_BLOBS)
                .findFirst().orElseThrow();
    }

    private static PartManifestEntry part(StoredManifest manifest, DocumentPart part) {
        return manifest.getPartsList().stream().map(StoredPartEntry::getEntry)
                .filter(entry -> entry.getSelector().getPart() == part).findFirst().orElseThrow();
    }

    private static BlobObjectRef binding(StoredManifest manifest, String blobId) {
        return blobsEntry(manifest).getBlobRefsList().stream().filter(ref -> ref.getBlobId().equals(blobId))
                .findFirst().orElseThrow();
    }

    private static int timestampOrder(Timestamp left, Timestamp right) {
        int seconds = Long.compare(left.getSeconds(), right.getSeconds());
        return seconds != 0 ? seconds : Integer.compare(left.getNanos(), right.getNanos());
    }
}
