package ai.protomolt.proto.repo.archive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.repo.archive.v1.EntryAddress;
import ai.protomolt.proto.repo.archive.v1.EntryInfo;
import ai.protomolt.proto.repo.archive.v1.RenditionDescriptor;
import ai.protomolt.proto.repo.archive.v1.RenditionManifestEntry;
import ai.protomolt.proto.repo.archive.v1.RenditionState;
import ai.protomolt.proto.repo.archive.v1.VersionManifest;
import ai.protomolt.proto.repo.archive.v1.VersionSummary;
import ai.protomolt.proto.repo.archive.v1.WriteAttribution;
import ai.protomolt.proto.repo.v1.Blob;
import ai.protomolt.proto.repo.v1.BlobBag;
import ai.protomolt.proto.repo.v1.BlobWrite;
import ai.protomolt.proto.repo.v1.Blobs;
import ai.protomolt.proto.repo.v1.ChecksumType;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v1.WriteProvenance;
import ai.protomolt.proto.repo.v2.DocumentLookup;
import ai.protomolt.proto.repo.v2.DocumentManifest;
import ai.protomolt.proto.repo.v2.DocumentPart;
import ai.protomolt.proto.repo.v2.DocumentSummary;
import ai.protomolt.proto.repo.v2.GetDocumentResponse;
import ai.protomolt.proto.repo.v2.IntakeOrigin;
import ai.protomolt.proto.repo.v2.PartManifestEntry;
import ai.protomolt.proto.repo.v2.PartSelector;
import ai.protomolt.proto.repo.v2.PartState;
import ai.protomolt.proto.repo.v2.RevisionSummary;
import ai.protomolt.proto.repo.v2.RevisionValidation;
import ai.protomolt.proto.repo.v2.SaveOwner;
import ai.protomolt.proto.repo.v2.ValidationStatus;
import com.google.protobuf.Timestamp;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;

/**
 * The archive view projections through the real view {@code .proto}, with no
 * repository behind them: each test builds the v2 read response by hand.
 */
class ArchiveViewsTest {

    private static final String SHA_ORIGINAL = "a".repeat(64);
    private static final String SHA_PAGE = "b".repeat(64);
    private static final String SHA_PARSED = "c".repeat(64);
    private static final String CANDIDATE = "d".repeat(64);
    private static final Timestamp REVISION_1_AT = Timestamp.newBuilder().setSeconds(1_700_000_000).build();
    private static final Timestamp REVISION_2_AT = Timestamp.newBuilder().setSeconds(1_700_000_600).build();

    private final ArchiveViews views = new ArchiveViews();

    private static NodeAddress address() {
        return ArchiveCoordinates.address(EntryAddress.newBuilder()
                .setAccountId("acct").setArchive("reports").setEntryId("q3.pdf").build());
    }

    private static SaveOwner intake() {
        return SaveOwner.newBuilder().setIntake(IntakeOrigin.newBuilder().setDatasourceId("ds-archive")).build();
    }

    private static PartManifestEntry part(DocumentPart part, long writtenIn, Timestamp writtenAt, String sha256) {
        return PartManifestEntry.newBuilder()
                .setSelector(PartSelector.newBuilder().setPart(part))
                .setState(PartState.PART_STATE_PRESENT)
                .setSizeBytes(40)
                .setSha256(sha256)
                .setWrittenInRevision(writtenIn)
                .setWrittenAt(writtenAt)
                .setOwner(SaveOwner.newBuilder().setStep(WriteProvenance.newBuilder().setModuleId("grparse")))
                .build();
    }

    /** Revision 2 of an entry: an original, a page-1 rendition from revision 1, and a PARSED part from revision 1. */
    private static GetDocumentResponse revisionTwo() {
        Blob original = Blob.newBuilder()
                .setBlobId("original").setMimeType("application/pdf").setFilename("q3.pdf")
                .setSizeBytes(1024).setChecksum(SHA_ORIGINAL).setChecksumType(ChecksumType.CHECKSUM_TYPE_SHA256)
                .setWritten(BlobWrite.newBuilder().setRevision(2).setCommittedAt(REVISION_2_AT))
                .build();
        Blob page = Blob.newBuilder()
                .setBlobId("page/0001").setMimeType("image/png").setSchemaSubject("pages-v1")
                .setSizeBytes(512).setChecksum(SHA_PAGE).setChecksumType(ChecksumType.CHECKSUM_TYPE_SHA256)
                .setWritten(BlobWrite.newBuilder().setRevision(1).setCommittedAt(REVISION_1_AT)
                        .setStep(WriteProvenance.newBuilder().setModuleId("page-render")))
                .build();
        Document document = Document.newBuilder()
                .setDocId("q3.pdf")
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setTitle("Q3 report").setSourceMimeType("application/pdf")
                        .setSourceUri("s3://bucket/q3.pdf").setCreationDate(REVISION_1_AT)
                        .putMetadata("team", "finance"))
                .setBlobBag(BlobBag.newBuilder().setBlobs(Blobs.newBuilder().addBlob(original).addBlob(page)))
                .build();
        DocumentManifest manifest = DocumentManifest.newBuilder()
                .setAddress(address()).setRevision(2).setCommittedAt(REVISION_2_AT)
                .addParts(part(DocumentPart.DOCUMENT_PART_CORE, 2, REVISION_2_AT, "e".repeat(64)))
                .addParts(part(DocumentPart.DOCUMENT_PART_BLOBS, 2, REVISION_2_AT, "f".repeat(64)))
                .addParts(part(DocumentPart.DOCUMENT_PART_PARSED, 1, REVISION_1_AT, SHA_PARSED))
                .build();
        return GetDocumentResponse.newBuilder()
                .setDocument(document)
                .setManifest(manifest)
                .setValidation(RevisionValidation.newBuilder()
                        .setStatus(ValidationStatus.VALIDATION_STATUS_TYPED_VALID)
                        .setCandidateSha256(CANDIDATE).setValidatorProfile("v1"))
                .build();
    }

    @Test
    void projectsTwoBlobsAndAParsedPartIntoEveryRow() {
        VersionManifest manifest = views.manifest(revisionTwo());

        EntryAddress entry = EntryAddress.newBuilder()
                .setAccountId("acct").setArchive("reports").setEntryId("q3.pdf").build();
        assertThat(manifest.getAddress()).isEqualTo(entry);
        assertThat(manifest.getVersion()).isEqualTo(2);
        assertThat(manifest.getRootChecksum()).isEqualTo(CANDIDATE);
        assertThat(manifest.getCreatedAt()).isEqualTo(REVISION_2_AT);
        assertThat(manifest.getTotalBytes()).isEqualTo(1024 + 512 + 40);
        assertThat(manifest.getRenditionsList()).containsExactly(
                // The raw blob's state comes from the number_value: 1 fallback.
                RenditionManifestEntry.newBuilder()
                        .setRendition(RenditionDescriptor.newBuilder()
                                .setName("original").setMediaType("application/pdf"))
                        .setState(RenditionState.RENDITION_STATE_PRESENT)
                        .setSizeBytes(1024).setSha256(SHA_ORIGINAL)
                        .setWrittenAt(REVISION_2_AT)
                        .setWrittenBy(WriteAttribution.getDefaultInstance())
                        .build(),
                RenditionManifestEntry.newBuilder()
                        .setRendition(RenditionDescriptor.newBuilder()
                                .setName("page").setSubKey("0001").setMediaType("image/png")
                                .setSchemaSubject("pages-v1"))
                        .setState(RenditionState.RENDITION_STATE_PRESENT)
                        .setSizeBytes(512).setSha256(SHA_PAGE)
                        .setWrittenAt(REVISION_1_AT)
                        .setWrittenBy(WriteAttribution.newBuilder().setModule("page-render"))
                        .build(),
                // The PARSED row's state converts from PartState; its write time is revision 1's.
                RenditionManifestEntry.newBuilder()
                        .setRendition(RenditionDescriptor.newBuilder()
                                .setName("parsed").setMediaType("application/x-protobuf"))
                        .setState(RenditionState.RENDITION_STATE_PRESENT)
                        .setSizeBytes(40).setSha256(SHA_PARSED)
                        .setWrittenAt(REVISION_1_AT)
                        .setWrittenBy(WriteAttribution.newBuilder().setModule("grparse"))
                        .build());
        assertThat(manifest.getMetadataSnapshot()).isEqualTo(EntryInfo.newBuilder()
                .setAddress(entry)
                .setCurrentVersion(2)
                .setTitle("Q3 report")
                .setFilename("q3.pdf")
                .setContentType("application/pdf")
                .setSourceUri("s3://bucket/q3.pdf")
                .setCreatedAt(REVISION_1_AT)
                .setUpdatedAt(REVISION_2_AT)
                .putMetadata("team", "finance")
                .build());
    }

    @Test
    void aManifestWithoutParsedListsOnlyTheBlobs() {
        GetDocumentResponse read = revisionTwo();
        GetDocumentResponse noParsed = read.toBuilder()
                .setManifest(read.getManifest().toBuilder().removeParts(2))
                .build();
        assertThat(views.manifest(noParsed).getRenditionsList())
                .extracting(row -> row.getRendition().getName())
                .containsExactly("original", "page");
    }

    @Test
    void aDeletedParsedPartKeepsItsReasonAndCountsNoBytes() {
        GetDocumentResponse read = revisionTwo();
        GetDocumentResponse deleted = read.toBuilder()
                .setManifest(read.getManifest().toBuilder().setParts(2, read.getManifest().getParts(2).toBuilder()
                        .setState(PartState.PART_STATE_DELETED).setDeletedReason("RTBF")))
                .build();
        VersionManifest manifest = views.manifest(deleted);
        RenditionManifestEntry parsed = manifest.getRenditions(2);
        assertThat(parsed.getState()).isEqualTo(RenditionState.RENDITION_STATE_DELETED);
        assertThat(parsed.getDeletedReason()).isEqualTo("RTBF");
        assertThat(manifest.getTotalBytes()).isEqualTo(1024 + 512);
    }

    @Test
    void aSingularBlobArmIsNotAnArchiveEntry() {
        GetDocumentResponse read = revisionTwo();
        GetDocumentResponse singular = read.toBuilder()
                .setDocument(read.getDocument().toBuilder().setBlobBag(BlobBag.newBuilder()
                        .setBlob(read.getDocument().getBlobBag().getBlobs().getBlob(0))))
                .build();
        assertThatThrownBy(() -> views.manifest(singular))
                .isInstanceOfSatisfying(StatusRuntimeException.class, failure -> {
                    assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.FAILED_PRECONDITION);
                    assertThat(failure.getStatus().getDescription()).contains("blob_bag.blob");
                });
    }

    @Test
    void aViewThatBreaksItsOwnRulesIsDataLossNamingTheField() {
        GetDocumentResponse read = revisionTwo();
        Blob badMime = read.getDocument().getBlobBag().getBlobs().getBlob(0).toBuilder()
                .setMimeType("not a media type").build();
        GetDocumentResponse invalid = read.toBuilder()
                .setDocument(read.getDocument().toBuilder().setBlobBag(BlobBag.newBuilder().setBlobs(
                        read.getDocument().getBlobBag().getBlobs().toBuilder().setBlob(0, badMime))))
                .build();
        assertThatThrownBy(() -> views.manifest(invalid))
                .isInstanceOfSatisfying(StatusRuntimeException.class, failure -> {
                    assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.DATA_LOSS);
                    assertThat(failure.getStatus().getDescription()).contains("renditions[0].rendition.media_type");
                });
    }

    @Test
    void anUnknownPartStateNumberIsDataLoss() {
        GetDocumentResponse read = revisionTwo();
        GetDocumentResponse unknown = read.toBuilder()
                .setManifest(read.getManifest().toBuilder().setParts(2, read.getManifest().getParts(2).toBuilder()
                        .setStateValue(9)))
                .build();
        assertThatThrownBy(() -> views.manifest(unknown))
                .isInstanceOfSatisfying(StatusRuntimeException.class,
                        failure -> assertThat(failure.getStatus().getCode()).isEqualTo(Status.Code.DATA_LOSS));
    }

    @Test
    void listingRowsProjectThroughTheirLookup() {
        DocumentSummary row = DocumentSummary.newBuilder()
                .setAddress(address()).setRevision(3).setCommittedAt(REVISION_2_AT)
                .setLookup(DocumentLookup.newBuilder()
                        .setTitle("Q3 report").setSourceMimeType("application/pdf")
                        .setSourceUri("s3://bucket/q3.pdf").setCreationDate(REVISION_1_AT)
                        .setLastModifiedDate(REVISION_1_AT).putMetadata("team", "finance"))
                .build();
        assertThat(views.entryInfo(row)).isEqualTo(EntryInfo.newBuilder()
                .setAddress(EntryAddress.newBuilder().setAccountId("acct").setArchive("reports").setEntryId("q3.pdf"))
                .setCurrentVersion(3)
                .setTitle("Q3 report")
                .setContentType("application/pdf")
                .setSourceUri("s3://bucket/q3.pdf")
                .setSourceModifiedAt(REVISION_1_AT)
                .setCreatedAt(REVISION_1_AT)
                .setUpdatedAt(REVISION_2_AT)
                .putMetadata("team", "finance")
                .build());
    }

    @Test
    void revisionSummariesBecomeVersionSummaries() {
        RevisionSummary revision = RevisionSummary.newBuilder()
                .setRevision(2).setPreviousRevision(1).setCommittedAt(REVISION_2_AT).setOperationId("op")
                .setOwner(SaveOwner.newBuilder().setStep(WriteProvenance.newBuilder().setModuleId("grparse")))
                .setValidationStatus(ValidationStatus.VALIDATION_STATUS_TYPED_VALID)
                .build();
        assertThat(views.summary(revision)).isEqualTo(VersionSummary.newBuilder()
                .setVersion(2).setPreviousVersion(1).setCreatedAt(REVISION_2_AT).setModule("grparse").build());
        RevisionSummary tombstone = revision.toBuilder().setRevision(3).setPreviousRevision(2)
                .setOwner(intake()).setTombstone(true).build();
        assertThat(views.summary(tombstone)).isEqualTo(VersionSummary.newBuilder()
                .setVersion(3).setPreviousVersion(2).setCreatedAt(REVISION_2_AT).setDeleted(true).build());
    }
}
