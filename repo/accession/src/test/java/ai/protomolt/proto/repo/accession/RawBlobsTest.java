package ai.protomolt.proto.repo.accession;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.repo.v1.Blob;
import ai.protomolt.proto.repo.v1.BlobBag;
import ai.protomolt.proto.repo.v1.BlobWrite;
import ai.protomolt.proto.repo.v1.Blobs;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.WriteProvenance;
import ai.protomolt.proto.repo.v2.BlobObjectRef;
import ai.protomolt.proto.repo.v2.DocumentPart;
import ai.protomolt.proto.repo.v2.PartManifestEntry;
import ai.protomolt.proto.repo.v2.PartSelector;
import ai.protomolt.proto.repo.v2.PartState;
import ai.protomolt.proto.repo.v2.StackRef;
import ai.protomolt.proto.repo.v2.StoredPartEntry;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import io.grpc.Status;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link RawBlobs} single-parse extraction and decoded {@link BlobBag}
 * helpers, and multi-part write stamping in {@link DocumentSaves#stampNewWrites}.
 */
class RawBlobsTest {

    @Test
    void extractCarriesDecodedBagThroughStoredUnboundOrderRefsAndMetadata() {
        var inlineOne = Blob.newBuilder()
                .setBlobId("b1")
                .setMimeType("text/plain")
                .setFilename("first.txt")
                .setSizeBytes(5)
                .setData(ByteString.copyFromUtf8("alpha"))
                .build();
        var carriedTwo = Blob.newBuilder()
                .setBlobId("b2")
                .setMimeType("application/pdf")
                .setFilename("second.pdf")
                .setSizeBytes(12)
                .build();
        var inlineThree = Blob.newBuilder()
                .setBlobId("b3")
                .setMimeType("application/octet-stream")
                .setFilename("third.bin")
                .setData(ByteString.copyFromUtf8("gamma"))
                .build();
        var doc = Document.newBuilder()
                .setDocId("doc-1")
                .setBlobBag(BlobBag.newBuilder().setBlobs(Blobs.newBuilder()
                        .addBlob(inlineOne)
                        .addBlob(carriedTwo)
                        .addBlob(inlineThree)))
                .build();
        var fragment = RawBlobs.deterministic(doc);

        var extracted = RawBlobs.extract(fragment);
        var fromParsed = RawBlobs.extract(fragment, doc);
        assertThat(fromParsed).isEqualTo(extracted);

        assertThat(extracted.blobs()).containsExactly(
                new RawBlobs.Raw("b1", ByteString.copyFromUtf8("alpha")),
                new RawBlobs.Raw("b3", ByteString.copyFromUtf8("gamma")));
        assertThat(RawBlobs.parse(extracted.stored()).getBlobBag()).isEqualTo(extracted.bag());
        assertThat(RawBlobs.blobs(extracted.bag()))
                .allSatisfy(blob -> assertThat(blob.getContentCase()).isEqualTo(Blob.ContentCase.CONTENT_NOT_SET));

        // Carry the decoded BlobBag through stored, unbound, orderRefs, and metadata without re-parsing.
        var bag = extracted.bag();
        assertThat(RawBlobs.stored(bag, "b2")).contains(carriedTwo);
        assertThat(RawBlobs.stored(bag, "missing")).isEmpty();
        assertThat(RawBlobs.stored(extracted.stored(), "b2")).contains(carriedTwo);

        var bound = Set.of("b1", "b3");
        assertThat(RawBlobs.unbound(bag, bound)).containsExactly(carriedTwo);
        assertThat(RawBlobs.unbound(extracted.stored(), bound)).containsExactly(carriedTwo);

        var ref1 = ref("b1", "k1");
        var ref2 = ref("b2", "k2");
        var ref3 = ref("b3", "k3");
        var unordered = List.of(ref3, ref1, ref2);
        assertThat(RawBlobs.orderRefs(bag, unordered)).containsExactly(ref1, ref2, ref3);
        assertThat(RawBlobs.orderRefs(extracted.stored(), unordered)).containsExactly(ref1, ref2, ref3);
        assertThat(RawBlobs.orderRefs(bag, List.of(ref2))).containsExactly(ref2);

        assertThat(RawBlobs.metadata(bag, "b2")).isEqualTo(new RawBlobs.Metadata("application/pdf", "second.pdf"));
        assertThat(RawBlobs.metadata(extracted.stored(), "b1")).isEqualTo(new RawBlobs.Metadata("text/plain", "first.txt"));
        assertThatThrownBy(() -> RawBlobs.metadata(bag, "absent"))
                .isInstanceOfSatisfying(AccessionException.class, e -> assertThat(e.code()).isEqualTo(Status.Code.DATA_LOSS));
    }

    @Test
    void extractForUploadCarriesDecodedBagAndValidatesUploadedTarget() {
        var uploaded = Blob.newBuilder()
                .setBlobId("streamed")
                .setMimeType("video/mp4")
                .setFilename("clip.mp4")
                .setSizeBytes(1024)
                .build();
        var siblingInline = Blob.newBuilder()
                .setBlobId("inline")
                .setMimeType("text/plain")
                .setData(ByteString.copyFromUtf8("note"))
                .build();
        var siblingCarried = Blob.newBuilder()
                .setBlobId("carried")
                .setMimeType("image/png")
                .build();
        var doc = Document.newBuilder()
                .setDocId("doc-upload")
                .setBlobBag(BlobBag.newBuilder().setBlobs(Blobs.newBuilder()
                        .addBlob(siblingCarried)
                        .addBlob(uploaded)
                        .addBlob(siblingInline)))
                .build();
        var fragment = RawBlobs.deterministic(doc);

        var extracted = RawBlobs.extractForUpload(fragment, "streamed");
        assertThat(RawBlobs.extractForUpload(fragment, doc, "streamed")).isEqualTo(extracted);
        assertThat(extracted.blobs()).containsExactly(new RawBlobs.Raw("inline", ByteString.copyFromUtf8("note")));
        assertThat(RawBlobs.stored(extracted.bag(), "streamed")).contains(uploaded);
        assertThat(RawBlobs.unbound(extracted.bag(), Set.of("streamed", "inline"))).containsExactly(siblingCarried);

        var refStreamed = ref("streamed", "k-streamed");
        var refInline = ref("inline", "k-inline");
        var refCarried = ref("carried", "k-carried");
        assertThat(RawBlobs.orderRefs(extracted.bag(), List.of(refStreamed, refInline, refCarried)))
                .containsExactly(refCarried, refStreamed, refInline);

        assertThatThrownBy(() -> RawBlobs.extractForUpload(fragment, " "))
                .isInstanceOfSatisfying(AccessionException.class, e -> assertThat(e.code()).isEqualTo(Status.Code.INVALID_ARGUMENT));
        assertThatThrownBy(() -> RawBlobs.extractForUpload(fragment, "missing"))
                .isInstanceOfSatisfying(AccessionException.class, e -> assertThat(e.code()).isEqualTo(Status.Code.INVALID_ARGUMENT));
        assertThatThrownBy(() -> RawBlobs.extractForUpload(fragment, "inline"))
                .isInstanceOfSatisfying(AccessionException.class, e -> assertThat(e.code()).isEqualTo(Status.Code.INVALID_ARGUMENT));
    }

    @Test
    void singleBlobBagSupportsExtractAndClaimChecks() {
        var single = Blob.newBuilder()
                .setBlobId("solo")
                .setMimeType("text/plain")
                .setFilename("solo.txt")
                .setData(ByteString.copyFromUtf8("hello"))
                .build();
        var doc = Document.newBuilder()
                .setDocId("doc-solo")
                .setBlobBag(BlobBag.newBuilder().setBlob(single))
                .build();
        var extracted = RawBlobs.extract(RawBlobs.deterministic(doc));
        assertThat(extracted.blobs()).containsExactly(new RawBlobs.Raw("solo", ByteString.copyFromUtf8("hello")));
        assertThat(RawBlobs.stored(extracted.bag(), "solo")).isPresent();
        assertThat(RawBlobs.unbound(extracted.bag(), Set.of())).hasSize(1);
        assertThat(RawBlobs.unbound(extracted.bag(), Set.of("solo"))).isEmpty();
    }

    @Test
    void stampNewWritesAppliesSharedBlobWriteAcrossMultiplePartsAndPreservesCarriedEntries() {
        var coreSelector = DocumentSaves.CORE_SELECTOR;
        var blobsSelector = DocumentSaves.BLOBS_SELECTOR;
        var parsedSelector = PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_PARSED).build();

        var earlierAt = Timestamp.newBuilder().setSeconds(1_700_000_000L).build();
        var earlierWrite = BlobWrite.newBuilder().setRevision(1).setCommittedAt(earlierAt).build();
        var committedAt = Timestamp.newBuilder().setSeconds(1_700_000_500L).setNanos(250).build();
        var step = WriteProvenance.newBuilder()
                .setModuleId("mod")
                .setNodeId("node")
                .setGraphId("graph")
                .setGraphVersion(2)
                .build();
        var written = BlobWrite.newBuilder()
                .setRevision(2)
                .setCommittedAt(committedAt)
                .setStep(step)
                .build();

        var carriedCore = StoredPartEntry.newBuilder()
                .setEntry(PartManifestEntry.newBuilder()
                        .setSelector(coreSelector)
                        .setState(PartState.PART_STATE_PRESENT)
                        .setWrittenInRevision(1)
                        .setWrittenAt(earlierAt))
                .build();
        var carriedBlobRef = ref("b-carried", "k-carried").toBuilder().setWritten(earlierWrite).build();
        var newBlobRefOne = ref("b-new-1", "k-new-1");
        var newBlobRefTwo = ref("b-new-2", "k-new-2");
        var writtenBlobs = StoredPartEntry.newBuilder()
                .setEntry(PartManifestEntry.newBuilder()
                        .setSelector(blobsSelector)
                        .setState(PartState.PART_STATE_PRESENT)
                        .setWrittenInRevision(2))
                .addBlobRefs(carriedBlobRef)
                .addBlobRefs(newBlobRefOne)
                .addBlobRefs(newBlobRefTwo)
                .build();
        var writtenParsed = StoredPartEntry.newBuilder()
                .setEntry(PartManifestEntry.newBuilder()
                        .setSelector(parsedSelector)
                        .setState(PartState.PART_STATE_PRESENT)
                        .setWrittenInRevision(2))
                .build();

        var newBindings = Set.of(
                new DocumentSaves.BindingKey(blobsSelector, "b-new-1"),
                new DocumentSaves.BindingKey(blobsSelector, "b-new-2"));
        var parts = List.of(carriedCore, writtenBlobs, writtenParsed);
        var stamped = parts.stream()
                .map(part -> DocumentSaves.stampNewWrites(part, 2L, written, newBindings))
                .toList();

        assertThat(stamped.get(0)).isSameAs(carriedCore);
        assertThat(stamped.get(1).getEntry().getWrittenAt()).isEqualTo(committedAt);
        assertThat(stamped.get(1).getBlobRefs(0).getWritten()).isEqualTo(earlierWrite);
        assertThat(stamped.get(1).getBlobRefs(1).getWritten()).isEqualTo(written);
        assertThat(stamped.get(1).getBlobRefs(2).getWritten()).isEqualTo(written);
        assertThat(stamped.get(2).getEntry().getWrittenAt()).isEqualTo(committedAt);
    }

    private static BlobObjectRef ref(String blobId, String key) {
        return BlobObjectRef.newBuilder()
                .setBlobId(blobId)
                .setRef(StackRef.newBuilder()
                        .setStackId("drive-1")
                        .setKey(key)
                        .setLength(5)
                        .setSha256("a".repeat(64)))
                .build();
    }
}
