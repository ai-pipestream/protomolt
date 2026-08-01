package ai.pipestream.proto.repo.container.blob;

import ai.pipestream.proto.repo.v1.Blob;
import ai.pipestream.proto.repo.v1.BlobBag;
import ai.pipestream.proto.repo.v1.Document;
import ai.pipestream.proto.repo.v1.DocumentManifest;
import ai.pipestream.proto.repo.v1.DocumentPart;
import ai.pipestream.proto.repo.v1.NodeAddress;
import ai.pipestream.proto.repo.v1.OwnershipContext;
import ai.pipestream.proto.repo.v1.ParserResult;
import ai.pipestream.proto.repo.v1.PartManifestEntry;
import ai.pipestream.proto.repo.v1.PartState;
import ai.pipestream.proto.repo.v1.SearchMetadata;
import ai.pipestream.proto.repo.v1.SemanticChunk;
import ai.pipestream.proto.repo.v1.SemanticProcessingResult;
import ai.pipestream.proto.repo.v1.WriteProvenance;
import ai.pipestream.proto.repo.container.codec.DocumentPartCodec;
import ai.pipestream.proto.repo.container.codec.PartLayout;
import ai.pipestream.proto.repo.container.codec.PartLayouts;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PartStorageTest {

    private static final String BUCKET = "documents";
    private static final String PREFIX = "accounts/acct-1/nodes/node-a";
    private static final PartLayout LAYOUT = PartLayouts.document();
    private static final NodeAddress ADDRESS = NodeAddress.newBuilder()
            .setDocId("doc-1")
            .setGraphAddressId("node-a")
            .setAccountId("acct-1")
            .setGraphId("graph-1")
            .build();
    private static final WriteProvenance PROVENANCE = WriteProvenance.newBuilder()
            .setModuleId("chunker")
            .setNodeId("node-a")
            .setGraphId("graph-1")
            .setGraphVersion(3)
            .build();

    private final PartStorage storage = new PartStorage();
    private InMemoryBlobStore store;

    @BeforeEach
    void setUp() {
        store = new InMemoryBlobStore();
    }

    @Test
    void writePartsBuildsCompleteManifest() {
        Document doc = fullDocument();

        PartStorage.WriteResult result = write(doc, PREFIX);

        DocumentManifest manifest = result.manifest();
        assertThat(manifest.getAddress()).isEqualTo(ADDRESS);
        assertThat(manifest.getAddress().getDocId()).isEqualTo("doc-1");
        assertThat(manifest.getAddress().getGraphAddressId()).isEqualTo("node-a");
        assertThat(manifest.getAddress().getAccountId()).isEqualTo("acct-1");
        assertThat(manifest.getAddress().getGraphId()).isEqualTo("graph-1");
        assertThat(manifest.getDocVersion()).isEqualTo(7L);

        // CORE, BLOBS, PARSED: one PRESENT entry each, fully described.
        for (DocumentPart part : List.of(DocumentPart.DOCUMENT_PART_CORE,
                DocumentPart.DOCUMENT_PART_BLOBS, DocumentPart.DOCUMENT_PART_PARSED)) {
            List<PartManifestEntry> entries = entriesFor(manifest, part);
            assertThat(entries).hasSize(1);
            PartManifestEntry e = entries.get(0);
            assertThat(e.getState()).isEqualTo(PartState.PART_STATE_PRESENT);
            assertThat(e.getSha256()).isNotBlank();
            assertThat(e.getSizeBytes()).isPositive();
            assertThat(e.getObjectKey()).startsWith(PREFIX);
            assertThat(e.hasUpdatedAt()).isTrue();
            assertThat(e.getWrittenBy()).isEqualTo(PROVENANCE);
        }
        // CHUNKS: one PRESENT entry per chunk set (sub-keyed by result_id).
        List<PartManifestEntry> chunks = entriesFor(manifest, DocumentPart.DOCUMENT_PART_CHUNKS);
        assertThat(chunks).hasSize(2);
        assertThat(chunks).extracting(PartManifestEntry::getSubKey)
                .containsExactlyInAnyOrder("set-a", "set-b");
        assertThat(chunks).allSatisfy(e -> {
            assertThat(e.getState()).isEqualTo(PartState.PART_STATE_PRESENT);
            assertThat(e.getSha256()).isNotBlank();
            assertThat(e.getObjectKey()).startsWith(PREFIX);
        });

        // Root checksum is the codec's own Merkle root over the same split.
        assertThat(result.rootChecksum()).isEqualTo(
                DocumentPartCodec.rootChecksum(DocumentPartCodec.split(doc, LAYOUT)));
        // Every written object key is reported and actually landed.
        assertThat(result.partObjectKeys()).isNotEmpty();
        assertThat(result.partObjectKeys()).containsExactlyInAnyOrderElementsOf(
                manifest.getPartsList().stream()
                        .filter(e -> e.getState() == PartState.PART_STATE_PRESENT)
                        .map(PartManifestEntry::getObjectKey).toList());
        assertThat(result.totalSizeBytes()).isEqualTo(manifest.getPartsList().stream()
                .filter(e -> e.getState() == PartState.PART_STATE_PRESENT)
                .mapToLong(PartManifestEntry::getSizeBytes).sum());
        assertThat(result.coreEtag()).isNotBlank();
        for (String key : result.partObjectKeys()) {
            store.headObject(BUCKET, key); // throws when absent
        }
    }

    @Test
    void writePartsRecordsAbsentPartsAsEmpty() {
        Document doc = Document.newBuilder()
                .setDocId("doc-1")
                .setSearchMetadata(SearchMetadata.newBuilder().setTitle("bare"))
                .build();

        PartStorage.WriteResult result = write(doc, PREFIX);

        for (DocumentPart part : List.of(DocumentPart.DOCUMENT_PART_BLOBS,
                DocumentPart.DOCUMENT_PART_CHUNKS, DocumentPart.DOCUMENT_PART_PARSED)) {
            List<PartManifestEntry> entries = entriesFor(result.manifest(), part);
            assertThat(entries).hasSize(1);
            PartManifestEntry e = entries.get(0);
            assertThat(e.getState()).isEqualTo(PartState.PART_STATE_EMPTY);
            assertThat(e.getObjectKey()).isEmpty();
            assertThat(e.getSha256()).isEmpty();
        }
        assertThat(entriesFor(result.manifest(), DocumentPart.DOCUMENT_PART_CORE))
                .singleElement()
                .extracting(PartManifestEntry::getState)
                .isEqualTo(PartState.PART_STATE_PRESENT);
    }

    @Test
    void writeThenReadRoundTripsTheDocument() {
        Document doc = fullDocument();
        PartStorage.WriteResult result = write(doc, PREFIX);

        Document back = storage.readParts(store, BUCKET, result.manifest(),
                Set.of(), Set.of(), Document.getDefaultInstance());

        assertThat(back).isEqualTo(doc);
    }

    @Test
    void partialReadHonoursThePartsMask() {
        Document doc = fullDocument();
        PartStorage.WriteResult result = write(doc, PREFIX);

        Document coreOnly = storage.readParts(store, BUCKET, result.manifest(),
                Set.of(DocumentPart.DOCUMENT_PART_CORE), Set.of(), Document.getDefaultInstance());

        Document expected = doc.toBuilder()
                .clearBlobBag()
                .clearParserResults()
                .setSearchMetadata(doc.getSearchMetadata().toBuilder().clearSemanticResults())
                .build();
        assertThat(coreOnly).isEqualTo(expected);
    }

    @Test
    void partialReadHonoursTheChunkSetFilter() {
        Document doc = fullDocument();
        PartStorage.WriteResult result = write(doc, PREFIX);

        Document oneSet = storage.readParts(store, BUCKET, result.manifest(),
                Set.of(DocumentPart.DOCUMENT_PART_CORE, DocumentPart.DOCUMENT_PART_CHUNKS),
                Set.of("set-a"), Document.getDefaultInstance());

        assertThat(oneSet.getSearchMetadata().getSemanticResultsList())
                .extracting(SemanticProcessingResult::getResultId)
                .containsExactly("set-a");
        assertThat(oneSet.hasBlobBag()).isFalse();
        assertThat(oneSet.getParserResultsMap()).isEmpty();
    }

    @Test
    void readWithNoWantedPartsReturnsTheDefaultInstance() {
        Document doc = fullDocument();
        PartStorage.WriteResult result = write(doc, PREFIX);

        // CHUNKS with a filter matching no sub-key leaves nothing wanted —
        // the chunk_sets filter narrows CHUNKS entries only.
        Document nothing = storage.readParts(store, BUCKET, result.manifest(),
                Set.of(DocumentPart.DOCUMENT_PART_CHUNKS), Set.of("no-such-set"),
                Document.getDefaultInstance());

        assertThat(nothing).isEqualTo(Document.getDefaultInstance());
    }

    @Test
    void copyForwardThenReadAtDestinationEqualsSource() {
        Document doc = fullDocument();
        String srcPrefix = PREFIX + "/hop-1";
        String dstPrefix = PREFIX + "/hop-2";
        PartStorage.WriteResult source = write(doc, srcPrefix);

        // Same-store server-side copy-forward of every PRESENT part object.
        List<PartStorage.CopySpec> specs = source.manifest().getPartsList().stream()
                .filter(e -> e.getState() == PartState.PART_STATE_PRESENT)
                .map(e -> new PartStorage.CopySpec(e, e.getObjectKey().replace(srcPrefix, dstPrefix)))
                .toList();
        storage.copyParts(store, BUCKET, store, BUCKET, true, specs);

        DocumentManifest destManifest = remap(source.manifest(), srcPrefix, dstPrefix);
        Document back = storage.readParts(store, BUCKET, destManifest,
                Set.of(), Set.of(), Document.getDefaultInstance());
        assertThat(back).isEqualTo(doc);
    }

    @Test
    void crossStoreCopyFallsBackToGetAndPut() {
        Document doc = fullDocument();
        InMemoryBlobStore other = new InMemoryBlobStore();
        PartStorage.WriteResult source = write(doc, PREFIX);

        List<PartStorage.CopySpec> specs = source.manifest().getPartsList().stream()
                .filter(e -> e.getState() == PartState.PART_STATE_PRESENT)
                .map(e -> new PartStorage.CopySpec(e, "copied/" + e.getObjectKey()))
                .toList();
        storage.copyParts(store, BUCKET, other, BUCKET, false, specs);

        DocumentManifest destManifest = prefix(source.manifest(), "copied/");
        Document back = storage.readParts(other, BUCKET, destManifest,
                Set.of(), Set.of(), Document.getDefaultInstance());
        assertThat(back).isEqualTo(doc);
    }

    @Test
    void missingObjectIsAttributedToTheExactPart() {
        Document doc = fullDocument();
        PartStorage.WriteResult result = write(doc, PREFIX);
        PartManifestEntry blobs = entriesFor(result.manifest(), DocumentPart.DOCUMENT_PART_BLOBS).get(0);
        store.delete(BUCKET, blobs.getObjectKey());

        assertThatThrownBy(() -> storage.readParts(store, BUCKET, result.manifest(),
                Set.of(), Set.of(), Document.getDefaultInstance()))
                .isInstanceOfSatisfying(PartStorage.PartObjectMissingException.class, ex ->
                        assertThat(ex.missingParts())
                                .containsExactly(DocumentPart.DOCUMENT_PART_BLOBS));
    }

    private PartStorage.WriteResult write(Document doc, String prefix) {
        return storage.writeParts(store, BUCKET, prefix, doc, LAYOUT, ADDRESS, PROVENANCE,
                "application/x-protobuf", Map.of("origin", "test"), true, 7L);
    }

    private static List<PartManifestEntry> entriesFor(DocumentManifest manifest, DocumentPart part) {
        return manifest.getPartsList().stream().filter(e -> e.getPart() == part).toList();
    }

    /** Rewrites every PRESENT entry's object key from one prefix to another. */
    private static DocumentManifest remap(DocumentManifest manifest, String from, String to) {
        DocumentManifest.Builder b = manifest.toBuilder().clearParts();
        for (PartManifestEntry e : manifest.getPartsList()) {
            b.addParts(e.getState() == PartState.PART_STATE_PRESENT
                    ? e.toBuilder().setObjectKey(e.getObjectKey().replace(from, to)).build()
                    : e);
        }
        return b.build();
    }

    /** Rewrites every PRESENT entry's object key under an added prefix. */
    private static DocumentManifest prefix(DocumentManifest manifest, String prefix) {
        DocumentManifest.Builder b = manifest.toBuilder().clearParts();
        for (PartManifestEntry e : manifest.getPartsList()) {
            b.addParts(e.getState() == PartState.PART_STATE_PRESENT
                    ? e.toBuilder().setObjectKey(prefix + e.getObjectKey()).build()
                    : e);
        }
        return b.build();
    }

    private static Document fullDocument() {
        return Document.newBuilder()
                .setDocId("doc-1")
                .setOwnership(OwnershipContext.newBuilder().setAccountId("acct-1"))
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setTitle("Quarterly report")
                        .setBody("the body text")
                        .addSemanticResults(chunkSet("set-a", "alpha chunk"))
                        .addSemanticResults(chunkSet("set-b", "beta chunk")))
                .setBlobBag(BlobBag.newBuilder().setBlob(Blob.newBuilder()
                        .setBlobId("blob-1")
                        .setData(ByteString.copyFromUtf8("raw bytes"))))
                .putParserResults("tika", ParserResult.newBuilder()
                        .setParserName("tika")
                        .putMetadata("pages", "3")
                        .build())
                .build();
    }

    private static SemanticProcessingResult chunkSet(String resultId, String text) {
        return SemanticProcessingResult.newBuilder()
                .setResultId(resultId)
                .addChunks(SemanticChunk.newBuilder()
                        .setChunkId(resultId + "-0")
                        .setChunkNumber(0)
                        .setText(text))
                .build();
    }
}
