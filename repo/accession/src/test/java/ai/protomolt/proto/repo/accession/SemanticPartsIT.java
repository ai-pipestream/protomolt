package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.*;
import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.codec.SegmentationDigests;
import ai.protomolt.proto.repo.semantic.v1.DerivedText;
import ai.protomolt.proto.repo.semantic.v1.EmbeddingSet;
import ai.protomolt.proto.repo.semantic.v1.EmbeddingSetKey;
import ai.protomolt.proto.repo.semantic.v1.Lineage;
import ai.protomolt.proto.repo.semantic.v1.OffsetUnit;
import ai.protomolt.proto.repo.semantic.v1.Segment;
import ai.protomolt.proto.repo.semantic.v1.SegmentVector;
import ai.protomolt.proto.repo.semantic.v1.Segmentation;
import ai.protomolt.proto.repo.semantic.v1.SegmentationKind;
import ai.protomolt.proto.repo.semantic.v1.Similarity;
import ai.protomolt.proto.repo.semantic.v1.SourceTextBinding;
import ai.protomolt.proto.repo.semantic.v1.TextSource;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v1.SemanticProcessingResult;
import ai.protomolt.proto.repo.v2.*;
import ai.protomolt.proto.repo.v2.DocumentPart;
import io.grpc.Status;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Segmentations as CHUNKS objects, embedding sets as EMBEDDINGS objects, and the
 * source binding between them and the document text (requirements §5, §9; acceptance
 * §15.1 on CHUNKS, §15.2, §15.15). Real PostgreSQL and S3; every stack call is counted.
 */
class SemanticPartsIT {
    /** The fixture document's body for title "v1". */
    static final String BODY = "body of v1";
    static final String S1 = "sentences", S2 = "normalized", M1 = "model2vec", M2 = "minilm";

    private AccessionFixture f;
    private String account;
    private NodeAddress address;

    @BeforeEach void open() throws Exception {
        f = new AccessionFixture();
        account = "acct-" + UUID.randomUUID();
        address = address(account, "doc-sem");
    }

    @AfterEach void close() { f.close(); }

    @Test void fullDocumentStoresOneObjectPerSegmentationAndEmbeddingSetAndReadsBack() {
        var saved = f.saves.save(processAuthority(), fullSave("op-1").build());
        var manifest = saved.getAccession().getManifest();
        assertThat(manifest.getPartsList()).extracting(entry -> DocumentSaves.describe(entry.getSelector()))
                .containsExactly("CORE", "BLOBS", "CHUNKS " + S2, "CHUNKS " + S1, "PARSED",
                        "EMBEDDINGS " + S2 + "/" + M1, "EMBEDDINGS " + S1 + "/" + M1);
        assertThat(f.stack.puts.get()).as("5 parts + 2 semantic objects + 1 raw blob").isEqualTo(8);
        assertThat(f.stack.opens.get()).isZero();
        var s1 = segmentationEntry(manifest, S1);
        var s2 = segmentationEntry(manifest, S2);
        assertThat(s1.getSegmentationDigest()).isEqualTo(SegmentationDigests.digest(bodySegmentation()));
        assertThat(s2.getSegmentationDigest()).isEqualTo(SegmentationDigests.digest(derivedSegmentation()));
        assertThat(embeddingEntry(manifest, S1, M1).getSegmentationDigest()).isEqualTo(s1.getSegmentationDigest());
        assertThat(embeddingEntry(manifest, S2, M1).getSegmentationDigest()).isEqualTo(s2.getSegmentationDigest());
        assertThat(manifest.getPartsList()).allSatisfy(entry -> assertThat(entry.getState()).isEqualTo(PartState.PART_STATE_PRESENT));

        f.stack.reset();
        var read = f.reads.getDocument(processAuthority(), GetDocumentRequest.newBuilder().setAddress(address).build());
        var search = read.getDocument().getSearchMetadata();
        assertThat(search.getBody()).isEqualTo(BODY);
        assertThat(search.getSegmentationsList()).extracting(Segmentation::getSegmentationId).containsExactly(S2, S1);
        assertThat(search.getSegmentationsList()).extracting(Segmentation::getContentDigest)
                .containsExactly(s2.getSegmentationDigest(), s1.getSegmentationDigest());
        assertThat(search.getSegmentations(1).toBuilder().clearContentDigest().build()).isEqualTo(bodySegmentation());
        assertThat(search.getEmbeddingSetsList()).containsExactly(embeddingSet(S2, M1, s2.getSegmentationDigest(), "n0", "n1"),
                embeddingSet(S1, M1, s1.getSegmentationDigest(), "a", "b", "c"));
        assertThat(f.stack.opens.get()).isEqualTo(7);

        f.stack.reset();
        var one = f.reads.getDocument(processAuthority(), GetDocumentRequest.newBuilder().setAddress(address)
                .setSelection(PartSelection.newBuilder().addSelectors(PartSelector.newBuilder().setSegmentationId(S1))).build());
        assertThat(one.getDocument().getSearchMetadata().getSegmentationsList()).extracting(Segmentation::getSegmentationId)
                .containsExactly(S1);
        assertThat(one.getDocument().getSearchMetadata().hasBody()).isFalse();
        assertThat(one.getDocument().getSearchMetadata().getEmbeddingSetsCount()).isZero();
        assertThat(one.getManifest().getPartsList()).filteredOn(PartManifestEntry::getContentIncluded)
                .extracting(entry -> entry.getSelector().getSegmentationId()).containsExactly(S1);
        assertThat(f.stack.opens.get()).isEqualTo(1);

        f.stack.reset();
        var sets = f.reads.getDocument(processAuthority(), GetDocumentRequest.newBuilder().setAddress(address)
                .setSelection(PartSelection.newBuilder().setAllEmbeddingSets(true)).build());
        assertThat(sets.getDocument().getSearchMetadata().getEmbeddingSetsCount()).isEqualTo(2);
        assertThat(sets.getDocument().getSearchMetadata().getSegmentationsCount()).isZero();
        assertThat(f.stack.opens.get()).isEqualTo(2);
    }

    // §15.1 on CHUNKS: a segmentation bound to CORE text re-reads exactly that CORE object.
    @Test void updatingABodyBoundSegmentationReadsCoreOnceAndTouchesNoSibling() {
        f.saves.save(processAuthority(), fullSave("op-1").build());
        var before = f.catalog.manifest(address, 1).orElseThrow();
        f.stack.reset();

        var relabeled = bodySegmentation().toBuilder().setLineage(Lineage.newBuilder().setProducerId("chunker").setProducerVersion("2")).build();
        var saved = f.saves.save(processAuthority(), partial("op-2", 1, semantic(List.of(relabeled), List.of()),
                writeSegmentation(S1)).build());

        assertThat(f.stack.puts.get()).isEqualTo(1);
        assertThat(f.stack.opens.get()).as("one GET on CORE to recompute the source binding").isEqualTo(1);
        assertThat(f.stack.opened).containsOnlyKeys(ref(before, CORE_SELECTOR()).getKey());
        var after = f.catalog.manifest(address, 2).orElseThrow();
        for (var selector : List.of(CORE_SELECTOR(), part(DocumentPart.DOCUMENT_PART_BLOBS), part(DocumentPart.DOCUMENT_PART_PARSED),
                segmentation(S2), embedding(S1, M1), embedding(S2, M1))) {
            assertThat(ref(after, selector)).as("%s", DocumentSaves.describe(selector)).isEqualTo(ref(before, selector));
            assertThat(stored(after, selector).getEntry().getWrittenInRevision()).isEqualTo(1);
        }
        assertThat(ref(after, segmentation(S1)).getKey()).isNotEqualTo(ref(before, segmentation(S1)).getKey());
        assertThat(stored(after, segmentation(S1)).getEntry().getSegmentationDigest())
                .as("lineage is not part of the digest, so the carried embedding set still binds")
                .isEqualTo(stored(before, segmentation(S1)).getEntry().getSegmentationDigest());
        assertThat(saved.getAccession().getRevision()).isEqualTo(2);
        var event = f.catalog.events(address).get(1).getAccessioned();
        assertThat(event.getChangesList()).singleElement().satisfies(change -> {
            assertThat(change.getSelector().getSegmentationId()).isEqualTo(S1);
            assertThat(change.getKind()).isEqualTo(ChangeKind.CHANGE_KIND_REPLACED);
            assertThat(change.getSha256()).isEqualTo(stored(after, segmentation(S1)).getEntry().getSha256());
        });
        var read = f.reads.getDocument(processAuthority(), GetDocumentRequest.newBuilder().setAddress(address)
                .setSelection(PartSelection.newBuilder().addSelectors(segmentation(S1))).build());
        assertThat(read.getDocument().getSearchMetadata().getSegmentations(0).getLineage().getProducerVersion()).isEqualTo("2");
    }

    // §15.1 on CHUNKS: derived text needs no sibling at all.
    @Test void updatingADerivedTextSegmentationReadsNothing() {
        f.saves.save(processAuthority(), fullSave("op-1").build());
        var before = f.catalog.manifest(address, 1).orElseThrow();
        f.stack.reset();

        var relabeled = derivedSegmentation().toBuilder().putMetadata("note", "re-labeled").build();
        f.saves.save(processAuthority(), partial("op-2", 1, semantic(List.of(relabeled), List.of()), writeSegmentation(S2)).build());

        assertThat(f.stack.puts.get()).isEqualTo(1);
        assertThat(f.stack.opens.get()).as("no GET: the text is inside the segmentation").isZero();
        var after = f.catalog.manifest(address, 2).orElseThrow();
        assertThat(after.getPartsList()).hasSize(before.getPartsList().size());
        for (var entry : before.getPartsList()) {
            if (entry.getEntry().getSelector().equals(segmentation(S2))) continue;
            assertThat(stored(after, entry.getEntry().getSelector())).isEqualTo(entry);
        }
        assertThat(stored(after, segmentation(S2)).getEntry().getSegmentationDigest())
                .isEqualTo(stored(before, segmentation(S2)).getEntry().getSegmentationDigest());
    }

    // §15.2: a new embedding set writes one object and reads only the segmentation it binds to.
    @Test void addingAnEmbeddingSetWritesOneObjectAndReadsOnlyItsSegmentation() {
        f.saves.save(processAuthority(), fullSave("op-1").build());
        var before = f.catalog.manifest(address, 1).orElseThrow();
        f.stack.reset();

        var digest = stored(before, segmentation(S1)).getEntry().getSegmentationDigest();
        var saved = f.saves.save(processAuthority(), partial("op-2", 1,
                semantic(List.of(), List.of(embeddingSet(S1, M2, digest, "c", "a", "b"))), writeEmbedding(S1, M2)).build());

        assertThat(f.stack.puts.get()).isEqualTo(1);
        assertThat(f.stack.opens.get()).as("one GET on the bound CHUNKS object for its segment IDs").isEqualTo(1);
        assertThat(f.stack.opened).containsOnlyKeys(ref(before, segmentation(S1)).getKey());
        var after = f.catalog.manifest(address, 2).orElseThrow();
        assertThat(after.getPartsList()).hasSize(before.getPartsList().size() + 1);
        for (var entry : before.getPartsList())
            assertThat(stored(after, entry.getEntry().getSelector())).isEqualTo(entry);
        var added = stored(after, embedding(S1, M2)).getEntry();
        assertThat(added.getSegmentationDigest()).isEqualTo(digest);
        assertThat(added.getWrittenInRevision()).isEqualTo(2);
        assertThat(saved.getAccession().getManifest().getPartsList()).extracting(entry -> DocumentSaves.describe(entry.getSelector()))
                .containsExactly("CORE", "BLOBS", "CHUNKS " + S2, "CHUNKS " + S1, "PARSED",
                        "EMBEDDINGS " + S2 + "/" + M1, "EMBEDDINGS " + S1 + "/" + M2, "EMBEDDINGS " + S1 + "/" + M1);
        var event = f.catalog.events(address).get(1).getAccessioned();
        assertThat(event.getChangesList()).singleElement().satisfies(change -> {
            assertThat(change.getSelector().getEmbeddingSet()).isEqualTo(key(S1, M2));
            assertThat(change.getKind()).isEqualTo(ChangeKind.CHANGE_KIND_ADDED);
        });
    }

    // §15.15: new text under a carried embedding set is refused, and recorded.
    @Test void changingASegmentationUnderACarriedEmbeddingSetIsASegmentationMismatch() {
        f.saves.save(processAuthority(), fullSave("op-1").build());
        var before = f.catalog.manifest(address, 1).orElseThrow();
        f.stack.reset();

        var recut = bodySegmentation().toBuilder().clearSegments()
                .addSegments(segment("a", 0, "body of", 0, 7)).addSegments(segment("b", 1, "v1", 8, 10)).build();
        var newDigest = SegmentationDigests.digest(recut);
        assertThat(newDigest).isNotEqualTo(stored(before, segmentation(S1)).getEntry().getSegmentationDigest());
        var request = partial("op-2", 1, semantic(List.of(recut), List.of()), writeSegmentation(S1)).build();
        assertThatThrownBy(() -> f.saves.save(processAuthority(), request))
                .isInstanceOfSatisfying(AccessionException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(Status.Code.FAILED_PRECONDITION);
                    var rejection = (SaveRejection) failure.detail().orElseThrow();
                    assertThat(rejection.getReason()).isEqualTo(RejectionReason.REJECTION_REASON_SEGMENTATION_MISMATCH);
                    assertThat(rejection.getViolationsList()).singleElement().satisfies(violation -> {
                        assertThat(violation.getPart().getEmbeddingSet()).isEqualTo(key(S1, M1));
                        assertThat(violation.getRuleId()).isEqualTo("embedding-set.segmentation-digest");
                        assertThat(violation.getMessage()).contains(newDigest);
                    });
                });
        assertThat(f.stack.puts.get()).as("refused before any upload").isZero();
        assertThat(f.catalog.outcome(account, "op-2")).isPresent();
        assertThat(f.catalog.current(address).orElseThrow().revision()).isEqualTo(1);

        // The same recut with its embedding set rewritten against the new digest is accepted.
        var rebound = embeddingSet(S1, M1, newDigest, "a", "b");
        var saved = f.saves.save(processAuthority(), partial("op-3", 1, semantic(List.of(recut), List.of(rebound)),
                writeSegmentation(S1), writeEmbedding(S1, M1)).build());
        assertThat(f.stack.puts.get()).isEqualTo(2);
        var after = f.catalog.manifest(address, 2).orElseThrow();
        assertThat(stored(after, segmentation(S1)).getEntry().getSegmentationDigest()).isEqualTo(newDigest);
        assertThat(stored(after, embedding(S1, M1)).getEntry().getSegmentationDigest()).isEqualTo(newDigest);
        assertThat(stored(after, embedding(S2, M1))).isEqualTo(stored(before, embedding(S2, M1)));
        assertThat(saved.getAccession().getRevision()).isEqualTo(2);
    }

    @Test void segmentOrderIsOrdinalOrderAndNeverRepaired() {
        var outOfOrder = bodySegmentation().toBuilder().clearSegments()
                .addSegments(segment("b", 1, "of", 5, 7)).addSegments(segment("a", 0, "body", 0, 4)).build();
        var rejected = rejected(() -> f.saves.save(processAuthority(), fullSave("op-order", outOfOrder, derivedSegmentation()).build()),
                Status.Code.INVALID_ARGUMENT, RejectionReason.REJECTION_REASON_VALIDATION_FAILED);
        assertThat(rejected.getViolationsList()).extracting(FieldViolation::getRuleId).containsOnly("segmentation.ordinal-order");
        assertThat(rejected.getViolationsList()).extracting(violation -> violation.getPart().getSegmentationId()).containsOnly(S1);
        assertThat(f.catalog.outcome(account, "op-order")).isPresent();

        var duplicate = bodySegmentation().toBuilder().clearSegments()
                .addSegments(segment("a", 0, "body", 0, 4)).addSegments(segment("a", 1, "of", 5, 7)).build();
        var twice = rejected(() -> f.saves.save(processAuthority(), fullSave("op-dup", duplicate, derivedSegmentation()).build()),
                Status.Code.INVALID_ARGUMENT, RejectionReason.REJECTION_REASON_VALIDATION_FAILED);
        assertThat(twice.getViolationsList()).extracting(FieldViolation::getRuleId).containsExactly("segmentation.segment-id-unique");
        assertThat(f.stack.puts.get()).isZero();

        // A valid different order is a different segmentation with a different digest.
        var reversed = bodySegmentation().toBuilder().clearSegments()
                .addSegments(segment("c", 0, "v1", 8, 10)).addSegments(segment("b", 1, "of", 5, 7)).addSegments(segment("a", 2, "body", 0, 4)).build();
        var saved = f.saves.save(processAuthority(), fullSave("op-rev", reversed, derivedSegmentation()).build());
        var entry = segmentationEntry(saved.getAccession().getManifest(), S1);
        assertThat(entry.getSegmentationDigest()).isEqualTo(SegmentationDigests.digest(reversed))
                .isNotEqualTo(SegmentationDigests.digest(bodySegmentation()));
        var read = f.reads.getDocument(processAuthority(), GetDocumentRequest.newBuilder().setAddress(address)
                .setSelection(PartSelection.newBuilder().addSelectors(segmentation(S1))).build());
        assertThat(read.getDocument().getSearchMetadata().getSegmentations(0).getSegmentsList())
                .extracting(Segment::getSegmentId).containsExactly("c", "b", "a");
    }

    @Test void bindingOffsetsAndTextAreVerifiedAgainstTheComposedDocument() {
        var wrongSha = bodySegmentation().toBuilder();
        wrongSha.getSourceBuilder().getBindingBuilder().setResolvedTextSha256("0".repeat(64));
        var sha = rejected(() -> f.saves.save(processAuthority(), fullSave("op-sha", wrongSha.build(), derivedSegmentation()).build()),
                Status.Code.INVALID_ARGUMENT, RejectionReason.REJECTION_REASON_VALIDATION_FAILED);
        assertThat(sha.getViolationsList()).extracting(FieldViolation::getRuleId).containsExactly("segmentation.binding-sha256");

        var wrongText = bodySegmentation().toBuilder().setSegments(0, segment("a", 0, "BODY", 0, 4)).build();
        var text = rejected(() -> f.saves.save(processAuthority(), fullSave("op-text", wrongText, derivedSegmentation()).build()),
                Status.Code.INVALID_ARGUMENT, RejectionReason.REJECTION_REASON_VALIDATION_FAILED);
        assertThat(text.getViolationsList()).extracting(FieldViolation::getRuleId, FieldViolation::getFieldPath)
                .containsExactly(tuple("segmentation.segment-text", "segments[0].text"));

        // Derived text with a multi-byte character: a byte offset inside it is refused.
        var split = derivedSegmentation().toBuilder().setSegments(0, segment("n0", 0, "wÃ", 6, 8)).build();
        var boundary = rejected(() -> f.saves.save(processAuthority(), fullSave("op-split", bodySegmentation(), split).build()),
                Status.Code.INVALID_ARGUMENT, RejectionReason.REJECTION_REASON_VALIDATION_FAILED);
        assertThat(boundary.getViolationsList()).extracting(FieldViolation::getRuleId).containsExactly("segmentation.segment-boundary");
        assertThat(boundary.getViolationsList().getFirst().getPart().getSegmentationId()).isEqualTo(S2);

        var missingField = bodySegmentation().toBuilder();
        missingField.getSourceBuilder().setFieldPath("search_metadata.keywords");
        var resolves = rejected(() -> f.saves.save(processAuthority(), fullSave("op-field", missingField.build(), derivedSegmentation()).build()),
                Status.Code.INVALID_ARGUMENT, RejectionReason.REJECTION_REASON_VALIDATION_FAILED);
        assertThat(resolves.getViolationsList()).extracting(FieldViolation::getRuleId).containsExactly("segmentation.source-resolves");

        // A quoted literal is a value to the mapper, not a field of the document; the binding over it would
        // otherwise verify against text that no part holds.
        var literal = bodySegmentation().toBuilder();
        literal.getSourceBuilder().setFieldPath("\"" + BODY + "\"");
        var quoted = rejected(() -> f.saves.save(processAuthority(), fullSave("op-literal", literal.build(), derivedSegmentation()).build()),
                Status.Code.INVALID_ARGUMENT, RejectionReason.REJECTION_REASON_VALIDATION_FAILED);
        assertThat(quoted.getViolationsList()).singleElement().satisfies(violation -> {
            assertThat(violation.getRuleId()).isEqualTo("segmentation.source-resolves");
            assertThat(violation.getFieldPath()).isEqualTo("source.field_path");
            assertThat(violation.getMessage()).contains("dot-separated field names");
        });
        var padded = bodySegmentation().toBuilder();
        padded.getSourceBuilder().setFieldPath(" search_metadata.body");
        rejected(() -> f.saves.save(processAuthority(), fullSave("op-padded", padded.build(), derivedSegmentation()).build()),
                Status.Code.INVALID_ARGUMENT, RejectionReason.REJECTION_REASON_VALIDATION_FAILED);

        var claimed = bodySegmentation().toBuilder().setContentDigest(SegmentationDigests.digest(bodySegmentation())).build();
        var saved = f.saves.save(processAuthority(), fullSave("op-claim", claimed, derivedSegmentation()).build());
        assertThat(segmentationEntry(saved.getAccession().getManifest(), S1).getSegmentationDigest()).isEqualTo(claimed.getContentDigest());
        var wrongClaim = bodySegmentation().toBuilder().setContentDigest("1".repeat(64)).build();
        var digest = rejected(() -> f.saves.save(processAuthority(), partial("op-wrong", 1, semantic(List.of(wrongClaim), List.of()),
                writeSegmentation(S1)).build()), Status.Code.INVALID_ARGUMENT, RejectionReason.REJECTION_REASON_VALIDATION_FAILED);
        assertThat(digest.getViolationsList()).extracting(FieldViolation::getRuleId).containsExactly("segmentation.content-digest");
        assertThat(f.stack.puts.get()).as("only the accepted save uploaded").isEqualTo(8);
    }

    @Test void rewritingCoreRereadsCarriedSegmentationsAndRefusesChangedSourceText() {
        f.saves.save(processAuthority(), fullSave("op-1").build());
        var before = f.catalog.manifest(address, 1).orElseThrow();
        f.stack.reset();

        // Same body, new title: the carried body-bound segmentation still binds.
        var retitled = document(account, "doc-sem", "v1").toBuilder().clearBlobBag().clearParserResults();
        retitled.getSearchMetadataBuilder().setTitle("v1 retitled");
        f.saves.save(processAuthority(), partial("op-2", 1, retitled.build(), put(DocumentPart.DOCUMENT_PART_CORE)).build());
        assertThat(f.stack.puts.get()).isEqualTo(1);
        assertThat(f.stack.opens.get()).as("both carried CHUNKS objects are read to learn their source parts").isEqualTo(2);
        assertThat(f.stack.opened).containsOnlyKeys(ref(before, segmentation(S1)).getKey(), ref(before, segmentation(S2)).getKey());
        var after = f.catalog.manifest(address, 2).orElseThrow();
        for (var selector : List.of(segmentation(S1), segmentation(S2), embedding(S1, M1), embedding(S2, M1)))
            assertThat(stored(after, selector)).isEqualTo(stored(before, selector));

        // New body: the carried segmentation no longer describes the text it is bound to.
        f.stack.reset();
        var rebodied = document(account, "doc-sem", "v2").toBuilder().clearBlobBag().clearParserResults().build();
        var rejected = rejected(() -> f.saves.save(processAuthority(), partial("op-3", 2, rebodied, put(DocumentPart.DOCUMENT_PART_CORE)).build()),
                Status.Code.INVALID_ARGUMENT, RejectionReason.REJECTION_REASON_VALIDATION_FAILED);
        // "body of v2" has the old length, so the length rule holds; the digest and the last segment do not.
        assertThat(rejected.getViolationsList()).extracting(violation -> violation.getPart().getSegmentationId(),
                FieldViolation::getRuleId, FieldViolation::getFieldPath)
                .containsExactlyInAnyOrder(tuple(S1, "segmentation.binding-sha256", "source.binding.resolved_text_sha256"),
                        tuple(S1, "segmentation.segment-text", "segments[2].text"));
        assertThat(f.stack.puts.get()).isZero();
        assertThat(f.catalog.current(address).orElseThrow().revision()).isEqualTo(2);

        // Writing PARSED reads the carried CHUNKS objects too, but verifies neither: no segmentation sources PARSED.
        f.stack.reset();
        var parsed = Document.newBuilder().putParserResults("docling", ai.protomolt.proto.repo.v1.ParserResult.newBuilder()
                .setParserName("docling").setParserVersion("2").setStatus(ai.protomolt.proto.repo.v1.ParseStatus.PARSE_STATUS_OK).build()).build();
        f.saves.save(processAuthority(), partial("op-4", 2, parsed, put(DocumentPart.DOCUMENT_PART_PARSED)).build());
        assertThat(f.stack.opens.get()).isEqualTo(2);
        assertThat(f.stack.opened).doesNotContainKey(ref(before, CORE_SELECTOR()).getKey());
    }

    @Test void embeddingSetsMustCoverTheirSegmentationAndPassTheContractRules() {
        f.saves.save(processAuthority(), fullSave("op-1").build());
        var digest = stored(f.catalog.manifest(address, 1).orElseThrow(), segmentation(S1)).getEntry().getSegmentationDigest();
        f.stack.reset();

        var missing = rejected(() -> f.saves.save(processAuthority(), partial("op-missing", 1,
                semantic(List.of(), List.of(embeddingSet(S1, M2, digest, "a", "b"))), writeEmbedding(S1, M2)).build()),
                Status.Code.FAILED_PRECONDITION, RejectionReason.REJECTION_REASON_SEGMENTATION_MISMATCH);
        assertThat(missing.getViolationsList()).singleElement().satisfies(violation -> {
            assertThat(violation.getRuleId()).isEqualTo("embedding-set.vector-coverage");
            assertThat(violation.getMessage()).contains("[c]");
        });
        var orphan = rejected(() -> f.saves.save(processAuthority(), partial("op-orphan", 1,
                semantic(List.of(), List.of(embeddingSet("nowhere", M2, digest, "a"))), writeEmbedding("nowhere", M2)).build()),
                Status.Code.FAILED_PRECONDITION, RejectionReason.REJECTION_REASON_SEGMENTATION_MISMATCH);
        assertThat(orphan.getViolationsList()).extracting(FieldViolation::getRuleId).containsExactly("embedding-set.segmentation-present");

        // The contract's own rules on EmbeddingSet and SegmentVector (vector-dimensions, finite
        // floats) are request structure: ProtoValidator refuses the save request before anything
        // is authorized or recorded, as it does for every rule declared on the Document.
        var shortVector = embeddingSet(S1, M2, digest, "a", "b", "c").toBuilder();
        shortVector.setVectors(1, shortVector.getVectors(1).toBuilder().clearValues().addValues(1f));
        var dims = rejected(() -> f.saves.save(processAuthority(), partial("op-dims", 1,
                semantic(List.of(), List.of(shortVector.build())), writeEmbedding(S1, M2)).build()),
                Status.Code.INVALID_ARGUMENT, RejectionReason.REJECTION_REASON_MALFORMED_REQUEST);
        assertThat(dims.getMessage()).contains("vector-dimensions");
        var nan = embeddingSet(S1, M2, digest, "a", "b", "c").toBuilder();
        nan.setVectors(0, nan.getVectors(0).toBuilder().setValues(0, Float.NaN));
        var finite = rejected(() -> f.saves.save(processAuthority(), partial("op-nan", 1,
                semantic(List.of(), List.of(nan.build())), writeEmbedding(S1, M2)).build()),
                Status.Code.INVALID_ARGUMENT, RejectionReason.REJECTION_REASON_MALFORMED_REQUEST);
        assertThat(finite.getMessage()).contains("embedding_sets[0].vectors[0].values[0]");
        assertThat(f.catalog.outcome(account, "op-dims")).isEmpty();
        assertThat(f.stack.puts.get()).isZero();
        assertThat(f.catalog.current(address).orElseThrow().revision()).isEqualTo(1);
    }

    @Test void semanticSavesReplayByOperationIdAndRefuseMalformedShapes() {
        var request = fullSave("op-1").build();
        var first = f.saves.save(processAuthority(), request);
        var again = f.saves.save(processAuthority(), request);
        assertThat(again.getReplayed()).isTrue();
        assertThat(again.getAccession()).isEqualTo(first.getAccession());
        assertThat(f.stack.puts.get()).isEqualTo(8);

        var notWritten = fullSave("op-2").clearWrites().addAllWrites(putAll()).addWrites(writeSegmentation(S1))
                .addWrites(writeEmbedding(S1, M1)).addWrites(writeEmbedding(S2, M1)).build();
        assertThatThrownBy(() -> f.saves.save(processAuthority(), notWritten)).isInstanceOfSatisfying(AccessionException.class,
                failure -> assertThat(failure.getMessage()).contains("CHUNKS " + S2).contains("not written"));
        var noContent = fullSave("op-2").addWrites(writeSegmentation("ghost")).build();
        assertThatThrownBy(() -> f.saves.save(processAuthority(), noContent)).isInstanceOfSatisfying(AccessionException.class,
                failure -> assertThat(failure.getMessage()).contains("CHUNKS ghost").contains("write EMPTY"));
        var twice = fullSave("op-2", bodySegmentation(), bodySegmentation().toBuilder().setSegmentationId(S1).build()).build();
        assertThatThrownBy(() -> f.saves.save(processAuthority(), twice)).isInstanceOfSatisfying(AccessionException.class,
                failure -> assertThat(failure.getMessage()).contains("Duplicate").contains(S1));
        var legacy = fullSave("op-2").build().toBuilder();
        legacy.getDocumentBuilder().getSearchMetadataBuilder().addSemanticResults(SemanticProcessingResult.newBuilder().setResultId("r"));
        assertThatThrownBy(() -> f.saves.save(processAuthority(), legacy.build())).isInstanceOfSatisfying(AccessionException.class,
                failure -> assertThat(failure.getMessage()).contains("Legacy semantic results"));
        assertThat(f.catalog.outcome(account, "op-2")).as("request structure failures are not recorded").isEmpty();

        var emptied = f.saves.save(processAuthority(), SaveDocumentRequest.newBuilder().setOperationId("op-3")
                .setTarget(DocumentTarget.newBuilder().setAddress(address)).setExpected(at(1)).setOwner(intakeOwner())
                .addWrites(PartWrite.newBuilder().setSelector(embedding(S2, M1)).setAction(WriteAction.WRITE_ACTION_DELETE)
                        .setDeletedReason("model retired")).build());
        assertThat(stored(f.catalog.manifest(address, 2).orElseThrow(), embedding(S2, M1)).getEntry().getState())
                .isEqualTo(PartState.PART_STATE_DELETED);
        assertThat(emptied.getAccession().getRevision()).isEqualTo(2);
        var orphaned = rejected(() -> f.saves.save(processAuthority(), SaveDocumentRequest.newBuilder().setOperationId("op-4")
                .setTarget(DocumentTarget.newBuilder().setAddress(address)).setExpected(at(2)).setOwner(intakeOwner())
                .addWrites(PartWrite.newBuilder().setSelector(segmentation(S1)).setAction(WriteAction.WRITE_ACTION_EMPTY)).build()),
                Status.Code.FAILED_PRECONDITION, RejectionReason.REJECTION_REASON_SEGMENTATION_MISMATCH);
        assertThat(orphaned.getViolationsList()).singleElement().satisfies(violation -> {
            assertThat(violation.getPart().getEmbeddingSet()).isEqualTo(key(S1, M1));
            assertThat(violation.getRuleId()).isEqualTo("embedding-set.segmentation-present");
        });
    }

    // ------------------------------------------------------------------ fixtures

    static PartSelector CORE_SELECTOR() { return part(DocumentPart.DOCUMENT_PART_CORE); }
    static PartSelector part(DocumentPart part) { return PartSelector.newBuilder().setPart(part).build(); }
    static PartSelector segmentation(String id) { return PartSelector.newBuilder().setSegmentationId(id).build(); }
    static PartSelector embedding(String segmentation, String set) { return PartSelector.newBuilder().setEmbeddingSet(key(segmentation, set)).build(); }
    static EmbeddingSetKey key(String segmentation, String set) {
        return EmbeddingSetKey.newBuilder().setSegmentationId(segmentation).setEmbeddingSetId(set).build();
    }
    static PartWrite writeSegmentation(String id) {
        return PartWrite.newBuilder().setSelector(segmentation(id)).setAction(WriteAction.WRITE_ACTION_PUT).build();
    }
    static PartWrite writeEmbedding(String segmentation, String set) {
        return PartWrite.newBuilder().setSelector(embedding(segmentation, set)).setAction(WriteAction.WRITE_ACTION_PUT).build();
    }

    static StoredPartEntry stored(StoredManifest manifest, PartSelector selector) {
        return manifest.getPartsList().stream().filter(p -> p.getEntry().getSelector().equals(selector)).findFirst()
                .orElseThrow(() -> new AssertionError("no " + DocumentSaves.describe(selector) + " in revision " + manifest.getRevision()));
    }
    static StackRef ref(StoredManifest manifest, PartSelector selector) { return stored(manifest, selector).getRef(); }
    static PartManifestEntry segmentationEntry(DocumentManifest manifest, String id) {
        return manifest.getPartsList().stream().filter(p -> p.getSelector().equals(segmentation(id))).findFirst().orElseThrow();
    }
    static PartManifestEntry embeddingEntry(DocumentManifest manifest, String segmentation, String set) {
        return manifest.getPartsList().stream().filter(p -> p.getSelector().equals(embedding(segmentation, set))).findFirst().orElseThrow();
    }

    /** The body-bound segmentation over "body of v1" in UTF-8 bytes. */
    static Segmentation bodySegmentation() {
        var utf8 = BODY.getBytes(StandardCharsets.UTF_8);
        return Segmentation.newBuilder().setSegmentationId(S1).setKind(SegmentationKind.SEGMENTATION_KIND_TOKEN_WINDOW)
                .setSource(TextSource.newBuilder().setFieldPath("search_metadata.body").setBinding(SourceTextBinding.newBuilder()
                        .setResolvedTextSha256(DocumentSaves.sha256(com.google.protobuf.ByteString.copyFrom(utf8))).setUtf8Length(utf8.length)))
                .setOffsetUnit(OffsetUnit.OFFSET_UNIT_UTF8_BYTE)
                .addSegments(segment("a", 0, "body", 0, 4)).addSegments(segment("b", 1, "of", 5, 7)).addSegments(segment("c", 2, "v1", 8, 10))
                .build();
    }

    /** A segmentation over derived text "hello wörld" (12 UTF-8 bytes), cut in code points. */
    static Segmentation derivedSegmentation() {
        var text = "hello wörld";
        var utf8 = text.getBytes(StandardCharsets.UTF_8);
        return Segmentation.newBuilder().setSegmentationId(S2).setKind(SegmentationKind.SEGMENTATION_KIND_SEMANTIC_BOUNDARY)
                .setSource(TextSource.newBuilder().setDerived(DerivedText.newBuilder().setDerivationId("lowercase").setText(text))
                        .setBinding(SourceTextBinding.newBuilder()
                                .setResolvedTextSha256(DocumentSaves.sha256(com.google.protobuf.ByteString.copyFrom(utf8))).setUtf8Length(utf8.length)))
                .setOffsetUnit(OffsetUnit.OFFSET_UNIT_UTF8_BYTE)
                .addSegments(segment("n0", 0, "hello", 0, 5)).addSegments(segment("n1", 1, "wörld", 6, 12))
                .build();
    }

    static Segment segment(String id, int ordinal, String text, long start, long end) {
        return Segment.newBuilder().setSegmentId(id).setOrdinal(ordinal).setText(text).setStart(start).setEnd(end)
                .putMetadata("ordinal-" + ordinal, "set").build();
    }

    static EmbeddingSet embeddingSet(String segmentation, String id, String digest, String... segmentIds) {
        var set = EmbeddingSet.newBuilder().setEmbeddingSetId(id).setSegmentationId(segmentation).setSegmentationDigest(digest)
                .setModel(id + "-v1").setDimensions(2).setSimilarity(Similarity.SIMILARITY_COSINE);
        for (var segmentId : segmentIds)
            set.addVectors(SegmentVector.newBuilder().setSegmentId(segmentId).addValues(0.25f).addValues(-0.5f));
        return set.build();
    }

    /** A document fragment carrying only the given semantic parts. */
    static Document semantic(List<Segmentation> segmentations, List<EmbeddingSet> sets) {
        return Document.newBuilder().setSearchMetadata(SearchMetadata.newBuilder().addAllSegmentations(segmentations)
                .addAllEmbeddingSets(sets)).build();
    }

    /** The fixture document plus the two segmentations and one embedding set each, with every part written. */
    SaveDocumentRequest.Builder fullSave(String operation) { return fullSave(operation, bodySegmentation(), derivedSegmentation()); }

    SaveDocumentRequest.Builder fullSave(String operation, Segmentation s1, Segmentation s2) {
        var base = document(account, "doc-sem", "v1").toBuilder();
        base.getSearchMetadataBuilder().addSegmentations(s1).addSegmentations(s2)
                .addEmbeddingSets(embeddingSet(s1.getSegmentationId(), M1, SegmentationDigests.digest(s1),
                        s1.getSegmentsList().stream().map(Segment::getSegmentId).toArray(String[]::new)))
                .addEmbeddingSets(embeddingSet(s2.getSegmentationId(), M1, SegmentationDigests.digest(s2),
                        s2.getSegmentsList().stream().map(Segment::getSegmentId).toArray(String[]::new)));
        return create(operation, address, base.build()).addWrites(writeSegmentation(s1.getSegmentationId()))
                .addWrites(writeSegmentation(s2.getSegmentationId())).addWrites(writeEmbedding(s1.getSegmentationId(), M1))
                .addWrites(writeEmbedding(s2.getSegmentationId(), M1));
    }

    SaveDocumentRequest.Builder partial(String operation, long expected, Document content, PartWrite... writes) {
        return SaveDocumentRequest.newBuilder().setOperationId(operation).setTarget(DocumentTarget.newBuilder().setAddress(address))
                .setExpected(at(expected)).setDocument(content).addAllWrites(List.of(writes)).setOwner(intakeOwner());
    }

    static SaveRejection rejected(ThrowingCall call, Status.Code code, RejectionReason reason) {
        var failure = catchThrowableOfType(AccessionException.class, call::run);
        assertThat(failure).as("expected a %s rejection", reason).isNotNull();
        assertThat(failure.code()).as(failure.getMessage()).isEqualTo(code);
        var rejection = (SaveRejection) failure.detail().orElseThrow();
        assertThat(rejection.getReason()).as(rejection.getMessage()).isEqualTo(reason);
        return rejection;
    }

    @FunctionalInterface interface ThrowingCall { void run() throws Exception; }
}
