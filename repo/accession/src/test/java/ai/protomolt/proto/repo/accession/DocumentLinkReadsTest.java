package ai.protomolt.proto.repo.accession;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.groups.Tuple.tuple;

import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.repo.admission.BuiltinDocumentSchema;
import ai.protomolt.proto.repo.admission.DocumentAdmissionReservations;
import ai.protomolt.proto.repo.admission.DocumentCandidateAdmission;
import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.admission.DocumentSchemaRetention;
import ai.protomolt.proto.repo.catalog.Catalog;
import ai.protomolt.proto.repo.catalog.CatalogRecords;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.stack.Stack;
import ai.protomolt.proto.repo.stack.StackDataLoss;
import ai.protomolt.proto.repo.v1.Access;
import ai.protomolt.proto.repo.v1.AccessRule;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.OwnershipContext;
import ai.protomolt.proto.repo.v1.Principal;
import ai.protomolt.proto.repo.v1.PublicationSchemaCondition;
import ai.protomolt.proto.repo.v1.RepositoryAnyResolution;
import ai.protomolt.proto.repo.v1.RepositoryOccurrenceKeyType;
import ai.protomolt.proto.repo.v1.RepositoryOccurrenceMapKey;
import ai.protomolt.proto.repo.v1.RepositoryResolvedSchema;
import ai.protomolt.proto.repo.v1.RepositorySchemaAsset;
import ai.protomolt.proto.repo.v1.RepositorySchemaOccurrenceStep;
import ai.protomolt.proto.repo.v1.SchemaCompilationOrigin;
import ai.protomolt.proto.repo.v1.SchemaCompilationProvenance;
import ai.protomolt.proto.repo.v1.SchemaCompilerEvidence;
import ai.protomolt.proto.repo.v1.SchemaToolIdentity;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v2.CatalogEntry;
import ai.protomolt.proto.repo.v2.DocumentFilter;
import ai.protomolt.proto.repo.v2.DocumentLink;
import ai.protomolt.proto.repo.v2.DocumentLookup;
import ai.protomolt.proto.repo.v2.DynamicTarget;
import ai.protomolt.proto.repo.v2.LinkField;
import ai.protomolt.proto.repo.v2.LinksProto;
import ai.protomolt.proto.repo.v2.ListLinksRequest;
import ai.protomolt.proto.repo.v2.OccurrenceLocator;
import ai.protomolt.proto.repo.v2.OpaqueOccurrence;
import ai.protomolt.proto.repo.v2.PartManifestEntry;
import ai.protomolt.proto.repo.v2.PartSchemaBinding;
import ai.protomolt.proto.repo.v2.PartState;
import ai.protomolt.proto.repo.v2.ResolveLinkRequest;
import ai.protomolt.proto.repo.v2.ResolvedLink;
import ai.protomolt.proto.repo.v2.ResolvedOccurrence;
import ai.protomolt.proto.repo.v2.RevisionValidation;
import ai.protomolt.proto.repo.v2.StackRef;
import ai.protomolt.proto.repo.v2.StaticTarget;
import ai.protomolt.proto.repo.v2.StoredManifest;
import ai.protomolt.proto.repo.v2.StoredPartEntry;
import ai.protomolt.proto.repo.v2.WriterGrant;
import ai.protomolt.proto.repo.v2.WriterGrantEvidence;
import ai.protomolt.proto.types.ConceptRef;
import ai.protomolt.proto.types.ConceptsProto;
import ai.protomolt.proto.validate.FieldRules;
import ai.protomolt.proto.validate.ValidateProto;
import com.google.protobuf.Any;
import com.google.protobuf.AnyProto;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldOptions;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.MessageOptions;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Timestamp;
import io.grpc.Status;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for lightweight {@code CORE} link extraction (issue #608) in
 * {@link DocumentReads#coreLinks}, {@link DocumentLinkReads#listLinks}, and
 * {@link DocumentLinkReads#resolveLink}.
 */
class DocumentLinkReadsTest {
    private static final String ACCOUNT = "account-1";
    private static final String STACK_ID = "stack-1";
    private static final String DRIVE = "default";
    private static final String LINK_TYPE = ".ai.protomolt.proto.repo.v2.DocumentLink";
    private static final String CONCEPT_REF_TYPE = ".ai.protomolt.proto.types.v1.ConceptRef";
    private static final String CITATION_URL = "type.googleapis.com/test.links.unit.v1.Citation";
    private static final String TAGGED_URL = "type.googleapis.com/test.links.unit.v1.Tagged";
    private static final String ANY_URL = "type.googleapis.com/google.protobuf.Any";
    private static final String OPAQUE_URL = "type.googleapis.com/test.links.unit.v1.OpaqueNote";

    private static final FileDescriptor FILE = buildTestFile();
    private static final Descriptor CITATION = FILE.findMessageTypeByName("Citation");
    private static final Descriptor SECTION = FILE.findMessageTypeByName("Section");
    private static final Descriptor TAGGED = FILE.findMessageTypeByName("Tagged");
    private static final Descriptor NOTES_ENTRY = CITATION.findNestedTypeByName("NotesEntry");

    private static final DocumentSchemaAdmission.Definition DOCUMENT_DEF =
            BuiltinDocumentSchema.definition(Document.getDescriptor());
    private static final DocumentSchemaRetention.Retained DOCUMENT_RETAINED =
            DocumentSchemaRetention.retain(DOCUMENT_DEF);
    private static final DocumentSchemaAdmission.Definition CITATION_DEF =
            definition(CITATION, CITATION_URL);
    private static final DocumentSchemaRetention.Retained CITATION_RETAINED =
            DocumentSchemaRetention.retain(CITATION_DEF);
    private static final DocumentSchemaAdmission.Definition TAGGED_DEF =
            definition(TAGGED, TAGGED_URL);
    private static final DocumentSchemaRetention.Retained TAGGED_RETAINED =
            DocumentSchemaRetention.retain(TAGGED_DEF);
    private static final DocumentSchemaAdmission.Definition ANY_DEF =
            definition(Any.getDescriptor(), ANY_URL);
    private static final DocumentSchemaRetention.Retained ANY_RETAINED =
            DocumentSchemaRetention.retain(ANY_DEF);
    private static final DocumentSchemaRetention.Retained GRANT_SCHEMA =
            DocumentSchemaRetention.retain(BuiltinDocumentSchema.definition(WriterGrant.getDescriptor()));
    private static final WriterGrantEvidence OPAQUE_GRANT_EVIDENCE = buildOpaqueGrantEvidence();

    private static final DocumentSchemaAdmission.Limits LIMITS = DocumentSchemaAdmission.Limits.SERVICE_DEFAULTS;
    private static final DocumentAdmissionReservations RESERVATIONS = bytes -> () -> {};
    private static final RepositoryCaller READER = new RepositoryCaller(
            "reader", false, Set.of(ACCOUNT),
            Set.of(Principal.newBuilder().setIdentityType("service").setIdentity("reader").build()));

    @Test
    void listLinksAndResolveLinkExtractTopLevelAndNestedAnyLinksAndDeduplicateArtifacts() {
        var harness = new Harness();
        var t1 = address("target-1");
        var t2 = address("target-2");
        harness.putTarget(t1, "cat-1", false);
        harness.putTarget(t2, "cat-2", false);

        var opaqueBytes = ByteString.copyFromUtf8("opaque-payload");
        var opaqueAny = Any.newBuilder().setTypeUrl(OPAQUE_URL).setValue(opaqueBytes).build();
        var nestedCitation = citation(c -> {
            c.setField(CITATION.findFieldByName("cites"), link(staticTarget(t2, null)));
            c.setField(CITATION.findFieldByName("related"), link(dynamicTarget("cat-2")));
        });
        var extraZero = citation(c -> c.addRepeatedField(
                CITATION.findFieldByName("sections"), section(staticTarget(t1, 1L))));
        var extraOneInner = citation(c -> c.setField(
                CITATION.findFieldByName("cites"), link(staticTarget(t2, null))));
        var extraOneWrapped = Any.newBuilder()
                .setTypeUrl(ANY_URL)
                .setValue(packCitation(extraOneInner).toByteString())
                .build();

        var rootCitation = citation(c -> {
            c.setField(CITATION.findFieldByName("cites"), link(staticTarget(t1, null)));
            c.addRepeatedField(CITATION.findFieldByName("sections"), section(staticTarget(t2, null)));
            c.setField(CITATION.findFieldByName("nested"), packCitation(nestedCitation));
            c.addRepeatedField(CITATION.findFieldByName("extras"), packCitation(extraZero));
            c.addRepeatedField(CITATION.findFieldByName("extras"), extraOneWrapped);
            c.addRepeatedField(CITATION.findFieldByName("notes"),
                    DynamicMessage.newBuilder(NOTES_ENTRY)
                            .setField(NOTES_ENTRY.findFieldByName("key"), "opaque-key")
                            .setField(NOTES_ENTRY.findFieldByName("value"), opaqueAny)
                            .build());
        });

        var src = address("src");
        var stored = harness.storeAdmitted(src, 1L, document("src", rootCitation),
                List.of(new DocumentCandidateAdmission.AttestedOpaque(
                        ai.protomolt.proto.repo.v1.DocumentPart.DOCUMENT_PART_CORE,
                        List.of(
                                stepField(Document.STRUCTURED_DATA_FIELD_NUMBER),
                                stepBoundary(CITATION_URL, rootCitation.toByteString(), CITATION_DEF, CITATION_RETAINED),
                                stepField(CITATION.findFieldByName("notes").getNumber()),
                                stepMapKey("opaque-key")),
                        OPAQUE_URL)));

        assertThat(stored.getValidation().getResolvedOccurrencesCount()).isEqualTo(5);
        assertThat(stored.getValidation().getOpaqueOccurrencesCount()).isEqualTo(1);

        harness.artifactReads.clear();
        var listed = harness.linkReads.listLinks(READER, ListLinksRequest.newBuilder().setAddress(src).build());
        assertThat(listed.getRevision()).isEqualTo(1L);
        assertThat(listed.getLinksList())
                .extracting(ResolvedLink::getFieldPath, ResolvedLink::getOrdinal, ResolvedLink::getRelation)
                .containsExactly(
                        tuple("structured_data.cites", 0, "references"),
                        tuple("structured_data.sections[0].cites", 0, "references"),
                        tuple("structured_data.nested.cites", 0, "references"),
                        tuple("structured_data.nested.related", 0, "related"),
                        tuple("structured_data.extras[0].sections[0].cites", 0, "references"),
                        tuple("structured_data.extras[1].cites", 0, "references"));

        // Although Citation appears 4 times in resolved_occurrences, its descriptor artifact is read once.
        assertThat(harness.artifactReads.get(CITATION_RETAINED.reference().getDescriptorSha256())).isEqualTo(1);
        assertThat(harness.artifactReads.get(DOCUMENT_RETAINED.reference().getDescriptorSha256())).isEqualTo(1);
        assertThat(harness.artifactReads.get(ANY_RETAINED.reference().getDescriptorSha256())).isEqualTo(1);

        harness.artifactReads.clear();
        var resolved = harness.linkReads.resolveLink(READER, ResolveLinkRequest.newBuilder()
                .setAddress(src)
                .setFieldPath("structured_data.nested.related")
                .setOrdinal(0)
                .setPageSize(10)
                .build());
        assertThat(resolved.getDocumentsList())
                .extracting(summary -> summary.getAddress().getDocId())
                .containsExactly("target-2");
        assertThat(harness.artifactReads.get(CITATION_RETAINED.reference().getDescriptorSha256())).isEqualTo(1);

        // Verify decodeRetained also deduplicates artifact reads across repeated schema occurrences.
        harness.artifactReads.clear();
        var coreBytes = harness.stack.objects.get(stored.getParts(0).getRef().getKey());
        var retained = harness.reads.decodeRetained(stored, DocumentSaves.CORE_SELECTOR, coreBytes);
        assertThat(retained.links()).hasSize(6);
        assertThat(harness.artifactReads.get(CITATION_RETAINED.reference().getDescriptorSha256())).isEqualTo(1);
    }

    @Test
    void coreLinksListLinksAndResolveLinkDoNotEvaluateConceptTagsOrCandidateRules() {
        var harness = new Harness();
        var target = address("target-doc");
        harness.putTarget(target, "cat-target", false);

        // Build a Tagged message with an empty ConceptRef that fails both ProtoValidator and DocumentConcepts.extract,
        // and also place a Tagged Any under a map value (which DocumentConcepts refuses with TagUnderMap).
        var invalidConceptRef = ConceptRef.newBuilder().setSchemeId("").setConceptId("").build();
        var taggedPayload = DynamicMessage.newBuilder(TAGGED)
                .setField(TAGGED.findFieldByName("subject"), toDynamic(TAGGED.findFieldByName("subject").getMessageType(), invalidConceptRef))
                .build();
        var taggedAny = Any.newBuilder().setTypeUrl(TAGGED_URL).setValue(taggedPayload.toByteString()).build();

        var rootCitation = citation(c -> {
            c.setField(CITATION.findFieldByName("cites"), link(staticTarget(target, null)));
            c.setField(CITATION.findFieldByName("related"), link(dynamicTarget("cat-target")));
            c.setField(CITATION.findFieldByName("subject"),
                    toDynamic(CITATION.findFieldByName("subject").getMessageType(), invalidConceptRef));
            c.addRepeatedField(CITATION.findFieldByName("notes"),
                    DynamicMessage.newBuilder(NOTES_ENTRY)
                            .setField(NOTES_ENTRY.findFieldByName("key"), "map-tag")
                            .setField(NOTES_ENTRY.findFieldByName("value"), taggedAny)
                            .build());
        });

        var src = address("concept-unscanned");
        var rootBytes = rootCitation.toByteString();
        var rootStep = stepBoundary(CITATION_URL, rootBytes, CITATION_DEF, CITATION_RETAINED);
        var mapTagStep = stepBoundary(TAGGED_URL, taggedPayload.toByteString(), TAGGED_DEF, TAGGED_RETAINED);
        var resolvedOccurrences = List.of(
                ResolvedOccurrence.newBuilder()
                        .setOccurrence(OccurrenceLocator.newBuilder()
                                .setPart(DocumentSaves.CORE_SELECTOR)
                                .addSteps(stepField(Document.STRUCTURED_DATA_FIELD_NUMBER))
                                .addSteps(rootStep))
                        .setSchema(CITATION_RETAINED.reference())
                        .setPayloadSha256(DocumentPartCodec.sha256Hex(rootBytes))
                        .setPayloadLength(rootBytes.size())
                        .build(),
                ResolvedOccurrence.newBuilder()
                        .setOccurrence(OccurrenceLocator.newBuilder()
                                .setPart(DocumentSaves.CORE_SELECTOR)
                                .addSteps(stepField(Document.STRUCTURED_DATA_FIELD_NUMBER))
                                .addSteps(rootStep)
                                .addSteps(stepField(CITATION.findFieldByName("notes").getNumber()))
                                .addSteps(stepMapKey("map-tag"))
                                .addSteps(mapTagStep))
                        .setSchema(TAGGED_RETAINED.reference())
                        .setPayloadSha256(DocumentPartCodec.sha256Hex(taggedPayload.toByteString()))
                        .setPayloadLength(taggedPayload.toByteString().size())
                        .build());

        var stored = harness.storeManual(src, 1L, document("concept-unscanned", rootCitation),
                resolvedOccurrences, List.of());

        // coreLinks, listLinks, and resolveLink succeed without running DocumentConcepts or ProtoValidator.
        assertThat(harness.reads.coreLinks(stored)).hasSize(2);
        var listed = harness.linkReads.listLinks(READER, ListLinksRequest.newBuilder().setAddress(src).build());
        assertThat(listed.getLinksList())
                .extracting(ResolvedLink::getFieldPath, ResolvedLink::getRelation)
                .containsExactly(
                        tuple("structured_data.cites", "references"),
                        tuple("structured_data.related", "related"));

        var resolved = harness.linkReads.resolveLink(READER, ResolveLinkRequest.newBuilder()
                .setAddress(src)
                .setFieldPath("structured_data.related")
                .setOrdinal(0)
                .setPageSize(10)
                .build());
        assertThat(resolved.getDocumentsList())
                .extracting(summary -> summary.getAddress().getDocId())
                .containsExactly("target-doc");

        // Meanwhile decodeRetained (used by CatalogEntryRebuild / getDocument) still runs full retained admission and fails DATA_LOSS.
        var coreBytes = harness.stack.objects.get(stored.getParts(0).getRef().getKey());
        assertThatThrownBy(() -> harness.reads.decodeRetained(stored, DocumentSaves.CORE_SELECTOR, coreBytes))
                .isInstanceOfSatisfying(AccessionException.class,
                        e -> assertThat(e.code()).isEqualTo(Status.Code.DATA_LOSS));
    }

    @Test
    void coreLinksPreservesDataLossOnCorruptBytesMissingArtifactsAndOccurrenceMismatch() {
        var harness = new Harness();
        var target = address("target-1");
        harness.putTarget(target, "cat-1", false);

        var rootCitation = citation(c -> c.setField(
                CITATION.findFieldByName("cites"), link(staticTarget(target, null))));
        var src = address("src-loss");
        var stored = harness.storeAdmitted(src, 1L, document("src-loss", rootCitation), List.of());

        // 1. Corrupt CORE wire bytes in the stored part (with matching StackRef digest so stack read passes).
        var corruptAddress = address("corrupt-bytes");
        harness.storeRawBytes(
                corruptAddress, 1L, ByteString.copyFrom(new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF}),
                stored.getValidation());
        assertDataLoss(() -> harness.linkReads.listLinks(READER,
                ListLinksRequest.newBuilder().setAddress(corruptAddress).build()));

        // 2. Missing retained schema artifact in catalog.
        var citationSha = CITATION_RETAINED.reference().getDescriptorSha256();
        var savedArtifact = harness.artifacts.remove(citationSha);
        try {
            assertDataLoss(() -> harness.linkReads.listLinks(READER,
                    ListLinksRequest.newBuilder().setAddress(src).build()));
        } finally {
            harness.artifacts.put(citationSha, savedArtifact);
        }

        // 3. Corrupt retained schema artifact failing its SHA-256 digest.
        harness.artifacts.put(citationSha, ByteString.copyFromUtf8("corrupt-descriptor-bytes"));
        try {
            assertDataLoss(() -> harness.linkReads.listLinks(READER,
                    ListLinksRequest.newBuilder().setAddress(src).build()));
        } finally {
            harness.artifacts.put(citationSha, savedArtifact);
        }

        // 4. ResolvedOccurrence mismatch (manifest records no resolved occurrence while CORE has one).
        var missingResolvedAddress = address("missing-resolved");
        var missingResolvedValidation = stored.getValidation().toBuilder().clearResolvedOccurrences().build();
        harness.storeRawBytes(missingResolvedAddress, 1L,
                harness.stack.objects.get(stored.getParts(0).getRef().getKey()),
                missingResolvedValidation);
        assertDataLoss(() -> harness.linkReads.listLinks(READER,
                ListLinksRequest.newBuilder().setAddress(missingResolvedAddress).build()));

        // 5. OpaqueOccurrence mismatch (manifest records a bogus opaque digest).
        var opaqueBytes = ByteString.copyFromUtf8("opaque-body");
        var opaqueDoc = document("opaque-mismatch", citation(c -> c.addRepeatedField(
                CITATION.findFieldByName("notes"),
                DynamicMessage.newBuilder(NOTES_ENTRY)
                        .setField(NOTES_ENTRY.findFieldByName("key"), "k")
                        .setField(NOTES_ENTRY.findFieldByName("value"),
                                Any.newBuilder().setTypeUrl(OPAQUE_URL).setValue(opaqueBytes).build())
                        .build())));
        var opaqueAddress = address("opaque-mismatch");
        var opaqueStored = harness.storeAdmitted(opaqueAddress, 1L, opaqueDoc,
                List.of(new DocumentCandidateAdmission.AttestedOpaque(
                        ai.protomolt.proto.repo.v1.DocumentPart.DOCUMENT_PART_CORE,
                        List.of(
                                stepField(Document.STRUCTURED_DATA_FIELD_NUMBER),
                                stepBoundary(CITATION_URL,
                                        opaqueDoc.getStructuredData().getValue(), CITATION_DEF, CITATION_RETAINED),
                                stepField(CITATION.findFieldByName("notes").getNumber()),
                                stepMapKey("k")),
                        OPAQUE_URL)));
        var tamperedOpaqueValidation = opaqueStored.getValidation().toBuilder()
                .setOpaqueOccurrences(0, opaqueStored.getValidation().getOpaqueOccurrences(0).toBuilder()
                        .setSha256("0".repeat(64)))
                .build();
        var tamperedOpaqueAddress = address("opaque-tampered");
        harness.storeRawBytes(tamperedOpaqueAddress, 1L,
                harness.stack.objects.get(opaqueStored.getParts(0).getRef().getKey()),
                tamperedOpaqueValidation);
        assertDataLoss(() -> harness.linkReads.listLinks(READER,
                ListLinksRequest.newBuilder().setAddress(tamperedOpaqueAddress).build()));

        // 6. Link-declaring Any payload placed under a map value is refused as DATA_LOSS.
        var nestedUnderMap = citation(c -> c.addRepeatedField(
                CITATION.findFieldByName("notes"),
                DynamicMessage.newBuilder(NOTES_ENTRY)
                        .setField(NOTES_ENTRY.findFieldByName("key"), "bad-link")
                        .setField(NOTES_ENTRY.findFieldByName("value"), packCitation(rootCitation))
                        .build()));
        var mapBytes = nestedUnderMap.toByteString();
        var outerStep = stepBoundary(CITATION_URL, mapBytes, CITATION_DEF, CITATION_RETAINED);
        var innerStep = stepBoundary(CITATION_URL, rootCitation.toByteString(), CITATION_DEF, CITATION_RETAINED);
        var linkUnderMapAddress = address("link-under-map");
        harness.storeManual(linkUnderMapAddress, 1L, document("link-under-map", nestedUnderMap),
                List.of(
                        ResolvedOccurrence.newBuilder()
                                .setOccurrence(OccurrenceLocator.newBuilder()
                                        .setPart(DocumentSaves.CORE_SELECTOR)
                                        .addSteps(stepField(Document.STRUCTURED_DATA_FIELD_NUMBER))
                                        .addSteps(outerStep))
                                .setSchema(CITATION_RETAINED.reference())
                                .setPayloadSha256(DocumentPartCodec.sha256Hex(mapBytes))
                                .setPayloadLength(mapBytes.size())
                                .build(),
                        ResolvedOccurrence.newBuilder()
                                .setOccurrence(OccurrenceLocator.newBuilder()
                                        .setPart(DocumentSaves.CORE_SELECTOR)
                                        .addSteps(stepField(Document.STRUCTURED_DATA_FIELD_NUMBER))
                                        .addSteps(outerStep)
                                        .addSteps(stepField(CITATION.findFieldByName("notes").getNumber()))
                                        .addSteps(stepMapKey("bad-link"))
                                        .addSteps(innerStep))
                                .setSchema(CITATION_RETAINED.reference())
                                .setPayloadSha256(DocumentPartCodec.sha256Hex(rootCitation.toByteString()))
                                .setPayloadLength(rootCitation.toByteString().size())
                                .build()),
                List.of());
        assertDataLoss(() -> harness.linkReads.listLinks(READER,
                ListLinksRequest.newBuilder().setAddress(linkUnderMapAddress).build()));
    }

    private static void assertDataLoss(Runnable action) {
        assertThatThrownBy(action::run)
                .isInstanceOfSatisfying(AccessionException.class,
                        e -> assertThat(e.code()).isEqualTo(Status.Code.DATA_LOSS));
    }

    // ------------------------------------------------------------------ test harness

    private static final class Harness {
        final Map<String, ByteString> artifacts = new HashMap<>();
        final Map<String, Integer> artifactReads = new HashMap<>();
        final Map<NodeAddress, StoredManifest> manifests = new HashMap<>();
        final Map<NodeAddress, CatalogRecords.LinkTarget> targets = new HashMap<>();
        final List<CatalogRecords.ListedDocument> listedDocuments = new ArrayList<>();
        final FakeStack stack = new FakeStack();
        final Catalog catalog = buildCatalogProxy();
        final DocumentReads reads;
        final DocumentLinkReads linkReads;

        Harness() {
            artifacts.putAll(DOCUMENT_RETAINED.artifacts());
            artifacts.putAll(CITATION_RETAINED.artifacts());
            artifacts.putAll(TAGGED_RETAINED.artifacts());
            artifacts.putAll(ANY_RETAINED.artifacts());
            var drives = new DocumentSaves.Drives(DRIVE, Map.of(DRIVE, stack));
            this.reads = new DocumentReads(catalog, drives, LIMITS, RESERVATIONS, Clock.systemUTC());
            this.linkReads = new DocumentLinkReads(reads, new DocumentLists(catalog));
        }

        void putTarget(NodeAddress address, String category, boolean tombstone) {
            var entry = CatalogEntry.newBuilder()
                    .setOwnership(ownership())
                    .setLookup(DocumentLookup.newBuilder().setTitle(address.getDocId()).setCategory(category))
                    .build();
            targets.put(address, new CatalogRecords.LinkTarget(
                    1L, tombstone, entry,
                    Optional.of(new CatalogRecords.PinnedRevision(1L, tombstone, Optional.of(CITATION.getFullName())))));
            var ts = Timestamp.newBuilder().setSeconds(1_700_000_000L).build();
            listedDocuments.add(new CatalogRecords.ListedDocument(
                    address, 1L, tombstone, ts, entry,
                    new CatalogRecords.Cursor(address, ts, address.getDocId())));
        }

        StoredManifest storeAdmitted(
                NodeAddress address,
                long revision,
                Document document,
                List<DocumentCandidateAdmission.AttestedOpaque> opaque) {
            var coreBytes = RawBlobs.deterministic(document);
            var fragment = new DocumentCandidateAdmission.Fragment(
                    ai.protomolt.proto.repo.v1.DocumentPart.DOCUMENT_PART_CORE, coreBytes);
            DocumentSchemaAdmission.Resolver resolver = selection -> switch (selection.typeUrl()) {
                case CITATION_URL -> CITATION_DEF;
                case TAGGED_URL -> TAGGED_DEF;
                case ANY_URL -> ANY_DEF;
                default -> throw new IllegalArgumentException("Unexpected typeUrl: " + selection.typeUrl());
            };
            try (var result = DocumentCandidateAdmission.check(
                    new DocumentCandidateAdmission.Request(address.getDocId(), List.of(fragment), DOCUMENT_DEF, Set.copyOf(opaque)),
                    resolver, LIMITS, RESERVATIONS, Clock.systemUTC().instant(), () -> {})) {
                assertThat(result).isInstanceOf(DocumentCandidateAdmission.Accepted.class);
                var accepted = (DocumentCandidateAdmission.Accepted) result;

                var resolvedList = new ArrayList<ResolvedOccurrence>();
                for (var occ : accepted.occurrences()) {
                    resolvedList.add(ResolvedOccurrence.newBuilder()
                            .setOccurrence(OccurrenceLocator.newBuilder()
                                    .setPart(DocumentSaves.CORE_SELECTOR)
                                    .addAllSteps(occ.steps()))
                            .setSchema(occ.schema())
                            .setPayloadSha256(occ.payloadSha256())
                            .setPayloadLength(occ.payloadLength())
                            .build());
                }
                var opaqueList = new ArrayList<OpaqueOccurrence>();
                for (var occ : accepted.opaque()) {
                    opaqueList.add(OpaqueOccurrence.newBuilder()
                            .setOccurrence(OccurrenceLocator.newBuilder()
                                    .setPart(DocumentSaves.CORE_SELECTOR)
                                    .addAllSteps(occ.steps()))
                            .setTypeUrl(occ.typeUrl())
                            .setSha256(occ.sha256())
                            .setLength(occ.length())
                            .setWriterGrant(OPAQUE_GRANT_EVIDENCE)
                            .build());
                }
                return storeManual(address, revision, document, resolvedList, opaqueList);
            } catch (InvalidProtocolBufferException impossible) {
                throw new IllegalStateException(impossible);
            }
        }

        StoredManifest storeManual(
                NodeAddress address,
                long revision,
                Document document,
                List<ResolvedOccurrence> resolvedOccurrences,
                List<OpaqueOccurrence> opaqueOccurrences) {
            var coreBytes = RawBlobs.deterministic(document);
            var validation = RevisionValidation.newBuilder()
                    .addPartSchemas(PartSchemaBinding.newBuilder()
                            .setPart(DocumentSaves.CORE_SELECTOR)
                            .setFragmentSchema(DOCUMENT_RETAINED.reference()))
                    .addAllResolvedOccurrences(resolvedOccurrences)
                    .addAllOpaqueOccurrences(opaqueOccurrences)
                    .build();
            return storeRawBytes(address, revision, coreBytes, validation);
        }

        StoredManifest storeRawBytes(
                NodeAddress address,
                long revision,
                ByteString coreBytes,
                RevisionValidation validation) {
            var ref = stack.putBytes(coreBytes);
            var stored = StoredManifest.newBuilder()
                    .setAddress(address)
                    .setRevision(revision)
                    .setOwnership(ownership())
                    .addParts(StoredPartEntry.newBuilder()
                            .setEntry(PartManifestEntry.newBuilder()
                                    .setSelector(DocumentSaves.CORE_SELECTOR)
                                    .setState(PartState.PART_STATE_PRESENT)
                                    .setWrittenInRevision(revision))
                            .setRef(ref))
                    .setValidation(validation)
                    .build();
            manifests.put(address, stored);
            return stored;
        }

        @SuppressWarnings("unchecked")
        private Catalog buildCatalogProxy() {
            return (Catalog) Proxy.newProxyInstance(
                    Catalog.class.getClassLoader(),
                    new Class<?>[]{Catalog.class},
                    (proxy, method, args) -> switch (method.getName()) {
                        case "currentManifest" -> Optional.ofNullable(manifests.get((NodeAddress) args[0]));
                        case "current" -> {
                            var stored = manifests.get((NodeAddress) args[0]);
                            yield stored == null
                                    ? Optional.empty()
                                    : Optional.of(new CatalogRecords.Current(stored.getRevision(), stored.getTombstone()));
                        }
                        case "manifest" -> {
                            var stored = manifests.get((NodeAddress) args[0]);
                            long rev = (Long) args[1];
                            yield stored != null && stored.getRevision() == rev
                                    ? Optional.of(stored)
                                    : Optional.empty();
                        }
                        case "artifact" -> {
                            String sha = (String) args[0];
                            artifactReads.merge(sha, 1, Integer::sum);
                            yield Optional.ofNullable(artifacts.get(sha));
                        }
                        case "linkTarget" -> Optional.ofNullable(targets.get((NodeAddress) args[0]));
                        case "documents" -> {
                            var listing = (CatalogRecords.Listing) args[0];
                            var after = (Optional<CatalogRecords.Cursor>) args[1];
                            if (after.isPresent()) yield List.of();
                            String category = listing.filter().getCategory();
                            yield listedDocuments.stream()
                                    .filter(doc -> category.isEmpty()
                                            || doc.entry().getLookup().getCategory().equals(category))
                                    .toList();
                        }
                        default -> throw new UnsupportedOperationException("Unexpected Catalog call: " + method.getName());
                    });
        }
    }

    private static final class FakeStack implements Stack {
        final Map<String, ByteString> objects = new HashMap<>();
        private int seq;

        StackRef putBytes(ByteString bytes) {
            String key = "obj-" + (++seq);
            objects.put(key, bytes);
            return StackRef.newBuilder()
                    .setStackId(STACK_ID)
                    .setKey(key)
                    .setSha256(DocumentPartCodec.sha256Hex(bytes))
                    .setLength(bytes.size())
                    .build();
        }

        @Override public String stackId() { return STACK_ID; }

        @Override public StackRef put(InputStream body, long expectedLength) throws IOException {
            return putBytes(ByteString.readFrom(body));
        }

        @Override public StackRef put(byte[] body) {
            return putBytes(ByteString.copyFrom(body));
        }

        @Override public InputStream open(StackRef ref) throws IOException {
            var bytes = objects.get(ref.getKey());
            if (bytes == null) throw new StackDataLoss("Missing stack object " + ref.getKey());
            if (!DocumentPartCodec.sha256Hex(bytes).equals(ref.getSha256()) || bytes.size() != ref.getLength())
                throw new StackDataLoss("Stack object digest mismatch for " + ref.getKey());
            return new ByteArrayInputStream(bytes.toByteArray());
        }

        @Override public InputStream openRange(StackRef ref, long offset, long length) {
            throw new UnsupportedOperationException();
        }

        @Override public long head(StackRef ref) throws IOException {
            var bytes = objects.get(ref.getKey());
            if (bytes == null) throw new StackDataLoss("Missing stack object " + ref.getKey());
            return bytes.size();
        }

        @Override public void delete(StackRef ref) {}

        @Override public void close() {}
    }

    // ------------------------------------------------------------------ proto helpers

    private static WriterGrantEvidence buildOpaqueGrantEvidence() {
        var grant = WriterGrant.newBuilder().setGrantId("grant-opaque-1").setAccountId(ACCOUNT).build();
        var bytes = grant.toByteString();
        return WriterGrantEvidence.newBuilder()
                .setWriterGrantId("grant-opaque-1")
                .setWriterGrantSha256(DocumentPartCodec.sha256Hex(bytes))
                .setGrantSnapshot(bytes)
                .setSnapshotSchema(GRANT_SCHEMA.reference())
                .setCredentialGeneration(1L)
                .build();
    }

    private static FileDescriptor buildTestFile() {
        var subjectRule = FieldOptions.newBuilder()
                .setExtension(ValidateProto.field, FieldRules.newBuilder().addConceptSchemes("subject").build())
                .build();
        var file = FileDescriptorProto.newBuilder()
                .setName("test/links/unit/v1/citation.proto")
                .setPackage("test.links.unit.v1")
                .setSyntax("proto3")
                .addDependency(LinksProto.getDescriptor().getName())
                .addDependency(ConceptsProto.getDescriptor().getName())
                .addDependency(ValidateProto.getDescriptor().getName())
                .addDependency(AnyProto.getDescriptor().getName())
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("Section")
                        .addField(linkField("cites", 1, true, LinkField.newBuilder().setRelation("references"))))
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("Tagged")
                        .addField(messageField("subject", 1, CONCEPT_REF_TYPE, false).setOptions(subjectRule)))
                .addMessageType(DescriptorProto.newBuilder()
                        .setName("Citation")
                        .addField(FieldDescriptorProto.newBuilder()
                                .setName("title")
                                .setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                        .addField(linkField("cites", 2, false, LinkField.newBuilder().setRelation("references")))
                        .addField(messageField("sections", 3, ".test.links.unit.v1.Section", true))
                        .addField(linkField("related", 4, false, LinkField.newBuilder().setRelation("related")))
                        .addField(messageField("nested", 5, ".google.protobuf.Any", false))
                        .addField(messageField("extras", 6, ".google.protobuf.Any", true))
                        .addField(messageField("notes", 7, ".test.links.unit.v1.Citation.NotesEntry", true))
                        .addField(messageField("subject", 8, CONCEPT_REF_TYPE, false).setOptions(subjectRule))
                        .addNestedType(DescriptorProto.newBuilder()
                                .setName("NotesEntry")
                                .setOptions(MessageOptions.newBuilder().setMapEntry(true))
                                .addField(FieldDescriptorProto.newBuilder()
                                        .setName("key")
                                        .setNumber(1)
                                        .setType(FieldDescriptorProto.Type.TYPE_STRING)
                                        .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                                .addField(messageField("value", 2, ".google.protobuf.Any", false))))
                .build();
        try {
            return FileDescriptor.buildFrom(file, new FileDescriptor[]{
                    LinksProto.getDescriptor(),
                    ConceptsProto.getDescriptor(),
                    ValidateProto.getDescriptor(),
                    AnyProto.getDescriptor()
            });
        } catch (DescriptorValidationException invalid) {
            throw new IllegalStateException(invalid);
        }
    }

    private static FieldDescriptorProto.Builder linkField(
            String name, int number, boolean repeated, LinkField.Builder option) {
        return messageField(name, number, LINK_TYPE, repeated)
                .setOptions(FieldOptions.newBuilder().setExtension(LinksProto.link, option.build()));
    }

    private static FieldDescriptorProto.Builder messageField(
            String name, int number, String typeName, boolean repeated) {
        return FieldDescriptorProto.newBuilder()
                .setName(name)
                .setNumber(number)
                .setTypeName(typeName)
                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE)
                .setLabel(repeated
                        ? FieldDescriptorProto.Label.LABEL_REPEATED
                        : FieldDescriptorProto.Label.LABEL_OPTIONAL);
    }

    private static DocumentSchemaAdmission.Definition definition(Descriptor type, String typeUrl) {
        var closure = DescriptorFingerprints.closure(type);
        var bytes = closure.toByteString();
        var metadata = RepositorySchemaAsset.newBuilder()
                .setTypeUrl(typeUrl)
                .setArtifactSha256(DocumentPartCodec.sha256Hex(bytes))
                .setSchema(PublicationSchemaCondition.newBuilder()
                        .setTypeName(type.getFullName())
                        .setDescriptorFingerprint(DescriptorFingerprints.fingerprint(closure)))
                .setCompilation(SchemaCompilationProvenance.newBuilder()
                        .setOrigin(SchemaCompilationOrigin.SCHEMA_COMPILATION_ORIGIN_IMPORTED_DESCRIPTOR)
                        .setEvidence(SchemaCompilerEvidence.SCHEMA_COMPILER_EVIDENCE_UNKNOWN)
                        .setUnknownCompilerReason("test schema built from descriptor protos")
                        .setAdmissionRuntime(SchemaToolIdentity.newBuilder().setName("protobuf-java").setVersion("test")))
                .build();
        return new DocumentSchemaAdmission.Definition(metadata, bytes, Optional.empty());
    }

    private static NodeAddress address(String docId) {
        return NodeAddress.newBuilder()
                .setAccountId(ACCOUNT)
                .setGraphId("graph-1")
                .setGraphAddressId("addr-1")
                .setDocId(docId)
                .build();
    }

    private static OwnershipContext ownership() {
        return OwnershipContext.newBuilder()
                .setAccountId(ACCOUNT)
                .setDatasourceId("ds-1")
                .setSecurity(DocumentSecurity.newBuilder()
                        .addPermissions(AccessRule.newBuilder()
                                .setIdentityType("service")
                                .setIdentity("reader")
                                .setAccess(Access.ACCESS_READ)))
                .build();
    }

    private static Document document(String docId, Message payload) {
        return Document.newBuilder()
                .setDocId(docId)
                .setOwnership(ownership())
                .setSearchMetadata(SearchMetadata.newBuilder().setTitle(docId))
                .setStructuredData(packCitation(payload))
                .build();
    }

    private static Any packCitation(Message citation) {
        return Any.newBuilder().setTypeUrl(CITATION_URL).setValue(citation.toByteString()).build();
    }

    private static Message citation(Consumer<DynamicMessage.Builder> fill) {
        var builder = DynamicMessage.newBuilder(CITATION).setField(CITATION.findFieldByName("title"), "citation");
        fill.accept(builder);
        return builder.build();
    }

    private static Message section(DocumentLink... cites) {
        var builder = DynamicMessage.newBuilder(SECTION);
        for (var cite : cites) {
            builder.addRepeatedField(SECTION.findFieldByName("cites"), link(cite));
        }
        return builder.build();
    }

    private static Message link(DocumentLink link) {
        return toDynamic(DocumentLink.getDescriptor(), link);
    }

    private static DynamicMessage toDynamic(Descriptor descriptor, Message message) {
        try {
            return DynamicMessage.parseFrom(descriptor, message.toByteString());
        } catch (InvalidProtocolBufferException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static DocumentLink staticTarget(NodeAddress target, Long pinnedRevision) {
        var fixed = StaticTarget.newBuilder().setAddress(target);
        if (pinnedRevision != null) fixed.setRevision(pinnedRevision);
        return DocumentLink.newBuilder().setStaticTarget(fixed).build();
    }

    private static DocumentLink dynamicTarget(String category) {
        return DocumentLink.newBuilder()
                .setDynamicTarget(DynamicTarget.newBuilder()
                        .setFilter(DocumentFilter.newBuilder().setCategory(category)))
                .build();
    }

    private static RepositorySchemaOccurrenceStep stepField(int fieldNumber) {
        return RepositorySchemaOccurrenceStep.newBuilder().setFieldNumber(fieldNumber).build();
    }

    private static RepositorySchemaOccurrenceStep stepMapKey(String key) {
        return RepositorySchemaOccurrenceStep.newBuilder()
                .setMapKey(RepositoryOccurrenceMapKey.newBuilder()
                        .setType(RepositoryOccurrenceKeyType.REPOSITORY_OCCURRENCE_KEY_TYPE_STRING)
                        .setStringValue(key))
                .build();
    }

    private static RepositorySchemaOccurrenceStep stepBoundary(
            String typeUrl,
            ByteString value,
            DocumentSchemaAdmission.Definition def,
            DocumentSchemaRetention.Retained retained) {
        return RepositorySchemaOccurrenceStep.newBuilder()
                .setAnyBoundary(RepositoryAnyResolution.newBuilder()
                        .setTypeUrl(typeUrl)
                        .setValueSha256(DocumentPartCodec.sha256Hex(value))
                        .setValueSizeBytes(value.size())
                        .setResolved(RepositoryResolvedSchema.newBuilder()
                                .setSchema(def.metadata().getSchema())
                                .setArtifactSha256(retained.reference().getDescriptorSha256())))
                .build();
    }
}
