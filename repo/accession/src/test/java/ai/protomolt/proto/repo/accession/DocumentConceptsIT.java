package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;

import ai.protomolt.proto.config.ConceptSchemeMounts;
import ai.protomolt.proto.config.ConfigSource;
import ai.protomolt.proto.config.DistributedConfig;
import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.admission.DocumentSchemas;
import ai.protomolt.proto.repo.catalog.CatalogRecords;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.PublicationSchemaCondition;
import ai.protomolt.proto.repo.v1.RepositorySchemaAsset;
import ai.protomolt.proto.repo.v1.SchemaCompilationOrigin;
import ai.protomolt.proto.repo.v1.SchemaCompilationProvenance;
import ai.protomolt.proto.repo.v1.SchemaCompilerEvidence;
import ai.protomolt.proto.repo.v1.SchemaToolIdentity;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v2.*;
import ai.protomolt.proto.types.Concept;
import ai.protomolt.proto.types.ConceptExpansion;
import ai.protomolt.proto.types.ConceptRef;
import ai.protomolt.proto.types.ConceptScheme;
import ai.protomolt.proto.types.ConceptsProto;
import ai.protomolt.proto.types.LangLabel;
import ai.protomolt.proto.validate.FieldRules;
import ai.protomolt.proto.validate.ValidateProto;
import com.google.protobuf.Any;
import com.google.protobuf.AnyProto;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldOptions;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import io.grpc.Status;
import java.sql.DriverManager;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

/**
 * Concept tags over real PostgreSQL and S3 (requirements §17, "Subject tags"):
 * membership decided against the host's mounted schemes at save, the
 * catalog_document_concepts projection across saves, carried CORE, tombstones,
 * purges and rebuilds, and the DocumentFilter concept filter expanded at query
 * time in ListDocuments and ResolveLink, including after a scheme swap that
 * rewrites nothing in the catalog.
 */
class DocumentConceptsIT {
    private static final String REF = ".ai.protomolt.proto.types.v1.ConceptRef";
    private static final FileDescriptor FILE = file();
    static final Descriptor PAPER = FILE.findMessageTypeByName("Paper");
    static final Descriptor SECTION = FILE.findMessageTypeByName("Section");
    static final Descriptor TAGGED = FILE.findMessageTypeByName("Tagged");
    static final String PAPER_URL = "type.googleapis.com/test.concepts.v1.Paper";
    static final String TAGGED_URL = "type.googleapis.com/test.concepts.v1.Tagged";
    private static final String ACCOUNT = "a";

    // ------------------------------------------------------------------ projection and filter

    @Test void tagsProjectAndAConceptFilterExpandsAtQueryTime() throws Exception {
        var source = new Source();
        mountAll(source);
        try (var f = new AccessionFixture(); var config = DistributedConfig.over(source)) {
            var mounts = follow(config);
            var saves = saves(f, mounts);
            saveTagged(saves, "d1", p -> p.setField(PAPER.findFieldByName("subject"), ref("subject", "physics")));
            saveTagged(saves, "d2", p -> p.setField(PAPER.findFieldByName("subject"), ref("subject", "quantum")));
            saveTagged(saves, "d3", p -> p.addRepeatedField(PAPER.findFieldByName("sections"),
                    section(ref("region", "emea"), ref("subject", "chemistry"))));
            saveTagged(saves, "d4", p -> p.setField(PAPER.findFieldByName("subject"), ref("subject", "old-physics")));
            // An unbound ConceptRef is not a tag.
            saveTagged(saves, "d5", p -> p.setField(PAPER.findFieldByName("mapped"), ref("subject", "physics")));

            assertThat(f.catalog.concepts(address(ACCOUNT, "d3"))).containsExactly(
                    new CatalogRecords.ConceptTag("structured_data.sections[0].tags", 0, "region", "emea"),
                    new CatalogRecords.ConceptTag("structured_data.sections[0].tags", 1, "subject", "chemistry"));
            assertThat(f.catalog.concepts(address(ACCOUNT, "d1"))).containsExactly(
                    new CatalogRecords.ConceptTag("structured_data.subject", 0, "subject", "physics"));
            assertThat(f.catalog.concepts(address(ACCOUNT, "d5"))).isEmpty();

            var lists = new DocumentLists(f.catalog, mounts);
            // Default steps: the concept, its exact_match closure and its deprecated predecessors.
            var physics = list(lists, filter("subject", "physics", ConceptExpansion.getDefaultInstance()), 1);
            assertThat(physics.docs()).containsExactly("d1", "d4");
            assertThat(physics.evidence()).allSatisfy(evidence -> {
                assertThat(DocumentSaves.VALIDATOR.validate(evidence).valid()).isTrue();
                assertThat(evidence.getExpandedConcepts()).isEqualTo(3);
                assertThat(evidence.getMountedSchemesList()).extracting(MountedConceptScheme::getSchemeId,
                        MountedConceptScheme::getVersion).contains(tuple("subject", "s1"), tuple("lcsh", "l1"));
                assertThat(evidence.getUnresolvedMappingsList()).extracting(m -> m.getTarget().getSchemeId(),
                        UnresolvedConceptMapping::getRelation).containsExactly(tuple("absent", "exact_match"));
            });
            var narrower = ConceptExpansion.newBuilder().setNarrower(true).build();
            assertThat(list(lists, filter("subject", "physics", narrower), 10).docs()).containsExactly("d1", "d2", "d4");
            assertThat(list(lists, filter("subject", "science", narrower), 10).docs()).containsExactly("d1", "d2", "d3");
            assertThat(list(lists, filter("region", "emea", ConceptExpansion.getDefaultInstance()), 10).docs())
                    .containsExactly("d3");
            // A concept filter composes with the other filters.
            assertThat(list(lists, filter("subject", "physics", narrower).toBuilder().setTitle("d2").build(), 10).docs())
                    .containsExactly("d2");

            // A scheme swap changes the expansion with no catalog rewrite: quantum moves under chemistry.
            source.put("subject", "s2", subjects(true));
            config.refresh();
            var swapped = list(lists, filter("subject", "physics", narrower), 10);
            assertThat(swapped.docs()).containsExactly("d1", "d4");
            assertThat(swapped.evidence().getFirst().getMountedSchemesList()).extracting(MountedConceptScheme::getSchemeId,
                    MountedConceptScheme::getVersion).contains(tuple("subject", "s2"));
            assertThat(f.catalog.concepts(address(ACCOUNT, "d2"))).containsExactly(
                    new CatalogRecords.ConceptTag("structured_data.subject", 0, "subject", "quantum"));

            // A page token is bound to the filter, concept included.
            var first = lists.list(processAuthority(), request(filter("subject", "physics", narrower), 1).build());
            DocumentDeletesIT.assertCode(Status.Code.INVALID_ARGUMENT, () -> lists.list(processAuthority(),
                    request(filter("subject", "chemistry", narrower), 1).setPageToken(first.getNextPageToken()).build()));
            // A listing without a concept filter carries no evidence.
            assertThat(lists.list(processAuthority(), request(DocumentFilter.getDefaultInstance(), 10).build())
                    .hasConceptEvidence()).isFalse();
        }
    }

    @Test void conceptFilterRefusalsNameTheirCause() throws Exception {
        var source = new Source();
        mountAll(source);
        try (var f = new AccessionFixture(); var config = DistributedConfig.over(source)) {
            var lists = new DocumentLists(f.catalog, follow(config));
            var any = ConceptExpansion.getDefaultInstance();
            // A queried scheme the host has not mounted.
            DocumentDeletesIT.assertCode(Status.Code.FAILED_PRECONDITION, () -> lists.list(processAuthority(),
                    request(filter("department", "hr", any), 10).build()));
            // A concept the mounted scheme lacks.
            DocumentDeletesIT.assertCode(Status.Code.INVALID_ARGUMENT, () -> lists.list(processAuthority(),
                    request(filter("subject", "biology", any), 10).build()));
            // A concept id that breaks ConceptRef's own rules.
            DocumentDeletesIT.assertCode(Status.Code.INVALID_ARGUMENT, () -> lists.list(processAuthority(),
                    request(filter("Not A Slug", "x", any), 10).build()));
            // A host that mounts no schemes refuses every concept filter.
            DocumentDeletesIT.assertCode(Status.Code.FAILED_PRECONDITION, () -> new DocumentLists(f.catalog)
                    .list(processAuthority(), request(filter("subject", "physics", any), 10).build()));
        }
    }

    @Test void membershipIsDecidedAtSaveAgainstTheMounts() throws Exception {
        var source = new Source();
        mountAll(source);
        try (var f = new AccessionFixture(); var config = DistributedConfig.over(source)) {
            var mounted = saves(f, follow(config));
            var unknown = SemanticPartsIT.rejected(() -> mounted.save(processAuthority(), save("bad",
                    address(ACCOUNT, "bad"), document("bad", paper(p -> p.setField(PAPER.findFieldByName("subject"),
                            ref("subject", "biology")))))),
                    Status.Code.INVALID_ARGUMENT, RejectionReason.REJECTION_REASON_VALIDATION_FAILED);
            assertThat(unknown.getViolationsList()).extracting(FieldViolation::getRuleId).containsExactly("concept.member");
            // A host without mounts refuses a bound tag fail-closed; an untagged document still saves.
            var unmounted = SemanticPartsIT.rejected(() -> new DocumentSaves(f.catalog, f.drives, resolver(), limits(),
                    bytes -> () -> {}, Clock.systemUTC()).save(processAuthority(), save("cold",
                    address(ACCOUNT, "cold"), document("cold", paper(p -> p.setField(PAPER.findFieldByName("subject"),
                            ref("subject", "physics")))))),
                    Status.Code.INVALID_ARGUMENT, RejectionReason.REJECTION_REASON_VALIDATION_FAILED);
            assertThat(unmounted.getViolationsList()).extracting(FieldViolation::getRuleId).containsExactly("concept.unmounted");
            // A type held by a nested Any is past the boundary the tag walk reads, so declaring tags there is refused.
            var tagged = DynamicMessage.newBuilder(TAGGED).setField(TAGGED.findFieldByName("subject"), ref("subject", "physics")).build();
            var nested = SemanticPartsIT.rejected(() -> mounted.save(processAuthority(), save("nested",
                    address(ACCOUNT, "nested"), document("nested", paper(p -> p.setField(PAPER.findFieldByName("inner"),
                            Any.newBuilder().setTypeUrl(TAGGED_URL).setValue(tagged.toByteString()).build()))))),
                    Status.Code.INVALID_ARGUMENT, RejectionReason.REJECTION_REASON_VALIDATION_FAILED);
            assertThat(nested.getMessage()).contains(TAGGED_URL).contains("nested Any");
            assertThat(f.catalog.current(address(ACCOUNT, "bad"))).isEmpty();
            assertThat(f.catalog.current(address(ACCOUNT, "cold"))).isEmpty();
        }
    }

    // ------------------------------------------------------------------ lifecycle

    @Test void rowsFollowTheCurrentRevisionAndRebuild() throws Exception {
        var source = new Source();
        mountAll(source);
        try (var f = new AccessionFixture(); var config = DistributedConfig.over(source)) {
            var mounts = follow(config);
            var saves = saves(f, mounts);
            var d1 = saveTagged(saves, "d1", p -> p.setField(PAPER.findFieldByName("subject"), ref("subject", "physics")));
            var d2 = saveTagged(saves, "d2", p -> p.addRepeatedField(PAPER.findFieldByName("sections"),
                    section(ref("subject", "chemistry"), ref("region", "apac"))));
            var d3 = saveTagged(saves, "d3", p -> p.setField(PAPER.findFieldByName("subject"), ref("subject", "quantum")));

            // A carried CORE keeps its rows; a written CORE replaces them.
            var before = f.catalog.concepts(d2);
            saves.save(processAuthority(), SaveDocumentRequest.newBuilder().setOperationId("carry")
                    .setTarget(DocumentTarget.newBuilder().setAddress(d2)).setExpected(at(1)).setOwner(intakeOwner())
                    .addWrites(PartWrite.newBuilder().setSelector(PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_PARSED))
                            .setAction(WriteAction.WRITE_ACTION_EMPTY)).build());
            assertThat(f.catalog.current(d2).orElseThrow().revision()).isEqualTo(2);
            assertThat(f.catalog.concepts(d2)).isEqualTo(before);
            saves.save(processAuthority(), save("d2-3", d2, document("d2", paper(p -> p.setField(
                    PAPER.findFieldByName("subject"), ref("subject", "science"))))).toBuilder().setExpected(at(2)).build());
            assertThat(f.catalog.concepts(d2)).containsExactly(
                    new CatalogRecords.ConceptTag("structured_data.subject", 0, "subject", "science"));

            // Rebuild reads stored CORE without deciding membership again: a host with no mounts rebuilds it.
            List<CatalogRecords.ConceptTag> d1Rows = f.catalog.concepts(d1);
            List<CatalogRecords.ConceptTag> d2Rows = f.catalog.concepts(d2);
            List<CatalogRecords.ConceptTag> d3Rows = f.catalog.concepts(d3);
            execute(f, "DELETE FROM catalog_document_concepts");
            assertThat(f.catalog.concepts(d1)).isEmpty();
            assertThat(new CatalogEntryRebuild(f.catalog, f.reads).rebuild(ACCOUNT)).isEqualTo(3);
            assertThat(f.catalog.concepts(d1)).isEqualTo(d1Rows);
            assertThat(f.catalog.concepts(d2)).isEqualTo(d2Rows);
            assertThat(f.catalog.concepts(d3)).isEqualTo(d3Rows);
            // The same reads serve a tagged revision.
            assertThat(DocumentLinksIT.linkReads(f).listLinks(processAuthority(), ListLinksRequest.newBuilder()
                    .setAddress(d1).build()).getRevision()).isEqualTo(1);

            // A tombstone clears the rows; tombstoned documents never match, even when listed.
            new DocumentDeletes(f.catalog, Clock.systemUTC()).delete(processAuthority(), DeleteDocumentRequest.newBuilder()
                    .setAddress(d1).setExpected(at(1)).setOperationId("delete-d1").build());
            assertThat(f.catalog.concepts(d1)).isEmpty();
            var lists = new DocumentLists(f.catalog, mounts);
            assertThat(lists.list(processAuthority(), request(filter("subject", "physics", ConceptExpansion.getDefaultInstance()), 10)
                    .setIncludeTombstoned(true).build()).getDocumentsList()).isEmpty();

            // A purge clears them too.
            assertThat(count(f, "SELECT count(*) FROM catalog_document_concepts c JOIN catalog_documents d"
                    + " ON d.document_id = c.document_id WHERE d.doc_id = 'd3'")).isEqualTo(1);
            DocumentLinksIT.purges(f).purge(DocumentPurgesIT.administrator(), PurgeDocumentRequest.newBuilder()
                    .setPurgeId("purge-d3").setAddress(d3).build());
            assertThat(count(f, "SELECT count(*) FROM catalog_document_concepts c JOIN catalog_documents d"
                    + " ON d.document_id = c.document_id WHERE d.doc_id = 'd3'")).isZero();
        }
    }

    @Test void aDynamicLinkResolvesItsConceptFilterWhenRead() throws Exception {
        var source = new Source();
        mountAll(source);
        try (var f = new AccessionFixture(); var config = DistributedConfig.over(source)) {
            var mounts = follow(config);
            var saves = saves(f, mounts);
            saveTagged(saves, "d1", p -> p.setField(PAPER.findFieldByName("subject"), ref("subject", "physics")));
            saveTagged(saves, "d2", p -> p.setField(PAPER.findFieldByName("subject"), ref("subject", "quantum")));
            saveTagged(saves, "d3", p -> p.setField(PAPER.findFieldByName("subject"), ref("subject", "chemistry")));
            var selector = DynamicTarget.newBuilder().setFilter(filter("subject", "physics",
                    ConceptExpansion.newBuilder().setNarrower(true).build())).build();
            var src = saveTagged(saves, "src", p -> p.setField(PAPER.findFieldByName("related"),
                    DocumentLinksIT.link(DocumentLink.newBuilder().setDynamicTarget(selector).build())));
            var reads = new DocumentLinkReads(f.reads, new DocumentLists(f.catalog, mounts));
            var found = new ArrayList<String>();
            var request = ResolveLinkRequest.newBuilder().setAddress(src).setFieldPath("structured_data.related").setPageSize(1);
            do {
                var page = reads.resolveLink(processAuthority(), request.build());
                page.getDocumentsList().forEach(summary -> found.add(summary.getAddress().getDocId()));
                assertThat(page.getConceptEvidence().getMountedSchemesList()).extracting(MountedConceptScheme::getSchemeId)
                        .contains("subject");
                request.setPageToken(page.getNextPageToken());
            } while (!request.getPageToken().isEmpty());
            assertThat(found).containsExactly("d1", "d2");
        }
    }

    // ------------------------------------------------------------------ fixtures

    /** A config source over a map, one subject per scheme. */
    static final class Source implements ConfigSource {
        private final Map<String, Fetched> documents = new HashMap<>();

        @Override public synchronized Optional<Fetched> fetch(String subject) {
            return Optional.ofNullable(documents.get(subject));
        }

        synchronized void put(String schemeId, String version, ConceptScheme scheme) {
            documents.put(ConceptSchemeMounts.SUBJECT_PREFIX + schemeId, new Fetched(version, scheme.toByteArray()));
        }
    }

    private static void mountAll(Source source) {
        source.put("subject", "s1", subjects(false));
        source.put("lcsh", "l1", scheme(concept("sh-physics")));
        source.put("wiki", "w1", scheme(concept("physics")));
        source.put("region", "r1", scheme(concept("emea"), concept("apac")));
    }

    private static ConceptSchemeMounts follow(DistributedConfig config) {
        var mounts = ConceptSchemeMounts.follow(config, List.of("subject", "lcsh", "wiki", "region"));
        assertThat(config.refresh().refused()).isEmpty();
        return mounts;
    }

    /** subject: physics maps to lcsh and to an unmounted scheme; quantum sits under physics, or under chemistry when moved. */
    private static ConceptScheme subjects(boolean quantumMoved) {
        return scheme(
                concept("science"),
                concept("physics").addBroader("science").addExactMatch(cref("lcsh", "sh-physics"))
                        .addExactMatch(cref("absent", "x")).addCloseMatch(cref("wiki", "physics")),
                concept("chemistry").addBroader("science"),
                concept("quantum").addBroader(quantumMoved ? "chemistry" : "physics"),
                concept("old-physics").setDeprecated(true).addReplacedBy("physics"));
    }

    private static Concept.Builder concept(String id) {
        return Concept.newBuilder().setId(id).addPrefLabel(LangLabel.newBuilder().setLanguage("en").setText(id));
    }

    private static ConceptScheme scheme(Concept.Builder... concepts) {
        var builder = ConceptScheme.newBuilder();
        for (var concept : concepts) builder.addConcepts(concept);
        return builder.build();
    }

    private static ConceptRef cref(String scheme, String concept) {
        return ConceptRef.newBuilder().setSchemeId(scheme).setConceptId(concept).build();
    }

    private static Message ref(String scheme, String concept) {
        return cref(scheme, concept);
    }

    static DocumentFilter filter(String scheme, String concept, ConceptExpansion expansion) {
        return DocumentFilter.newBuilder().setConcept(ConceptFilter.newBuilder().setConcept(cref(scheme, concept))
                .setExpansion(expansion)).build();
    }

    private static ListDocumentsRequest.Builder request(DocumentFilter filter, int pageSize) {
        return ListDocumentsRequest.newBuilder().setAccountId(ACCOUNT).setPageSize(pageSize).setFilter(filter);
    }

    private record Listed(List<String> docs, List<ConceptFilterEvidence> evidence) {}

    /** Every page of a concept-filtered listing. */
    private static Listed list(DocumentLists lists, DocumentFilter filter, int pageSize) {
        var docs = new ArrayList<String>();
        var evidence = new ArrayList<ConceptFilterEvidence>();
        var request = request(filter, pageSize);
        do {
            var page = lists.list(processAuthority(), request.build());
            assertThat(DocumentSaves.VALIDATOR.validate(page).valid()).isTrue();
            page.getDocumentsList().forEach(summary -> docs.add(summary.getAddress().getDocId()));
            evidence.add(page.getConceptEvidence());
            request.setPageToken(page.getNextPageToken());
        } while (!request.getPageToken().isEmpty());
        return new Listed(docs, evidence);
    }

    static DocumentSaves saves(AccessionFixture f, ConceptSchemeMounts mounts) {
        return new DocumentSaves(f.catalog, f.drives, resolver(), limits(), bytes -> () -> {}, Clock.systemUTC(),
                DocumentSchemas.compiled(), false, mounts);
    }

    private static NodeAddress saveTagged(DocumentSaves saves, String doc, Consumer<DynamicMessage.Builder> fill) {
        var address = address(ACCOUNT, doc);
        saves.save(processAuthority(), save(doc, address, document(doc, paper(fill))));
        return address;
    }

    static Message paper(Consumer<DynamicMessage.Builder> fill) {
        var builder = DynamicMessage.newBuilder(PAPER).setField(PAPER.findFieldByName("title"), "paper");
        fill.accept(builder);
        return builder.build();
    }

    static Message section(Message... tags) {
        var builder = DynamicMessage.newBuilder(SECTION);
        for (var tag : tags) builder.addRepeatedField(SECTION.findFieldByName("tags"), tag);
        return builder.build();
    }

    static Document document(String doc, Message payload) {
        return Document.newBuilder().setDocId(doc).setOwnership(ownership(ACCOUNT))
                .setSearchMetadata(SearchMetadata.newBuilder().setTitle(doc))
                .setStructuredData(Any.newBuilder().setTypeUrl(PAPER_URL).setValue(payload.toByteString())).build();
    }

    static SaveDocumentRequest save(String operation, NodeAddress address, Document document) {
        return SaveDocumentRequest.newBuilder().setOperationId(operation).setTarget(DocumentTarget.newBuilder().setAddress(address))
                .setExpected(ExpectedRevision.newBuilder().setNone(true)).setDocument(document)
                .addWrites(put(DocumentPart.DOCUMENT_PART_CORE)).setOwner(intakeOwner()).build();
    }

    /** The fixture's registered types, the link test's types, and Paper. */
    static DocumentSchemaAdmission.Resolver resolver() {
        var base = DocumentLinksIT.resolver();
        var paper = definition(PAPER, PAPER_URL);
        var tagged = definition(TAGGED, TAGGED_URL);
        return selection -> PAPER_URL.equals(selection.typeUrl()) ? paper
                : TAGGED_URL.equals(selection.typeUrl()) ? tagged : base.select(selection);
    }

    private static DocumentSchemaAdmission.Definition definition(Descriptor type, String typeUrl) {
        var closure = DescriptorFingerprints.closure(type);
        var bytes = closure.toByteString();
        var metadata = RepositorySchemaAsset.newBuilder().setTypeUrl(typeUrl).setArtifactSha256(DocumentSaves.sha256(bytes))
                .setSchema(PublicationSchemaCondition.newBuilder().setTypeName(type.getFullName())
                        .setDescriptorFingerprint(DescriptorFingerprints.fingerprint(closure)))
                .setCompilation(SchemaCompilationProvenance.newBuilder()
                        .setOrigin(SchemaCompilationOrigin.SCHEMA_COMPILATION_ORIGIN_IMPORTED_DESCRIPTOR)
                        .setEvidence(SchemaCompilerEvidence.SCHEMA_COMPILER_EVIDENCE_UNKNOWN)
                        .setUnknownCompilerReason("test schema built from descriptor protos")
                        .setAdmissionRuntime(SchemaToolIdentity.newBuilder().setName("protobuf-java").setVersion("test")))
                .build();
        return new DocumentSchemaAdmission.Definition(metadata, bytes, Optional.empty());
    }

    /**
     * test.concepts.v1: Paper binds subject and Section binds tags to schemes;
     * Paper.mapped is an unbound ConceptRef, Paper.related a dynamic link and
 * Paper.inner an Any; Tagged binds its subject.
     */
    private static FileDescriptor file() {
        var subject = FieldOptions.newBuilder().setExtension(ValidateProto.field,
                FieldRules.newBuilder().addConceptSchemes("subject").build()).build();
        var tags = FieldOptions.newBuilder().setExtension(ValidateProto.field,
                FieldRules.newBuilder().addConceptSchemes("subject").addConceptSchemes("region").build()).build();
        var link = FieldOptions.newBuilder().setExtension(LinksProto.link, LinkField.newBuilder().setRelation("related").build()).build();
        var file = FileDescriptorProto.newBuilder().setName("test/concepts/v1/paper.proto").setPackage("test.concepts.v1")
                .setSyntax("proto3").addDependency(ConceptsProto.getDescriptor().getName())
                .addDependency(ValidateProto.getDescriptor().getName()).addDependency(LinksProto.getDescriptor().getName())
                .addDependency(AnyProto.getDescriptor().getName())
                .addMessageType(DescriptorProto.newBuilder().setName("Section")
                        .addField(message("tags", 1, REF, true).setOptions(tags)))
                .addMessageType(DescriptorProto.newBuilder().setName("Paper")
                        .addField(FieldDescriptorProto.newBuilder().setName("title").setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_STRING).setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                        .addField(message("subject", 2, REF, false).setOptions(subject))
                        .addField(message("sections", 3, ".test.concepts.v1.Section", true))
                        .addField(message("mapped", 4, REF, false))
                        .addField(message("related", 5, ".ai.protomolt.proto.repo.v2.DocumentLink", false).setOptions(link))
                        .addField(message("inner", 6, ".google.protobuf.Any", false)))
                .addMessageType(DescriptorProto.newBuilder().setName("Tagged")
                        .addField(message("subject", 1, REF, false).setOptions(subject)))
                .build();
        try {
            return FileDescriptor.buildFrom(file, new FileDescriptor[]{ConceptsProto.getDescriptor(),
                    ValidateProto.getDescriptor(), LinksProto.getDescriptor(), AnyProto.getDescriptor()});
        } catch (DescriptorValidationException invalid) {
            throw new IllegalStateException(invalid);
        }
    }

    private static FieldDescriptorProto.Builder message(String name, int number, String type, boolean repeated) {
        return FieldDescriptorProto.newBuilder().setName(name).setNumber(number).setTypeName(type)
                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE).setLabel(repeated
                        ? FieldDescriptorProto.Label.LABEL_REPEATED : FieldDescriptorProto.Label.LABEL_OPTIONAL);
    }

    private static void execute(AccessionFixture f, String sql) throws Exception {
        try (var connection = DriverManager.getConnection(f.url, POSTGRES.getUsername(), POSTGRES.getPassword())) {
            connection.createStatement().execute(sql);
        }
    }

    private static long count(AccessionFixture f, String sql) throws Exception {
        try (var connection = DriverManager.getConnection(f.url, POSTGRES.getUsername(), POSTGRES.getPassword());
             var result = connection.createStatement().executeQuery(sql)) {
            result.next();
            return result.getLong(1);
        }
    }
}
