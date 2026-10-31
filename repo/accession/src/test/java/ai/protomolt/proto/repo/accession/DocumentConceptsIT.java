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
import com.google.protobuf.DescriptorProtos.MessageOptions;
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
    /** A bound field whose name alone passes the catalog's 1024-byte field path bound. */
    private static final String LONG_NAME = "long_" + "x".repeat(1030);
    private static final FileDescriptor FILE = file();
    static final Descriptor PAPER = FILE.findMessageTypeByName("Paper");
    static final Descriptor SECTION = FILE.findMessageTypeByName("Section");
    static final Descriptor TAGGED = FILE.findMessageTypeByName("Tagged");
    static final Descriptor EXTRAS_ENTRY = PAPER.findNestedTypeByName("ExtrasEntry");
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

    @Test void aConceptFilterMatchesOnlyTheListedAccount() throws Exception {
        var source = new Source();
        mountAll(source);
        try (var f = new AccessionFixture(); var config = DistributedConfig.over(source)) {
            var mounts = follow(config);
            var saves = saves(f, mounts);
            var physics = paper(p -> p.setField(PAPER.findFieldByName("subject"), ref("subject", "physics")));
            for (var account : List.of("a", "b"))
                for (var doc : List.of(account + "-1", account + "-2"))
                    saves.save(processAuthority(), save(doc, address(account, doc), Document.newBuilder().setDocId(doc)
                            .setOwnership(ownership(account, "reader-" + account))
                            .setSearchMetadata(SearchMetadata.newBuilder().setTitle(doc))
                            .setStructuredData(Any.newBuilder().setTypeUrl(PAPER_URL).setValue(physics.toByteString()))
                            .build()));
            var lists = new DocumentLists(f.catalog, mounts);
            var filter = filter("subject", "physics", ConceptExpansion.getDefaultInstance());
            // The same stored pair in another account never matches.
            assertThat(list(lists, filter, 1).docs()).containsExactly("a-1", "a-2");
            var readerOfA = writer("a", "reader-a", java.util.UUID.randomUUID(), 1);
            assertThat(lists.list(readerOfA, request(filter, 10).build()).getDocumentsList())
                    .extracting(summary -> summary.getAddress().getDocId()).containsExactly("a-1", "a-2");
            DocumentDeletesIT.assertCode(Status.Code.PERMISSION_DENIED, () -> lists.list(readerOfA,
                    request(filter, 10).setAccountId("b").build()));
            assertThat(lists.list(processAuthority(), request(filter, 10).setAccountId("b").build()).getDocumentsList())
                    .extracting(summary -> summary.getAddress().getDocId()).containsExactly("b-1", "b-2");
        }
    }

    @Test void anExpansionOverTheMountBoundIsRefusedNotTruncated() throws Exception {
        var source = new Source();
        mountAll(source);
        try (var f = new AccessionFixture(); var config = DistributedConfig.over(source)) {
            var mounts = ConceptSchemeMounts.follow(config, List.of("subject", "lcsh", "wiki", "region"), 3);
            assertThat(config.refresh().refused()).isEmpty();
            var saves = saves(f, mounts);
            saveTagged(saves, "d1", p -> p.setField(PAPER.findFieldByName("subject"), ref("subject", "physics")));
            var lists = new DocumentLists(f.catalog, mounts);
            // physics: itself, its lcsh exact match and old-physics, exactly the bound.
            assertThat(list(lists, filter("subject", "physics", ConceptExpansion.getDefaultInstance()), 10).docs())
                    .containsExactly("d1");
            // With narrower, quantum makes four.
            DocumentDeletesIT.assertCode(Status.Code.INVALID_ARGUMENT, () -> lists.list(processAuthority(),
                    request(filter("subject", "physics", ConceptExpansion.newBuilder().setNarrower(true).build()), 10)
                            .build()));
        }
    }

    @Test void aSchemaRedefiningConceptRefIsRefusedAsContentNotInternal() throws Exception {
        // A retained schema is self-contained, so it can carry its own concepts.proto.
        var withoutRules = forged(FieldDescriptorProto.Type.TYPE_STRING);
        var intScheme = forged(FieldDescriptorProto.Type.TYPE_INT64);
        var source = new Source();
        mountAll(source);
        try (var f = new AccessionFixture(); var config = DistributedConfig.over(source)) {
            var base = resolver();
            var noRules = definition(withoutRules, "type.googleapis.com/forged.v1.Paper");
            var badType = definition(intScheme, "type.googleapis.com/forged.v1.IntPaper");
            DocumentSchemaAdmission.Resolver forgedSchemas = selection -> switch (selection.typeUrl()) {
                case "type.googleapis.com/forged.v1.Paper" -> noRules;
                case "type.googleapis.com/forged.v1.IntPaper" -> badType;
                default -> base.select(selection);
            };
            var saves = new DocumentSaves(f.catalog, f.drives, forgedSchemas, limits(), bytes -> () -> {}, Clock.systemUTC(),
                    DocumentSchemas.compiled(), false, follow(config));
            // A set ConceptRef with empty ids: the forged type has no required rules to catch them.
            var emptyRef = DynamicMessage.getDefaultInstance(withoutRules.findFieldByName("subject").getMessageType());
            var empty = SemanticPartsIT.rejected(() -> saves.save(processAuthority(), save("forged-empty",
                    address(ACCOUNT, "forged-empty"), forgedDocument("forged-empty", "type.googleapis.com/forged.v1.Paper",
                            DynamicMessage.newBuilder(withoutRules).setField(withoutRules.findFieldByName("subject"), emptyRef)
                                    .build()))),
                    Status.Code.INVALID_ARGUMENT, RejectionReason.REJECTION_REASON_VALIDATION_FAILED);
            assertThat(empty.getMessage()).contains("forged.v1.Paper.subject").contains("required");
            // Ids that are not strings never reach a cast.
            var intRef = intScheme.findFieldByName("subject").getMessageType();
            var wrongType = SemanticPartsIT.rejected(() -> saves.save(processAuthority(), save("forged-int",
                    address(ACCOUNT, "forged-int"), forgedDocument("forged-int", "type.googleapis.com/forged.v1.IntPaper",
                            DynamicMessage.newBuilder(intScheme).setField(intScheme.findFieldByName("subject"),
                                    DynamicMessage.newBuilder(intRef).setField(intRef.findFieldByName("scheme_id"), 7L)
                                            .setField(intRef.findFieldByName("concept_id"), "physics").build()).build()))),
                    Status.Code.INVALID_ARGUMENT, RejectionReason.REJECTION_REASON_VALIDATION_FAILED);
            assertThat(wrongType.getViolationsList()).extracting(FieldViolation::getRuleId).containsExactly("concept.shape");
            assertThat(f.catalog.current(address(ACCOUNT, "forged-empty"))).isEmpty();
            assertThat(f.catalog.current(address(ACCOUNT, "forged-int"))).isEmpty();
        }
    }

    @Test void anAnyHoldingAnAnyIsUnwrappedForItsTags() throws Exception {
        var source = new Source();
        mountAll(source);
        try (var f = new AccessionFixture(); var config = DistributedConfig.over(source)) {
            var mounts = follow(config);
            var saves = saves(f, mounts);
            var nested = saveTagged(saves, "wrapped", p -> p.setField(PAPER.findFieldByName("inner"), wrapped("physics")));
            assertThat(f.catalog.concepts(nested)).containsExactly(
                    new CatalogRecords.ConceptTag("structured_data.inner.subject", 0, "subject", "physics"));
            assertThat(list(new DocumentLists(f.catalog, mounts), filter("subject", "physics",
                    ConceptExpansion.getDefaultInstance()), 10).docs()).containsExactly("wrapped");
            // The doubly wrapped tag is held to membership like any other.
            var biology = SemanticPartsIT.rejected(() -> saves.save(processAuthority(), save("wrapped-bad",
                    address(ACCOUNT, "wrapped-bad"), document("wrapped-bad", paper(p -> p.setField(
                            PAPER.findFieldByName("inner"), wrapped("biology")))))),
                    Status.Code.INVALID_ARGUMENT, RejectionReason.REJECTION_REASON_VALIDATION_FAILED);
            assertThat(biology.getViolationsList()).extracting(FieldViolation::getRuleId).containsExactly("concept.member");
            assertThat(f.catalog.current(address(ACCOUNT, "wrapped-bad"))).isEmpty();
        }
    }

    @Test void anOpaqueAnyIsNotReadThroughATypedSiblingWithTheSameBytes() throws Exception {
        var source = new Source();
        mountAll(source);
        try (var f = new AccessionFixture(); var config = DistributedConfig.over(source)) {
            var mounts = follow(config);
            var inner = tagged("physics");
            Consumer<DynamicMessage.Builder> both = p -> {
                p.setField(PAPER.findFieldByName("inner"), inner);
                p.addRepeatedField(PAPER.findFieldByName("others"), inner);
            };
            // Typed throughout, both siblings are entered.
            var typed = saveTagged(saves(f, mounts), "typed", both);
            assertThat(f.catalog.concepts(typed)).extracting(CatalogRecords.ConceptTag::fieldPath)
                    .containsExactly("structured_data.inner.subject", "structured_data.others[0].subject");
            // The attestation names inner's occurrence steps without the Any's own boundary.
            var nested = f.catalog.manifest(typed, 1).orElseThrow().getValidation().getResolvedOccurrencesList().stream()
                    .map(ResolvedOccurrence::getOccurrence)
                    .filter(locator -> locator.getStepsCount() > 1
                            && locator.getSteps(locator.getStepsCount() - 2).getFieldNumber() == 6)
                    .map(locator -> locator.toBuilder().removeSteps(locator.getStepsCount() - 1).build())
                    .findFirst().orElseThrow();

            // The same bytes with inner attested opaque: only the typed others[0] is read.
            var credential = java.util.UUID.randomUUID();
            f.catalog.registerCredential(ISSUER, credential, "chunker");
            f.opaqueStepGrant(ACCOUNT, credential, "chunker", true, "grant-producer", TAGGED_URL);
            var opaqueSaves = new DocumentSaves(f.catalog, f.drives, resolver(), limits(), bytes -> () -> {},
                    Clock.systemUTC(), DocumentSchemas.compiled(), true, mounts);
            var mixed = address(ACCOUNT, "mixed");
            var document = Document.newBuilder().setDocId("mixed").setOwnership(ownership(ACCOUNT, "chunker"))
                    .setSearchMetadata(SearchMetadata.newBuilder().setTitle("mixed"))
                    .setStructuredData(Any.newBuilder().setTypeUrl(PAPER_URL).setValue(paper(both).toByteString())).build();
            var saved = opaqueSaves.save(writer(ACCOUNT, "chunker", credential, 1), save("mixed", mixed, document).toBuilder()
                    .setOwner(stepOwner("chunker")).setValidationMode(ValidationMode.VALIDATION_MODE_OPAQUE_ALLOWED)
                    .addOpaqueAttestations(OpaqueAttestation.newBuilder().setOccurrence(nested).setTypeUrl(TAGGED_URL))
                    .build());
            assertThat(saved.getAccession().getValidation().getOpaqueOccurrencesList()).singleElement()
                    .satisfies(opaque -> assertThat(opaque.getOccurrence()).isEqualTo(nested));
            var expected = List.of(new CatalogRecords.ConceptTag("structured_data.others[0].subject", 0, "subject", "physics"));
            assertThat(f.catalog.concepts(mixed)).isEqualTo(expected);
            assertThat(list(new DocumentLists(f.catalog, mounts), filter("subject", "physics",
                    ConceptExpansion.getDefaultInstance()), 10).docs()).containsExactly("mixed", "typed");
            // A rebuild reads the stored occurrences the same way.
            execute(f, "DELETE FROM catalog_document_concepts");
            new CatalogEntryRebuild(f.catalog, f.reads).rebuild(ACCOUNT);
            assertThat(f.catalog.concepts(mixed)).isEqualTo(expected);
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
            // A typed nested Any is validated with the same mounts and projected without a segment of its own.
            var biology = SemanticPartsIT.rejected(() -> mounted.save(processAuthority(), save("nested-bad",
                    address(ACCOUNT, "nested-bad"), document("nested-bad", paper(p -> p.setField(PAPER.findFieldByName("inner"),
                            tagged("biology")))))),
                    Status.Code.INVALID_ARGUMENT, RejectionReason.REJECTION_REASON_VALIDATION_FAILED);
            assertThat(biology.getViolationsList()).extracting(FieldViolation::getRuleId).containsExactly("concept.member");
            var nested = saveTagged(mounted, "nested", p -> p.setField(PAPER.findFieldByName("inner"), tagged("physics")));
            assertThat(f.catalog.concepts(nested)).containsExactly(
                    new CatalogRecords.ConceptTag("structured_data.inner.subject", 0, "subject", "physics"));
            // A map value may not reach a tag, even through a typed Any.
            var keyed = SemanticPartsIT.rejected(() -> mounted.save(processAuthority(), save("keyed",
                    address(ACCOUNT, "keyed"), document("keyed", paper(p -> p.addRepeatedField(PAPER.findFieldByName("extras"),
                            DynamicMessage.newBuilder(EXTRAS_ENTRY).setField(EXTRAS_ENTRY.findFieldByName("key"), "k")
                                    .setField(EXTRAS_ENTRY.findFieldByName("value"), tagged("physics")).build()))))),
                    Status.Code.INVALID_ARGUMENT, RejectionReason.REJECTION_REASON_VALIDATION_FAILED);
            assertThat(keyed.getMessage()).contains("extras").contains("map value");
            assertThat(f.catalog.current(address(ACCOUNT, "keyed"))).isEmpty();
            // A tag whose field path the catalog index cannot hold is refused by name, before anything is written.
            var unstorable = SemanticPartsIT.rejected(() -> mounted.save(processAuthority(), save("long",
                    address(ACCOUNT, "long"), document("long", paper(p -> p.setField(PAPER.findFieldByName(LONG_NAME),
                            ref("subject", "physics")))))),
                    Status.Code.INVALID_ARGUMENT, RejectionReason.REJECTION_REASON_VALIDATION_FAILED);
            assertThat(unstorable.getViolationsList()).extracting(FieldViolation::getFieldPath, FieldViolation::getRuleId)
                    .containsExactly(tuple("structured_data." + LONG_NAME, "concept-catalog-storable"));
            assertThat(f.catalog.current(address(ACCOUNT, "long"))).isEmpty();
            assertThat(f.catalog.current(address(ACCOUNT, "bad"))).isEmpty();
            assertThat(f.catalog.current(address(ACCOUNT, "cold"))).isEmpty();
        }
    }

    @Test void aTagInParsedIsValidatedAndStoredButNotCatalogued() throws Exception {
        var source = new Source();
        mountAll(source);
        try (var f = new AccessionFixture(); var config = DistributedConfig.over(source)) {
            var mounts = follow(config);
            var saves = saves(f, mounts);
            var doc = saveTagged(saves, "parsed", p -> p.setField(PAPER.findFieldByName("subject"), ref("subject", "physics")));
            var core = f.catalog.concepts(doc);

            // A parser's output holding a bound tag saves; the tag is held to the mounts like any other.
            saves.save(processAuthority(), parsedWith(doc, 1, "chemistry"));
            assertThat(f.catalog.current(doc).orElseThrow().revision()).isEqualTo(2);
            var biology = SemanticPartsIT.rejected(() -> saves.save(processAuthority(), parsedWith(doc, 2, "biology")),
                    Status.Code.INVALID_ARGUMENT, RejectionReason.REJECTION_REASON_VALIDATION_FAILED);
            assertThat(biology.getViolationsList())
                    .extracting(v -> v.getPart().getPart(), FieldViolation::getFieldPath, FieldViolation::getRuleId)
                    .containsExactly(tuple(
                            DocumentPart.DOCUMENT_PART_PARSED,
                            "parser_results[\"tagger\"].document.shape.subject",
                            "concept.member"));
            assertThat(f.catalog.current(doc).orElseThrow().revision()).isEqualTo(2);

            // Only CORE's tags are catalogued: the PARSED tag is stored with PARSED and filters do not see it.
            assertThat(f.catalog.concepts(doc)).isEqualTo(core);
            var lists = new DocumentLists(f.catalog, mounts);
            assertThat(list(lists, filter("subject", "chemistry", ConceptExpansion.getDefaultInstance()), 10).docs()).isEmpty();
            assertThat(list(lists, filter("subject", "physics", ConceptExpansion.getDefaultInstance()), 10).docs())
                    .containsExactly("parsed");
            var read = f.reads.getDocument(processAuthority(), GetDocumentRequest.newBuilder().setAddress(doc)
                    .setSelection(PartSelection.newBuilder().addSelectors(PartSelector.newBuilder()
                            .setPart(DocumentPart.DOCUMENT_PART_PARSED))).build());
            var shape = read.getDocument().getParserResultsOrThrow("tagger").getDocument().getShape();
            var stored = (Message) DynamicMessage.parseFrom(PAPER, shape.getValue()).getField(PAPER.findFieldByName("subject"));
            assertThat(ConceptRef.parseFrom(stored.toByteString())).isEqualTo(cref("subject", "chemistry"));

            // A rebuild reads the stored PARSED tag the same way: it stays out of the catalog.
            execute(f, "DELETE FROM catalog_document_concepts");
            new CatalogEntryRebuild(f.catalog, f.reads).rebuild(ACCOUNT);
            assertThat(f.catalog.concepts(doc)).isEqualTo(core);
        }
    }

    @Test void aTagInParsedAboveTheCatalogPathBoundSavesWhileCoreIsRefused() throws Exception {
        var source = new Source();
        mountAll(source);
        try (var f = new AccessionFixture(); var config = DistributedConfig.over(source)) {
            var mounts = follow(config);
            var saves = saves(f, mounts);

            // In CORE, a tag whose field path exceeds 1024 UTF-8 bytes is refused with concept-catalog-storable.
            var unstorableCore = SemanticPartsIT.rejected(
                    () -> saves.save(processAuthority(), save("long-core", address(ACCOUNT, "long-core"),
                            document("long-core", paper(p -> p.setField(PAPER.findFieldByName(LONG_NAME),
                                    ref("subject", "physics")))))),
                    Status.Code.INVALID_ARGUMENT, RejectionReason.REJECTION_REASON_VALIDATION_FAILED);
            assertThat(unstorableCore.getViolationsList())
                    .extracting(v -> v.getPart().getPart(), FieldViolation::getFieldPath, FieldViolation::getRuleId)
                    .containsExactly(tuple(
                            DocumentPart.DOCUMENT_PART_CORE,
                            "structured_data." + LONG_NAME,
                            "concept-catalog-storable"));
            assertThat(f.catalog.current(address(ACCOUNT, "long-core"))).isEmpty();

            // In PARSED, tags are not catalogued, so the same > 1024-byte field path commits cleanly.
            var doc = saveTagged(saves, "long-parsed", p -> p.setField(PAPER.findFieldByName("subject"), ref("subject", "physics")));
            var core = f.catalog.concepts(doc);
            var shape = Any.newBuilder()
                    .setTypeUrl(PAPER_URL)
                    .setValue(paper(p -> p.setField(PAPER.findFieldByName(LONG_NAME), ref("subject", "chemistry"))).toByteString())
                    .build();
            var parsed = Document.newBuilder().putParserResults("tagger", ai.protomolt.proto.repo.v1.ParserResult.newBuilder()
                    .setParserName("tagger").setParserVersion("1").setStatus(ai.protomolt.proto.repo.v1.ParseStatus.PARSE_STATUS_OK)
                    .setDocument(ai.protomolt.proto.repo.v1.ParserDocument.newBuilder().setShape(shape)).build()).build();
            saves.save(processAuthority(), SaveDocumentRequest.newBuilder()
                    .setOperationId("long-parsed-2")
                    .setTarget(DocumentTarget.newBuilder().setAddress(doc))
                    .setExpected(at(1))
                    .setOwner(intakeOwner())
                    .setDocument(parsed)
                    .addWrites(put(DocumentPart.DOCUMENT_PART_PARSED))
                    .build());
            assertThat(f.catalog.current(doc).orElseThrow().revision()).isEqualTo(2);
            assertThat(f.catalog.concepts(doc)).isEqualTo(core);

            var read = f.reads.getDocument(processAuthority(), GetDocumentRequest.newBuilder().setAddress(doc)
                    .setSelection(PartSelection.newBuilder().addSelectors(PartSelector.newBuilder()
                            .setPart(DocumentPart.DOCUMENT_PART_PARSED))).build());
            var storedShape = read.getDocument().getParserResultsOrThrow("tagger").getDocument().getShape();
            var stored = (Message) DynamicMessage.parseFrom(PAPER, storedShape.getValue()).getField(PAPER.findFieldByName(LONG_NAME));
            assertThat(ConceptRef.parseFrom(stored.toByteString())).isEqualTo(cref("subject", "chemistry"));
        }
    }

    /** A PARSED write whose parser output is a Paper tagged {@code concept}, on top of {@code revision}. */
    private static SaveDocumentRequest parsedWith(NodeAddress doc, long revision, String concept) {
        var shape = Any.newBuilder().setTypeUrl(PAPER_URL).setValue(paper(p -> p.setField(PAPER.findFieldByName("subject"),
                ref("subject", concept))).toByteString()).build();
        var parsed = Document.newBuilder().putParserResults("tagger", ai.protomolt.proto.repo.v1.ParserResult.newBuilder()
                .setParserName("tagger").setParserVersion("1").setStatus(ai.protomolt.proto.repo.v1.ParseStatus.PARSE_STATUS_OK)
                .setDocument(ai.protomolt.proto.repo.v1.ParserDocument.newBuilder().setShape(shape)).build()).build();
        return SaveDocumentRequest.newBuilder().setOperationId("parsed-" + concept)
                .setTarget(DocumentTarget.newBuilder().setAddress(doc)).setExpected(at(revision)).setOwner(intakeOwner())
                .setDocument(parsed).addWrites(put(DocumentPart.DOCUMENT_PART_PARSED)).build();
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

    @Test void oneSaveChecksAllConceptTagsAgainstOneMountSnapshotEvenWhenMountsRefreshMidSave() throws Exception {
        var source = new Source();
        mountAll(source);
        try (var f = new AccessionFixture(); var config = DistributedConfig.over(source)) {
            var mounts = follow(config);
            var swappedMounts = ConceptSchemeMounts.empty();
            var activeCatalog = new java.util.concurrent.atomic.AtomicReference<ai.protomolt.proto.validate.spi.ConceptSchemeCatalog>();
            var firstLookupDone = new java.util.concurrent.atomic.AtomicBoolean();
            ai.protomolt.proto.validate.spi.ConceptSchemeCatalog refreshingMidSave = schemeId -> {
                var mounted = mounts.scheme(schemeId);
                if ("subject".equals(schemeId) && firstLookupDone.compareAndSet(false, true)) {
                    // Simulate a concurrent config refresh landing right after the first
                    // ConceptRef field in the save is validated: "physics" is removed and
                    // replaced by "astrophysics" in s2-swapped.
                    source.put("subject", "s2-swapped", scheme(concept("astrophysics")));
                    config.refresh();
                }
                return mounted;
            };
            activeCatalog.set(refreshingMidSave);

            var baseResolver = resolver();
            DocumentSchemaAdmission.Resolver swappingResolver = selection -> {
                if (TAGGED_URL.equals(selection.typeUrl())) {
                    // Also swap the supplier reference mid-save while resolving the nested Any.
                    activeCatalog.set(swappedMounts);
                }
                return baseResolver.select(selection);
            };
            var saves = new DocumentSaves(
                    f.catalog,
                    f.drives,
                    swappingResolver,
                    limits(),
                    bytes -> () -> {},
                    Clock.systemUTC(),
                    DocumentSchemas.compiled(),
                    false,
                    activeCatalog::get);

            // A document whose outer Paper.subject is "physics" (in s1) and inner Tagged.subject
            // is "astrophysics" (only in s2-swapped) must NOT be admitted across two snapshots:
            // the save pins s1 on the first lookup, so "astrophysics" fails against s1.
            var mixedPayload = paper(p -> p
                    .setField(PAPER.findFieldByName("subject"), ref("subject", "physics"))
                    .setField(PAPER.findFieldByName("inner"), tagged("astrophysics")));
            DocumentDeletesIT.assertCode(
                    Status.Code.INVALID_ARGUMENT,
                    () -> saves.save(
                            processAuthority(),
                            save("mixed-snap", address(ACCOUNT, "mixed"), document("mixed", mixedPayload))));
            assertThat(f.catalog.concepts(address(ACCOUNT, "mixed"))).isEmpty();
        }
    }

    @Test void oneSaveMustNotMixDifferentSchemeSnapshotsAcrossSchemes() throws Exception {
        var source = new Source();
        source.put("subject", "s1", scheme(concept("physics")));
        source.put("region", "r1", scheme(concept("emea")));
        try (var f = new AccessionFixture(); var config = DistributedConfig.over(source)) {
            var mounts = ConceptSchemeMounts.follow(config, List.of("subject", "region"));
            assertThat(config.refresh().refused()).isEmpty();
            var subjectLookedUp = new java.util.concurrent.atomic.AtomicBoolean();
            var refreshingCatalog = new ai.protomolt.proto.validate.spi.ConceptSchemeCatalog() {
                @Override
                public Optional<Mounted> scheme(String schemeId) {
                    return mounts.scheme(schemeId);
                }

                @Override
                public ai.protomolt.proto.validate.spi.ConceptSchemeCatalog snapshot() {
                    var pinned = mounts.snapshot();
                    return schemeId -> {
                        var mounted = pinned.scheme(schemeId);
                        if ("subject".equals(schemeId) && subjectLookedUp.compareAndSet(false, true)) {
                            // Right after "subject" is checked against s1={physics}, refresh
                            // both schemes to s2={astrophysics} and r2={amer}. Because the
                            // multi-scheme snapshot was already pinned before the first scheme
                            // lookup, the subsequent lookup of "region" must still see r1={emea}.
                            source.put("subject", "s2", DocumentConceptsIT.scheme(concept("astrophysics")));
                            source.put("region", "r2", DocumentConceptsIT.scheme(concept("amer")));
                            assertThat(config.refresh().refused()).isEmpty();
                        }
                        return mounted;
                    };
                }
            };
            var saves = new DocumentSaves(
                    f.catalog,
                    f.drives,
                    resolver(),
                    limits(),
                    bytes -> () -> {},
                    Clock.systemUTC(),
                    DocumentSchemas.compiled(),
                    false,
                    () -> refreshingCatalog);
            var crossSchemePayload = paper(p -> p
                    .setField(PAPER.findFieldByName("subject"), ref("subject", "physics"))
                    .addRepeatedField(PAPER.findFieldByName("sections"), section(ref("region", "amer"))));
            DocumentDeletesIT.assertCode(
                    Status.Code.INVALID_ARGUMENT,
                    () -> saves.save(
                            processAuthority(),
                            save("cross-scheme-snap", address(ACCOUNT, "cross-scheme"),
                                    document("cross-scheme", crossSchemePayload))));
            assertThat(subjectLookedUp.get()).isTrue();
            assertThat(f.catalog.concepts(address(ACCOUNT, "cross-scheme"))).isEmpty();
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

    /** A Tagged payload packed in an Any. */
    static Any tagged(String concept) {
        var tagged = DynamicMessage.newBuilder(TAGGED).setField(TAGGED.findFieldByName("subject"), ref("subject", concept)).build();
        return Any.newBuilder().setTypeUrl(TAGGED_URL).setValue(tagged.toByteString()).build();
    }

    /** A Tagged payload packed in an Any that is itself packed in an Any. */
    static Any wrapped(String concept) {
        return Any.newBuilder().setTypeUrl(DocumentLinksIT.ANY_URL).setValue(tagged(concept).toByteString()).build();
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
 * Paper.inner an Any, Paper.extras a map of Any and Paper.others a repeated Any;
 * Tagged binds its subject.
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
                        .addField(message("inner", 6, ".google.protobuf.Any", false))
                        .addField(message(LONG_NAME, 7, REF, false).setOptions(subject))
                        .addField(message("extras", 8, ".test.concepts.v1.Paper.ExtrasEntry", true))
                        .addField(message("others", 9, ".google.protobuf.Any", true))
                        .addNestedType(DescriptorProto.newBuilder().setName("ExtrasEntry")
                                .setOptions(MessageOptions.newBuilder().setMapEntry(true))
                                .addField(FieldDescriptorProto.newBuilder().setName("key").setNumber(1)
                                        .setType(FieldDescriptorProto.Type.TYPE_STRING).setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                                .addField(message("value", 2, ".google.protobuf.Any", false))))
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

    private static Document forgedDocument(String doc, String typeUrl, Message payload) {
        return Document.newBuilder().setDocId(doc).setOwnership(ownership(ACCOUNT))
                .setSearchMetadata(SearchMetadata.newBuilder().setTitle(doc))
                .setStructuredData(Any.newBuilder().setTypeUrl(typeUrl).setValue(payload.toByteString())).build();
    }

    /**
     * A payload type whose bound subject field uses a ConceptRef from a forged
     * ai/protomolt/proto/types/v1/concepts.proto: no field rules, and scheme_id of {@code schemeType}.
     */
    private static Descriptor forged(FieldDescriptorProto.Type schemeType) {
        var concepts = FileDescriptorProto.newBuilder().setName(ConceptsProto.getDescriptor().getName())
                .setPackage("ai.protomolt.proto.types.v1").setSyntax("proto3")
                .addMessageType(DescriptorProto.newBuilder().setName("ConceptRef")
                        .addField(FieldDescriptorProto.newBuilder().setName("scheme_id").setNumber(1).setType(schemeType)
                                .setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                        .addField(FieldDescriptorProto.newBuilder().setName("concept_id").setNumber(2)
                                .setType(FieldDescriptorProto.Type.TYPE_STRING).setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL)))
                .build();
        var name = schemeType == FieldDescriptorProto.Type.TYPE_STRING ? "Paper" : "IntPaper";
        var subject = FieldOptions.newBuilder().setExtension(ValidateProto.field,
                FieldRules.newBuilder().addConceptSchemes("subject").build()).build();
        var paper = FileDescriptorProto.newBuilder().setName("forged/v1/" + name.toLowerCase(java.util.Locale.ROOT) + ".proto")
                .setPackage("forged.v1").setSyntax("proto3")
                .addDependency(concepts.getName()).addDependency(ValidateProto.getDescriptor().getName())
                .addMessageType(DescriptorProto.newBuilder().setName(name)
                        .addField(message("subject", 1, REF, false).setOptions(subject)))
                .build();
        try {
            var forgedConcepts = FileDescriptor.buildFrom(concepts, new FileDescriptor[0]);
            return FileDescriptor.buildFrom(paper, new FileDescriptor[]{forgedConcepts, ValidateProto.getDescriptor()})
                    .findMessageTypeByName(name);
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
