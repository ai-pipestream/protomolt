package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;
import static org.assertj.core.groups.Tuple.tuple;

import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.catalog.CatalogRecords;
import ai.protomolt.proto.repo.spi.PurgeAuthorization;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v1.Principal;
import ai.protomolt.proto.repo.v1.PublicationSchemaCondition;
import ai.protomolt.proto.repo.v1.RepositorySchemaAsset;
import ai.protomolt.proto.repo.v1.RepositorySchemaOccurrenceStep;
import ai.protomolt.proto.repo.v1.SchemaCompilationOrigin;
import ai.protomolt.proto.repo.v1.SchemaCompilationProvenance;
import ai.protomolt.proto.repo.v1.SchemaCompilerEvidence;
import ai.protomolt.proto.repo.v1.SchemaToolIdentity;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v2.*;
import com.google.protobuf.Any;
import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldOptions;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Int64Value;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import io.grpc.Status;
import java.sql.DriverManager;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;

/**
 * Document links over real PostgreSQL and S3 (requirements §17): the save-time
 * target gate with recorded rejections, the catalog edge table across saves,
 * tombstones, purges and rebuilds, and the three link reads under different
 * readers' access.
 */
class DocumentLinksIT {
    private static final String LINK = ".ai.protomolt.proto.repo.v2.DocumentLink";
    /** A 260-character field name: four levels of it put a link path past the catalog's 1024-byte bound. */
    private static final String LONG_NAME = "n".repeat(260);
    private static final FileDescriptor FILE = file();
    static final Descriptor CITATION = FILE.findMessageTypeByName("Citation");
    static final Descriptor SECTION = FILE.findMessageTypeByName("Section");
    static final Descriptor UNLINKED = FILE.findMessageTypeByName("Unlinked");
    static final String CITATION_URL = "type.googleapis.com/test.links.v1.Citation";
    static final String UNLINKED_URL = "type.googleapis.com/test.links.v1.Unlinked";
    static final String ANY_URL = "type.googleapis.com/google.protobuf.Any";
    static final Descriptor DEEP = FILE.findMessageTypeByName("Deep0");
    static final String DEEP_URL = "type.googleapis.com/test.links.v1.Deep0";
    private static final String CITATION_TYPE = "test.links.v1.Citation";
    private static final String ACCOUNT = "a";

    // ------------------------------------------------------------------ static links

    @Test void staticLinksProjectReadBackAndDangle() throws Exception {
        try (var f = new AccessionFixture()) {
            var saves = saves(f);
            var t1 = address(ACCOUNT, "t1");
            var t2 = address(ACCOUNT, "t2");
            saves.save(processAuthority(), save("t1", t1, document("t1", citation(c -> {}), "reader")));
            saves.save(processAuthority(), save("t2", t2, document("t2", citation(c -> {}), "reader")));
            var src = address(ACCOUNT, "src");
            saves.save(processAuthority(), save("src", src, document("src", citation(c -> {
                c.setField(CITATION.findFieldByName("cites"), link(fixed(t1, null)));
                c.addRepeatedField(CITATION.findFieldByName("sections"), section(fixed(t2, null)));
                c.addRepeatedField(CITATION.findFieldByName("sections"), section(fixed(t1, 1L), fixed(t2, null)));
                c.setField(CITATION.findFieldByName("typed"), link(fixed(t1, null).toBuilder().setLabel("origin").build()));
            }), "reader")));

            // The edge table holds one row per static link value, keyed by its occurrence identity.
            assertThat(f.catalog.links(src)).extracting(CatalogRecords.Link::fieldPath, CatalogRecords.Link::ordinal,
                    CatalogRecords.Link::relation, link -> link.target().getDocId(), CatalogRecords.Link::pinnedRevision).containsExactly(
                    tuple("structured_data.cites", 0, "references", "t1", OptionalLong.empty()),
                    tuple("structured_data.sections[0].cites", 0, "references", "t2", OptionalLong.empty()),
                    tuple("structured_data.sections[1].cites", 0, "references", "t1", OptionalLong.of(1)),
                    tuple("structured_data.sections[1].cites", 1, "references", "t2", OptionalLong.empty()),
                    tuple("structured_data.typed", 0, "source", "t1", OptionalLong.empty()));
            var listed = f.catalog.documents(CatalogRecords.Listing.of(ACCOUNT, "", "", false), Optional.empty(), 10);
            assertThat(listed).allSatisfy(row -> assertThat(row.entry().getLookup().getStructuredDataType()).isEqualTo(CITATION_TYPE));

            var reads = linkReads(f);
            var reader = reader("reader");
            var links = reads.listLinks(reader, ListLinksRequest.newBuilder().setAddress(src).build());
            assertThat(DocumentSaves.VALIDATOR.validate(links).valid()).isTrue();
            assertThat(links.getRevision()).isEqualTo(1);
            assertThat(links.getLinksList()).extracting(ResolvedLink::getFieldPath, ResolvedLink::getOrdinal,
                    l -> l.getStaticTarget().getAddress().getDocId(), l -> l.getStaticTarget().getTargetTombstoned(),
                    l -> l.getStaticTarget().getTargetUnavailable()).containsExactly(
                    tuple("structured_data.cites", 0, "t1", false, false),
                    tuple("structured_data.sections[0].cites", 0, "t2", false, false),
                    tuple("structured_data.sections[1].cites", 0, "t1", false, false),
                    tuple("structured_data.sections[1].cites", 1, "t2", false, false),
                    tuple("structured_data.typed", 0, "t1", false, false));
            assertThat(links.getLinks(2).getStaticTarget().getPinnedRevision()).isEqualTo(1);
            assertThat(links.getLinks(4).getLabel()).isEqualTo("origin");

            // Backlinks: linking call number, field path, ordinal; paged one at a time; relation filtered.
            var backlinks = new ArrayList<Backlink>();
            var request = ListBacklinksRequest.newBuilder().setTarget(t1).setPageSize(1);
            do {
                var page = reads.listBacklinks(reader, request.build());
                backlinks.addAll(page.getBacklinksList());
                request.setPageToken(page.getNextPageToken());
            } while (!request.getPageToken().isEmpty());
            assertThat(backlinks).extracting(Backlink::getFieldPath, Backlink::getOrdinal, Backlink::getRelation).containsExactly(
                    tuple("structured_data.cites", 0, "references"), tuple("structured_data.sections[1].cites", 0, "references"),
                    tuple("structured_data.typed", 0, "source"));
            assertThat(backlinks.get(1).getPinnedRevision()).isEqualTo(1);
            assertThat(reads.listBacklinks(reader, ListBacklinksRequest.newBuilder().setTarget(t1).setRelation("source")
                    .setPageSize(10).build()).getBacklinksList()).extracting(Backlink::getFieldPath)
                    .containsExactly("structured_data.typed");
            DocumentDeletesIT.assertCode(Status.Code.INVALID_ARGUMENT, () -> reads.listBacklinks(reader, ListBacklinksRequest
                    .newBuilder().setTarget(t2).setPageSize(1).setPageToken(reads.listBacklinks(reader,
                            ListBacklinksRequest.newBuilder().setTarget(t1).setPageSize(1).build()).getNextPageToken()).build()));

            // A tombstoned target dangles as tombstoned; a purged one is unavailable. Neither edits the linker.
            new DocumentDeletes(f.catalog, Clock.systemUTC()).delete(processAuthority(), DeleteDocumentRequest.newBuilder()
                    .setAddress(t2).setExpected(at(1)).setOperationId("delete-t2").build());
            purges(f).purge(DocumentPurgesIT.administrator(), PurgeDocumentRequest.newBuilder().setPurgeId("purge-t1").setAddress(t1).build());
            var dangling = reads.listLinks(reader, ListLinksRequest.newBuilder().setAddress(src).build());
            assertThat(dangling.getLinksList()).extracting(l -> l.getStaticTarget().getTargetTombstoned(),
                    l -> l.getStaticTarget().getTargetUnavailable()).containsExactly(
                    tuple(false, true), tuple(true, false), tuple(false, true), tuple(true, false), tuple(false, true));
            assertThat(f.catalog.links(src)).hasSize(5);
            assertThat(reads.listBacklinks(reader, ListBacklinksRequest.newBuilder().setTarget(t1).setPageSize(10).build())
                    .getBacklinksCount()).isEqualTo(3);

            // The linker's tombstone clears its edges; its purge has nothing left to clear.
            new DocumentDeletes(f.catalog, Clock.systemUTC()).delete(processAuthority(), DeleteDocumentRequest.newBuilder()
                    .setAddress(src).setExpected(at(1)).setOperationId("delete-src").build());
            assertThat(f.catalog.links(src)).isEmpty();
            assertThat(reads.listBacklinks(reader, ListBacklinksRequest.newBuilder().setTarget(t2).setPageSize(10).build())
                    .getBacklinksCount()).isZero();
            assertThat(reads.listLinks(reader, ListLinksRequest.newBuilder().setAddress(src)
                    .setRevision(RevisionSelector.newBuilder().setRevision(1)).build()).getLinksCount()).isEqualTo(5);
        }
    }

    @Test void aPurgedLinkerLeavesNoEdge() throws Exception {
        try (var f = new AccessionFixture()) {
            var saves = saves(f);
            var target = address(ACCOUNT, "t");
            saves.save(processAuthority(), save("t", target, document("t", citation(c -> {}), "reader")));
            var src = address(ACCOUNT, "src");
            saves.save(processAuthority(), save("src", src, document("src", citation(c ->
                    c.setField(CITATION.findFieldByName("cites"), link(fixed(target, null)))), "reader")));
            assertThat(rows(f, "SELECT count(*) FROM catalog_document_links")).isEqualTo(1);
            purges(f).purge(DocumentPurgesIT.administrator(), PurgeDocumentRequest.newBuilder().setPurgeId("purge-src").setAddress(src).build());
            assertThat(rows(f, "SELECT count(*) FROM catalog_document_links")).isZero();
            assertThat(linkReads(f).listBacklinks(processAuthority(), ListBacklinksRequest.newBuilder().setTarget(target)
                    .setPageSize(10).build()).getBacklinksCount()).isZero();
        }
    }

    @Test void staticTargetsAreCheckedAtSaveAndRefusalsAreRecorded() throws Exception {
        try (var f = new AccessionFixture()) {
            var saves = saves(f);
            var credential = UUID.randomUUID();
            f.catalog.registerCredential(ISSUER, credential, "chunker");
            f.stepGrant(ACCOUNT, credential, "chunker", true);
            var chunker = writer(ACCOUNT, "chunker", credential, 1);
            var visible = address(ACCOUNT, "visible");
            var hidden = address(ACCOUNT, "hidden");
            saves.save(processAuthority(), save("visible", visible, document("visible", citation(c -> {}), "chunker")));
            saves.save(processAuthority(), save("hidden", hidden, document("hidden", citation(c -> {}), "other")));
            var elsewhere = address("b", "elsewhere");
            saves.save(processAuthority(), save("elsewhere", elsewhere, document("b", "elsewhere", citation(c -> {}), "chunker")));
            var purged = address(ACCOUNT, "purged");
            saves.save(processAuthority(), save("purged", purged, document("purged", citation(c -> {}), "chunker")));
            purges(f).purge(DocumentPurgesIT.administrator(), PurgeDocumentRequest.newBuilder().setPurgeId("purge").setAddress(purged).build());

            var cases = Map.of(
                    "missing", fixed(address(ACCOUNT, "nope"), null),
                    "unreadable", fixed(hidden, null),
                    "other-account", fixed(elsewhere, null),
                    "absent-revision", fixed(visible, 7L),
                    "purged", fixed(purged, null));
            for (var refused : cases.entrySet()) {
                var operation = "link-" + refused.getKey();
                var request = save(operation, address(ACCOUNT, operation), document(operation, citation(c ->
                        c.setField(CITATION.findFieldByName("cites"), link(refused.getValue()))), "chunker"))
                        .toBuilder().setOwner(stepOwner("chunker")).build();
                var rejection = SemanticPartsIT.rejected(() -> saves.save(chunker, request), Status.Code.FAILED_PRECONDITION,
                        RejectionReason.REJECTION_REASON_LINK_TARGET_NOT_FOUND);
                assertThat(rejection.getMessage()).as(refused.getKey())
                        .isEqualTo("The target of link structured_data.cites is not found");
                assertThat(recorded(f, operation)).as(refused.getKey()).isEqualTo(RejectionReason.REJECTION_REASON_LINK_TARGET_NOT_FOUND);
                assertThat(f.catalog.current(address(ACCOUNT, operation))).isEmpty();
            }
            var accepted = save("link-visible", address(ACCOUNT, "link-visible"), document("link-visible", citation(c ->
                    c.setField(CITATION.findFieldByName("cites"), link(fixed(visible, 1L)))), "chunker"))
                    .toBuilder().setOwner(stepOwner("chunker")).build();
            assertThat(saves.save(chunker, accepted).getAccession().getRevision()).isEqualTo(1);

            // A tombstoned target is accepted: the link is born dangling.
            new DocumentDeletes(f.catalog, Clock.systemUTC()).delete(processAuthority(), DeleteDocumentRequest.newBuilder()
                    .setAddress(visible).setExpected(at(1)).setOperationId("delete-visible").build());
            saves.save(chunker, save("born-dangling", address(ACCOUNT, "born-dangling"), document("born-dangling", citation(c ->
                    c.setField(CITATION.findFieldByName("cites"), link(fixed(visible, null)))), "chunker"))
                    .toBuilder().setOwner(stepOwner("chunker")).build());
            assertThat(linkReads(f).listLinks(chunker, ListLinksRequest.newBuilder().setAddress(address(ACCOUNT, "born-dangling"))
                    .build()).getLinks(0).getStaticTarget().getTargetTombstoned()).isTrue();
        }
    }

    @Test void targetTypesCheckTheAddressedRevisionsType() throws Exception {
        try (var f = new AccessionFixture()) {
            var saves = saves(f);
            var evolving = address(ACCOUNT, "evolving");
            saves.save(processAuthority(), save("evolving-1", evolving, document("evolving", citation(c -> {}), "reader")));
            var numeric = document("evolving", null, "reader").toBuilder().setStructuredData(Any.pack(Int64Value.of(7))).build();
            saves.save(processAuthority(), save("evolving-2", evolving, numeric).toBuilder().setExpected(at(1)).build());
            var untyped = address(ACCOUNT, "untyped");
            saves.save(processAuthority(), save("untyped", untyped, document("untyped", null, "reader")));

            assertThat(f.catalog.linkTarget(evolving, OptionalLong.of(1)).orElseThrow().pinned().orElseThrow()
                    .structuredDataType()).contains(CITATION_TYPE);
            assertThat(f.catalog.linkTarget(evolving, OptionalLong.empty()).orElseThrow().currentType())
                    .contains(Int64Value.getDescriptor().getFullName());

            for (var refused : List.of(fixed(evolving, null), fixed(evolving, 2L), fixed(untyped, null))) {
                var operation = "typed-" + UUID.randomUUID();
                var rejection = SemanticPartsIT.rejected(() -> saves.save(processAuthority(), save(operation,
                        address(ACCOUNT, operation), document(operation, citation(c ->
                                c.setField(CITATION.findFieldByName("typed"), link(refused))), "reader"))),
                        Status.Code.FAILED_PRECONDITION, RejectionReason.REJECTION_REASON_LINK_TARGET_TYPE);
                assertThat(rejection.getMessage()).startsWith("The target of link structured_data.typed holds ");
                assertThat(recorded(f, operation)).isEqualTo(RejectionReason.REJECTION_REASON_LINK_TARGET_TYPE);
            }
            // Pinned to the revision that held a Citation: the current type is not substituted.
            saves.save(processAuthority(), save("typed-pinned", address(ACCOUNT, "typed-pinned"), document("typed-pinned",
                    citation(c -> c.setField(CITATION.findFieldByName("typed"), link(fixed(evolving, 1L)))), "reader")));
            // A field with empty target_types takes any document, typed or not.
            saves.save(processAuthority(), save("untyped-ok", address(ACCOUNT, "untyped-ok"), document("untyped-ok",
                    citation(c -> c.setField(CITATION.findFieldByName("cites"), link(fixed(untyped, null)))), "reader")));
        }
    }

    @Test void anOpaqueStructuredDataHasNoAdmittedType() {
        var lookup = DocumentLookup.newBuilder().setStructuredDataType(CITATION_TYPE).build();
        var root = OccurrenceLocator.newBuilder().setPart(DocumentSaves.CORE_SELECTOR)
                .addSteps(RepositorySchemaOccurrenceStep.newBuilder().setFieldNumber(Document.STRUCTURED_DATA_FIELD_NUMBER));
        var opaqueRoot = RevisionValidation.newBuilder().addOpaqueOccurrences(OpaqueOccurrence.newBuilder().setOccurrence(root)).build();
        assertThat(DocumentSaves.admittedLookup(lookup, opaqueRoot).hasStructuredDataType()).isFalse();
        var nested = RevisionValidation.newBuilder().addOpaqueOccurrences(OpaqueOccurrence.newBuilder().setOccurrence(root.clone()
                .addSteps(RepositorySchemaOccurrenceStep.newBuilder().setFieldNumber(9)))).build();
        assertThat(DocumentSaves.admittedLookup(lookup, nested).getStructuredDataType()).isEqualTo(CITATION_TYPE);
        assertThat(DocumentSaves.admittedLookup(lookup, RevisionValidation.getDefaultInstance())).isEqualTo(lookup);
    }

    @Test void aLinkFieldWithoutItsOptionRefusesTheSave() throws Exception {
        try (var f = new AccessionFixture()) {
            var target = address(ACCOUNT, "t");
            var saves = saves(f);
            saves.save(processAuthority(), save("t", target, document("t", citation(c -> {}), "reader")));
            var payload = DynamicMessage.newBuilder(UNLINKED).setField(UNLINKED.findFieldByName("cites"), link(fixed(target, null))).build();
            var unlinked = document("bad", null, "reader").toBuilder().setStructuredData(Any.newBuilder().setTypeUrl(UNLINKED_URL)
                    .setValue(payload.toByteString())).build();
            var rejection = SemanticPartsIT.rejected(() -> saves.save(processAuthority(), save("bad", address(ACCOUNT, "bad"), unlinked)),
                    Status.Code.INVALID_ARGUMENT, RejectionReason.REJECTION_REASON_VALIDATION_FAILED);
            assertThat(rejection.getMessage()).contains("test.links.v1.Unlinked.cites").contains("needs the link option");
        }
    }

    @Test void aLinkThatDoesNotFitTheCatalogIsRefusedByName() throws Exception {
        try (var f = new AccessionFixture()) {
            var saves = saves(f);
            var target = address(ACCOUNT, "t");
            saves.save(processAuthority(), save("t", target, document("t", citation(c -> {}), "reader")));
            // structured_data plus four 260-character segments: 1059 bytes of names and dots.
            Message leaf = DynamicMessage.newBuilder(FILE.findMessageTypeByName("Deep3"))
                    .setField(FILE.findMessageTypeByName("Deep3").findFieldByName(LONG_NAME), link(fixed(target, null))).build();
            for (var level : List.of("Deep2", "Deep1", "Deep0")) {
                var type = FILE.findMessageTypeByName(level);
                leaf = DynamicMessage.newBuilder(type).setField(type.findFieldByName(LONG_NAME), leaf).build();
            }
            var path = "structured_data." + String.join(".", java.util.Collections.nCopies(4, LONG_NAME));
            assertThat(path.length()).isGreaterThan(1024);
            var deep = document("deep", null, "reader").toBuilder().setStructuredData(Any.newBuilder().setTypeUrl(DEEP_URL)
                    .setValue(leaf.toByteString())).build();
            var rejection = SemanticPartsIT.rejected(() -> saves.save(processAuthority(), save("deep", address(ACCOUNT, "deep"), deep)),
                    Status.Code.INVALID_ARGUMENT, RejectionReason.REJECTION_REASON_VALIDATION_FAILED);
            assertThat(rejection.getViolationsList()).extracting(FieldViolation::getFieldPath, FieldViolation::getRuleId,
                    FieldViolation::getMessage).containsExactly(tuple(path, "link-catalog-storable", "field path exceeds 1024 bytes"));
            assertThat(recorded(f, "deep")).isEqualTo(RejectionReason.REJECTION_REASON_VALIDATION_FAILED);
            assertThat(f.catalog.current(address(ACCOUNT, "deep"))).isEmpty();
            // Refused before any stack write: only the target's save wrote objects.
            assertThat(f.stack.puts.get()).isEqualTo(1);

            // A relation plus target call number past the backlink index bound is refused the same way.
            var long_ = address(ACCOUNT, "x".repeat(2100));
            var refused = SemanticPartsIT.rejected(() -> saves.save(processAuthority(), save("wide", address(ACCOUNT, "wide"),
                    document("wide", citation(c -> c.setField(CITATION.findFieldByName("cites"), link(fixed(long_, null)))), "reader"))),
                    Status.Code.INVALID_ARGUMENT, RejectionReason.REJECTION_REASON_VALIDATION_FAILED);
            assertThat(refused.getViolationsList()).extracting(FieldViolation::getFieldPath, FieldViolation::getRuleId,
                    FieldViolation::getMessage).containsExactly(tuple("structured_data.cites", "link-catalog-storable",
                    "relation and target call number exceed 2048 bytes"));
        }
    }

    @Test void aLinkPinnedToATombstoneRevisionIsNotFound() throws Exception {
        try (var f = new AccessionFixture()) {
            var saves = saves(f);
            var z = address(ACCOUNT, "z");
            saves.save(processAuthority(), save("z-1", z, document("z", citation(c -> {}), "reader")));
            new DocumentDeletes(f.catalog, Clock.systemUTC()).delete(processAuthority(), DeleteDocumentRequest.newBuilder()
                    .setAddress(z).setExpected(at(1)).setOperationId("delete-z").build());
            saves.save(processAuthority(), save("z-3", z, document("z", citation(c -> {}), "reader")).toBuilder()
                    .setExpected(at(2)).build());
            SemanticPartsIT.rejected(() -> saves.save(processAuthority(), save("pin-tombstone", address(ACCOUNT, "pin-tombstone"),
                    document("pin-tombstone", citation(c -> c.setField(CITATION.findFieldByName("cites"), link(fixed(z, 2L)))), "reader"))),
                    Status.Code.FAILED_PRECONDITION, RejectionReason.REJECTION_REASON_LINK_TARGET_NOT_FOUND);
            saves.save(processAuthority(), save("pin-live", address(ACCOUNT, "pin-live"), document("pin-live", citation(c ->
                    c.setField(CITATION.findFieldByName("cites"), link(fixed(z, 3L)))), "reader")));
        }
    }

    // ------------------------------------------------------------------ dynamic links

    @Test void dynamicLinksResolveUnderTheReadersAccess() throws Exception {
        try (var f = new AccessionFixture()) {
            var saves = saves(f);
            for (var target : List.of(new String[]{"d1", "Beta", "x", "reader"}, new String[]{"d2", "Alpha", "x", "other"},
                    new String[]{"d3", "Gamma", "x", "reader"}, new String[]{"d4", "Delta", "y", "reader"})) {
                var content = document(target[0], citation(c -> {}), target[3]).toBuilder().setSearchMetadata(SearchMetadata.newBuilder()
                        .setTitle(target[1]).setCategory(target[2])).build();
                saves.save(processAuthority(), save(target[0], address(ACCOUNT, target[0]), content));
            }
            var selector = DynamicTarget.newBuilder().setFilter(DocumentFilter.newBuilder().setCategory("x"))
                    .setSort(DocumentSort.newBuilder().setField(DocumentSortField.DOCUMENT_SORT_FIELD_TITLE)).build();
            var src = address(ACCOUNT, "src");
            saves.save(processAuthority(), save("src", src, document("src", citation(c -> {
                c.setField(CITATION.findFieldByName("related"), link(DocumentLink.newBuilder().setDynamicTarget(selector).build()));
                c.setField(CITATION.findFieldByName("cites"), link(fixed(address(ACCOUNT, "d4"), null)));
            }), "reader", "other")));
            // A dynamic link writes no edge.
            assertThat(f.catalog.links(src)).extracting(CatalogRecords.Link::fieldPath).containsExactly("structured_data.cites");

            var reads = linkReads(f);
            var listed = reads.listLinks(reader("reader"), ListLinksRequest.newBuilder().setAddress(src).build());
            assertThat(listed.getLinks(1).getFieldPath()).isEqualTo("structured_data.related");
            assertThat(listed.getLinks(1).getDynamicTarget().getFilter()).isEqualTo(selector.getFilter());
            assertThat(listed.getLinks(1).getRelation()).isEqualTo("related");

            var resolve = ResolveLinkRequest.newBuilder().setAddress(src).setFieldPath("structured_data.related").setPageSize(1);
            assertThat(resolveAll(reads, reader("reader"), resolve)).containsExactly("d1", "d3");
            assertThat(resolveAll(reads, reader("other"), resolve)).containsExactly("d2");
            new DocumentDeletes(f.catalog, Clock.systemUTC()).delete(processAuthority(), DeleteDocumentRequest.newBuilder()
                    .setAddress(address(ACCOUNT, "d3")).setExpected(at(1)).setOperationId("delete-d3").build());
            assertThat(resolveAll(reads, reader("reader"), resolve)).containsExactly("d1");

            DocumentDeletesIT.assertCode(Status.Code.FAILED_PRECONDITION, () -> reads.resolveLink(reader("reader"),
                    resolve.clone().setFieldPath("structured_data.cites").build()));
            DocumentDeletesIT.assertCode(Status.Code.NOT_FOUND, () -> reads.resolveLink(reader("reader"),
                    resolve.clone().setOrdinal(1).build()));
            DocumentDeletesIT.assertCode(Status.Code.PERMISSION_DENIED, () -> reads.resolveLink(reader("stranger"), resolve.build()));
        }
    }

    @Test void aDynamicSelectorMustSelectSomethingConsistently() throws Exception {
        try (var f = new AccessionFixture()) {
            var saves = saves(f);
            var empty = DynamicTarget.newBuilder().setFilter(DocumentFilter.getDefaultInstance()).build();
            var rejection = SemanticPartsIT.rejected(() -> saves.save(processAuthority(), save("empty", address(ACCOUNT, "empty"),
                    document("empty", citation(c -> c.setField(CITATION.findFieldByName("related"),
                            link(DocumentLink.newBuilder().setDynamicTarget(empty).build()))), "reader"))),
                    Status.Code.INVALID_ARGUMENT, RejectionReason.REJECTION_REASON_VALIDATION_FAILED);
            assertThat(rejection.getViolationsList()).extracting(FieldViolation::getFieldPath, FieldViolation::getRuleId)
                    .containsExactly(tuple("structured_data.related", "link-dynamic-filter-required"));
            var contradictory = DynamicTarget.newBuilder().setFilter(DocumentFilter.newBuilder()
                    .addMetadata(MetadataMatch.newBuilder().setKey("k").setValue("1"))
                    .addMetadata(MetadataMatch.newBuilder().setKey("k").setValue("2"))).build();
            var refused = SemanticPartsIT.rejected(() -> saves.save(processAuthority(), save("contradictory",
                    address(ACCOUNT, "contradictory"), document("contradictory", citation(c -> c.addRepeatedField(
                            CITATION.findFieldByName("sections"), section(DocumentLink.newBuilder().setDynamicTarget(contradictory)
                                    .build()))), "reader"))),
                    Status.Code.INVALID_ARGUMENT, RejectionReason.REJECTION_REASON_VALIDATION_FAILED);
            assertThat(refused.getViolationsList()).extracting(FieldViolation::getFieldPath, FieldViolation::getRuleId)
                    .containsExactly(tuple("structured_data.sections[0].cites[0]", "link-dynamic-filter-consistent"));
            assertThat(recorded(f, "contradictory")).isEqualTo(RejectionReason.REJECTION_REASON_VALIDATION_FAILED);
        }
    }

    @Test void aStaticTargetRefusalKeepsCollectedDynamicFilterViolations() throws Exception {
        try (var f = new AccessionFixture()) {
            var saves = saves(f);
            var empty = DynamicTarget.newBuilder().setFilter(DocumentFilter.getDefaultInstance()).build();
            var missing = citation(c -> c.setField(CITATION.findFieldByName("cites"),
                    link(fixed(address(ACCOUNT, "nope"), null))));
            var rejection = SemanticPartsIT.rejected(() -> saves.save(processAuthority(), save("kept",
                    address(ACCOUNT, "kept"), document("kept", citation(c -> {
                        c.setField(CITATION.findFieldByName("related"),
                                link(DocumentLink.newBuilder().setDynamicTarget(empty).build()));
                        c.setField(CITATION.findFieldByName("nested"), packed(missing));
                    }), "reader"))),
                    Status.Code.FAILED_PRECONDITION, RejectionReason.REJECTION_REASON_LINK_TARGET_NOT_FOUND);
            assertThat(rejection.getMessage()).isEqualTo("The target of link structured_data.nested.cites is not found");
            assertThat(rejection.getViolationsList()).extracting(FieldViolation::getFieldPath, FieldViolation::getRuleId)
                    .containsExactly(tuple("structured_data.related", "link-dynamic-filter-required"));
            assertThat(recorded(f, "kept")).isEqualTo(RejectionReason.REJECTION_REASON_LINK_TARGET_NOT_FOUND);
            assertThat(f.catalog.current(address(ACCOUNT, "kept"))).isEmpty();
        }
    }

    // ------------------------------------------------------------------ backlinks, carry-forward, rebuild

    @Test void backlinksShowOnlyLinkersTheReaderCanRead() throws Exception {
        try (var f = new AccessionFixture()) {
            var saves = saves(f);
            var target = address(ACCOUNT, "t");
            saves.save(processAuthority(), save("t", target, document("t", citation(c -> {}), "reader", "other")));
            for (var linker : List.of(new String[]{"l1", "reader"}, new String[]{"l2", "other"}, new String[]{"l3", "reader"}))
                saves.save(processAuthority(), save(linker[0], address(ACCOUNT, linker[0]), document(linker[0], citation(c ->
                        c.setField(CITATION.findFieldByName("cites"), link(fixed(target, null)))), linker[1])));
            var reads = linkReads(f);
            var request = ListBacklinksRequest.newBuilder().setTarget(target).setPageSize(1);
            var page = reads.listBacklinks(reader("reader"), request.build());
            assertThat(page.getBacklinksList()).extracting(b -> b.getAddress().getDocId()).containsExactly("l1");
            // The token names the last returned edge, never the skipped one.
            var token = BacklinkPageToken.parseFrom(java.util.Base64.getUrlDecoder().decode(page.getNextPageToken()));
            assertThat(token.getAfter().getDocId()).isEqualTo("l1");
            var next = reads.listBacklinks(reader("reader"), request.setPageToken(page.getNextPageToken()).build());
            assertThat(next.getBacklinksList()).extracting(b -> b.getAddress().getDocId()).containsExactly("l3");
            assertThat(next.getNextPageToken()).isEmpty();
            assertThat(reads.listBacklinks(reader("other"), ListBacklinksRequest.newBuilder().setTarget(target).setPageSize(10).build())
                    .getBacklinksList()).extracting(b -> b.getAddress().getDocId()).containsExactly("l2");
            assertThat(reads.listBacklinks(processAuthority(), ListBacklinksRequest.newBuilder().setTarget(target).setPageSize(10).build())
                    .getBacklinksCount()).isEqualTo(3);
            DocumentDeletesIT.assertCode(Status.Code.PERMISSION_DENIED, () -> reads.listBacklinks(reader("stranger"),
                    ListBacklinksRequest.newBuilder().setTarget(target).setPageSize(10).build()));
        }
    }

    @Test void aCarriedCoreKeepsItsLinksUnchecked() throws Exception {
        try (var f = new AccessionFixture()) {
            var saves = saves(f);
            var target = address(ACCOUNT, "t");
            saves.save(processAuthority(), save("t", target, document("t", citation(c -> {}), "reader")));
            var src = address(ACCOUNT, "src");
            saves.save(processAuthority(), save("src", src, document("src", citation(c ->
                    c.setField(CITATION.findFieldByName("cites"), link(fixed(target, null)))), "reader")));
            var before = f.catalog.links(src);
            purges(f).purge(DocumentPurgesIT.administrator(), PurgeDocumentRequest.newBuilder().setPurgeId("purge-t").setAddress(target).build());
            // PARSED changes, CORE is carried: its link is not rechecked against the purged target.
            saves.save(processAuthority(), SaveDocumentRequest.newBuilder().setOperationId("carry")
                    .setTarget(DocumentTarget.newBuilder().setAddress(src)).setExpected(at(1)).setOwner(intakeOwner())
                    .addWrites(PartWrite.newBuilder().setSelector(PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_PARSED))
                            .setAction(WriteAction.WRITE_ACTION_EMPTY)).build());
            assertThat(f.catalog.current(src).orElseThrow().revision()).isEqualTo(2);
            assertThat(f.catalog.links(src)).isEqualTo(before);
            assertThat(f.catalog.linkTarget(src, OptionalLong.of(2)).orElseThrow().pinned().orElseThrow().structuredDataType())
                    .contains(CITATION_TYPE);
        }
    }

    @Test void aTypedNestedAnyIsEnteredWithoutASegmentOfItsOwn() throws Exception {
        try (var f = new AccessionFixture()) {
            var saves = saves(f);
            var target = address(ACCOUNT, "t");
            saves.save(processAuthority(), save("t", target, document("t", citation(c -> {}), "reader")));
            var inner = citation(c -> c.setField(CITATION.findFieldByName("cites"), link(fixed(target, null))));
            var src = address(ACCOUNT, "src");
            saves.save(processAuthority(), save("src", src, document("src", citation(c -> {
                c.setField(CITATION.findFieldByName("nested"), packed(inner));
                c.addRepeatedField(CITATION.findFieldByName("extras"), packed(citation(x -> {})));
                c.addRepeatedField(CITATION.findFieldByName("extras"), packed(citation(x -> x.addRepeatedField(
                        CITATION.findFieldByName("sections"), section(fixed(target, 1L))))));
            }), "reader")));
            var expected = List.of(tuple("structured_data.extras[1].sections[0].cites", 0), tuple("structured_data.nested.cites", 0));
            assertThat(f.catalog.links(src)).extracting(CatalogRecords.Link::fieldPath, CatalogRecords.Link::ordinal)
                    .containsExactlyElementsOf(expected);
            var listed = linkReads(f).listLinks(reader("reader"), ListLinksRequest.newBuilder().setAddress(src).build());
            assertThat(listed.getLinksList()).extracting(ResolvedLink::getFieldPath, ResolvedLink::getOrdinal)
                    .containsExactly(tuple("structured_data.nested.cites", 0), tuple("structured_data.extras[1].sections[0].cites", 0));
            assertThat(linkReads(f).listBacklinks(reader("reader"), ListBacklinksRequest.newBuilder().setTarget(target)
                    .setPageSize(10).build()).getBacklinksList()).extracting(Backlink::getFieldPath)
                    .containsExactly("structured_data.extras[1].sections[0].cites", "structured_data.nested.cites");
            var edges = f.catalog.links(src);
            try (var connection = DriverManager.getConnection(f.url, POSTGRES.getUsername(), POSTGRES.getPassword())) {
                connection.createStatement().execute("DELETE FROM catalog_document_links");
            }
            new CatalogEntryRebuild(f.catalog, f.reads).rebuild(ACCOUNT);
            assertThat(f.catalog.links(src)).isEqualTo(edges);

            // The nested target is checked like any other: a missing one refuses the save.
            var missing = citation(c -> c.setField(CITATION.findFieldByName("cites"), link(fixed(address(ACCOUNT, "nope"), null))));
            var rejection = SemanticPartsIT.rejected(() -> saves.save(processAuthority(), save("nested-missing",
                    address(ACCOUNT, "nested-missing"), document("nested-missing", citation(c ->
                            c.setField(CITATION.findFieldByName("nested"), packed(missing))), "reader"))),
                    Status.Code.FAILED_PRECONDITION, RejectionReason.REJECTION_REASON_LINK_TARGET_NOT_FOUND);
            assertThat(rejection.getMessage()).isEqualTo("The target of link structured_data.nested.cites is not found");
        }
    }

    @Test void anOpaqueAnyIsNotReadThroughATypedSiblingWithTheSameBytes() throws Exception {
        try (var f = new AccessionFixture()) {
            var saves = saves(f);
            var target = address(ACCOUNT, "t");
            saves.save(processAuthority(), save("t", target, document("t", citation(c -> {}), "reader", "chunker")));
            var inner = packed(citation(c -> c.setField(CITATION.findFieldByName("cites"), link(fixed(target, null)))));
            var payload = citation(c -> {
                c.setField(CITATION.findFieldByName("nested"), inner);
                c.addRepeatedField(CITATION.findFieldByName("extras"), inner);
            });
            // Typed throughout, both siblings are entered. The save records nested's occurrence;
            // an attestation names the same steps without the Any's own boundary.
            var typed = address(ACCOUNT, "typed");
            saves.save(processAuthority(), save("typed", typed, document("typed", payload, "reader", "chunker")));
            assertThat(f.catalog.links(typed)).extracting(CatalogRecords.Link::fieldPath)
                    .containsExactly("structured_data.extras[0].cites", "structured_data.nested.cites");
            var nested = f.catalog.manifest(typed, 1).orElseThrow().getValidation().getResolvedOccurrencesList().stream()
                    .map(ResolvedOccurrence::getOccurrence)
                    .filter(locator -> locator.getStepsCount() > 1
                            && locator.getSteps(locator.getStepsCount() - 2).getFieldNumber() == 6)
                    .map(locator -> locator.toBuilder().removeSteps(locator.getStepsCount() - 1).build())
                    .findFirst().orElseThrow();

            // The same bytes with nested attested opaque: only the typed extras[0] is read.
            var credential = UUID.randomUUID();
            f.catalog.registerCredential(ISSUER, credential, "chunker");
            f.opaqueStepGrant(ACCOUNT, credential, "chunker", true, "grant-producer", CITATION_URL);
            var mixed = address(ACCOUNT, "mixed");
            var opaqueSaves = new DocumentSaves(f.catalog, f.drives, resolver(), limits(), bytes -> () -> {},
                    Clock.systemUTC(), true);
            var saved = opaqueSaves.save(writer(ACCOUNT, "chunker", credential, 1),
                    save("mixed", mixed, document("mixed", payload, "reader", "chunker")).toBuilder()
                            .setOwner(stepOwner("chunker")).setValidationMode(ValidationMode.VALIDATION_MODE_OPAQUE_ALLOWED)
                            .addOpaqueAttestations(OpaqueAttestation.newBuilder().setOccurrence(nested).setTypeUrl(CITATION_URL))
                            .build());
            assertThat(saved.getAccession().getValidation().getOpaqueOccurrencesList()).singleElement()
                    .satisfies(opaque -> assertThat(opaque.getOccurrence()).isEqualTo(nested));
            var expected = List.of("structured_data.extras[0].cites");
            assertThat(f.catalog.links(mixed)).extracting(CatalogRecords.Link::fieldPath).isEqualTo(expected);
            assertThat(linkReads(f).listLinks(reader("reader"), ListLinksRequest.newBuilder().setAddress(mixed).build())
                    .getLinksList()).extracting(ResolvedLink::getFieldPath).isEqualTo(expected);
            var edges = f.catalog.links(mixed);
            try (var connection = DriverManager.getConnection(f.url, POSTGRES.getUsername(), POSTGRES.getPassword())) {
                connection.createStatement().execute("DELETE FROM catalog_document_links");
            }
            new CatalogEntryRebuild(f.catalog, f.reads).rebuild(ACCOUNT);
            assertThat(f.catalog.links(mixed)).isEqualTo(edges);
        }
    }

    @Test void anAnyHoldingAnAnyIsUnwrappedForItsLinks() throws Exception {
        try (var f = new AccessionFixture()) {
            var saves = saves(f);
            var target = address(ACCOUNT, "t");
            saves.save(processAuthority(), save("t", target, document("t", citation(c -> {}), "reader")));
            var src = address(ACCOUNT, "src");
            saves.save(processAuthority(), save("src", src, document("src", citation(c -> c.setField(
                    CITATION.findFieldByName("nested"), wrapped(link(fixed(target, null))))), "reader")));
            assertThat(f.catalog.links(src)).extracting(CatalogRecords.Link::fieldPath)
                    .containsExactly("structured_data.nested.cites");
            assertThat(linkReads(f).listLinks(reader("reader"), ListLinksRequest.newBuilder().setAddress(src).build())
                    .getLinksList()).extracting(ResolvedLink::getFieldPath).containsExactly("structured_data.nested.cites");

            // The doubly wrapped target is gated like any other.
            var rejection = SemanticPartsIT.rejected(() -> saves.save(processAuthority(), save("wrapped-missing",
                    address(ACCOUNT, "wrapped-missing"), document("wrapped-missing", citation(c -> c.setField(
                            CITATION.findFieldByName("nested"), wrapped(link(fixed(address(ACCOUNT, "nope"), null))))),
                            "reader"))),
                    Status.Code.FAILED_PRECONDITION, RejectionReason.REJECTION_REASON_LINK_TARGET_NOT_FOUND);
            assertThat(rejection.getMessage()).isEqualTo("The target of link structured_data.nested.cites is not found");
        }
    }

    @Test void aLinkUnderAMapValueThroughAnAnyIsRefused() throws Exception {
        try (var f = new AccessionFixture()) {
            var saves = saves(f);
            var target = address(ACCOUNT, "t");
            saves.save(processAuthority(), save("t", target, document("t", citation(c -> {}), "reader")));
            var entry = DynamicMessage.newBuilder(CITATION.findNestedTypeByName("NotesEntry"));
            entry.setField(entry.getDescriptorForType().findFieldByName("key"), "k");
            entry.setField(entry.getDescriptorForType().findFieldByName("value"), packed(citation(c -> {})));
            var rejection = SemanticPartsIT.rejected(() -> saves.save(processAuthority(), save("mapped", address(ACCOUNT, "mapped"),
                    document("mapped", citation(c -> c.addRepeatedField(CITATION.findFieldByName("notes"), entry.build())), "reader"))),
                    Status.Code.INVALID_ARGUMENT, RejectionReason.REJECTION_REASON_VALIDATION_FAILED);
            assertThat(rejection.getMessage()).contains("test.links.v1.Citation.notes").contains("a link cannot sit under a map value");
        }
    }

    @Test void aRebuildRecomputesEdgesAndRevisionTypes() throws Exception {
        try (var f = new AccessionFixture()) {
            var saves = saves(f);
            var t1 = address(ACCOUNT, "t1");
            saves.save(processAuthority(), save("t1", t1, document("t1", citation(c -> {}), "reader")));
            saves.save(processAuthority(), save("t1-2", t1, document("t1", null, "reader").toBuilder()
                    .setStructuredData(Any.pack(Int64Value.of(1))).build()).toBuilder().setExpected(at(1)).build());
            var src = address(ACCOUNT, "src");
            saves.save(processAuthority(), save("src", src, document("src", citation(c -> {
                c.setField(CITATION.findFieldByName("cites"), link(fixed(t1, 2L)));
                c.addRepeatedField(CITATION.findFieldByName("sections"), section(fixed(t1, null), fixed(t1, 1L)));
            }), "reader")));
            var edges = f.catalog.links(src);
            assertThat(edges).hasSize(3);
            try (var connection = DriverManager.getConnection(f.url, POSTGRES.getUsername(), POSTGRES.getPassword())) {
                connection.createStatement().execute("DELETE FROM catalog_document_links");
                connection.createStatement().execute("UPDATE catalog_revisions SET structured_data_type = NULL");
            }
            assertThat(f.catalog.links(src)).isEmpty();
            assertThat(f.catalog.linkTarget(t1, OptionalLong.of(1)).orElseThrow().pinned().orElseThrow().structuredDataType()).isEmpty();
            assertThat(new CatalogEntryRebuild(f.catalog, f.reads).rebuild(ACCOUNT)).isEqualTo(2);
            assertThat(f.catalog.links(src)).isEqualTo(edges);
            assertThat(f.catalog.linkTarget(t1, OptionalLong.of(1)).orElseThrow().pinned().orElseThrow().structuredDataType())
                    .contains(CITATION_TYPE);
            assertThat(f.catalog.linkTarget(t1, OptionalLong.of(2)).orElseThrow().pinned().orElseThrow().structuredDataType())
                    .contains(Int64Value.getDescriptor().getFullName());
            // Revisions stay immutable apart from the derived type.
            try (var connection = DriverManager.getConnection(f.url, POSTGRES.getUsername(), POSTGRES.getPassword())) {
                var failure = catchThrowableOfType(java.sql.SQLException.class, () ->
                        connection.createStatement().execute("UPDATE catalog_revisions SET operation_id = 'x'"));
                assertThat(failure.getMessage()).contains("immutable");
            }
        }
    }

    // ------------------------------------------------------------------ fixtures

    static DocumentSaves saves(AccessionFixture f) {
        return new DocumentSaves(f.catalog, f.drives, resolver(), limits(), bytes -> () -> {}, Clock.systemUTC());
    }

    static DocumentLinkReads linkReads(AccessionFixture f) {
        return new DocumentLinkReads(f.reads, new DocumentLists(f.catalog));
    }

    static DocumentPurges purges(AccessionFixture f) {
        return new DocumentPurges(f.catalog, f.drives, (caller, account) -> new PurgeAuthorization.Decision(true, true),
                Clock.systemUTC());
    }

    /** The fixture's registered types, plus Citation, Unlinked, Deep0 and google.protobuf.Any. */
    static DocumentSchemaAdmission.Resolver resolver() {
        var base = AccessionFixture.resolver();
        var citation = definition(CITATION, CITATION_URL);
        var unlinked = definition(UNLINKED, UNLINKED_URL);
        var deep = definition(DEEP, DEEP_URL);
        var any = definition(Any.getDescriptor(), ANY_URL);
        return selection -> CITATION_URL.equals(selection.typeUrl()) ? citation
                : UNLINKED_URL.equals(selection.typeUrl()) ? unlinked
                : DEEP_URL.equals(selection.typeUrl()) ? deep
                : ANY_URL.equals(selection.typeUrl()) ? any : base.select(selection);
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
     * test.links.v1: Citation (cites, sections, typed, related) and Section (repeated
     * cites) declare links; Unlinked holds a DocumentLink without the option.
     */
    private static FileDescriptor file() {
        var file = FileDescriptorProto.newBuilder().setName("test/links/v1/citation.proto").setPackage("test.links.v1")
                .setSyntax("proto3").addDependency(LinksProto.getDescriptor().getName())
                .addDependency(Any.getDescriptor().getFile().getName())
                .addMessageType(DescriptorProto.newBuilder().setName("Section")
                        .addField(link("cites", 1, true, LinkField.newBuilder().setRelation("references"))))
                .addMessageType(DescriptorProto.newBuilder().setName("Citation")
                        .addField(FieldDescriptorProto.newBuilder().setName("title").setNumber(1)
                                .setType(FieldDescriptorProto.Type.TYPE_STRING).setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                        .addField(link("cites", 2, false, LinkField.newBuilder().setRelation("references")))
                        .addField(message("sections", 3, ".test.links.v1.Section", true))
                        .addField(link("typed", 4, false, LinkField.newBuilder().setRelation("source").addTargetTypes(CITATION_TYPE)))
                        .addField(link("related", 5, false, LinkField.newBuilder().setRelation("related")))
                        .addField(message("nested", 6, ".google.protobuf.Any", false))
                        .addField(message("extras", 7, ".google.protobuf.Any", true))
                        .addField(message("notes", 8, ".test.links.v1.Citation.NotesEntry", true))
                        .addNestedType(DescriptorProto.newBuilder().setName("NotesEntry")
                                .setOptions(com.google.protobuf.DescriptorProtos.MessageOptions.newBuilder().setMapEntry(true))
                                .addField(FieldDescriptorProto.newBuilder().setName("key").setNumber(1)
                                        .setType(FieldDescriptorProto.Type.TYPE_STRING).setLabel(FieldDescriptorProto.Label.LABEL_OPTIONAL))
                                .addField(message("value", 2, ".google.protobuf.Any", false))))
                .addMessageType(DescriptorProto.newBuilder().setName("Unlinked").addField(message("cites", 1, LINK, false)))
                .addMessageType(DescriptorProto.newBuilder().setName("Deep0").addField(message(LONG_NAME, 1, ".test.links.v1.Deep1", false)))
                .addMessageType(DescriptorProto.newBuilder().setName("Deep1").addField(message(LONG_NAME, 1, ".test.links.v1.Deep2", false)))
                .addMessageType(DescriptorProto.newBuilder().setName("Deep2").addField(message(LONG_NAME, 1, ".test.links.v1.Deep3", false)))
                .addMessageType(DescriptorProto.newBuilder().setName("Deep3").addField(link(LONG_NAME, 1, false,
                        LinkField.newBuilder().setRelation("references"))))
                .build();
        try {
            return FileDescriptor.buildFrom(file, new FileDescriptor[]{LinksProto.getDescriptor(), Any.getDescriptor().getFile()});
        } catch (DescriptorValidationException invalid) {
            throw new IllegalStateException(invalid);
        }
    }

    private static FieldDescriptorProto.Builder link(String name, int number, boolean repeated, LinkField.Builder field) {
        return message(name, number, LINK, repeated).setOptions(FieldOptions.newBuilder().setExtension(LinksProto.link, field.build()));
    }

    private static FieldDescriptorProto.Builder message(String name, int number, String type, boolean repeated) {
        return FieldDescriptorProto.newBuilder().setName(name).setNumber(number).setTypeName(type)
                .setType(FieldDescriptorProto.Type.TYPE_MESSAGE).setLabel(repeated
                        ? FieldDescriptorProto.Label.LABEL_REPEATED : FieldDescriptorProto.Label.LABEL_OPTIONAL);
    }

    static Message citation(Consumer<DynamicMessage.Builder> fill) {
        var builder = DynamicMessage.newBuilder(CITATION).setField(CITATION.findFieldByName("title"), "citation");
        fill.accept(builder);
        return builder.build();
    }

    /** A Citation holding one link, packed into an Any that is itself packed into an Any. */
    static Message wrapped(Message link) {
        var inner = packed(citation(c -> c.setField(CITATION.findFieldByName("cites"), link)));
        return Any.newBuilder().setTypeUrl(ANY_URL).setValue(inner.toByteString()).build();
    }

    /** A Citation packed into a nested Any. */
    static Message packed(Message citation) {
        return Any.newBuilder().setTypeUrl(CITATION_URL).setValue(citation.toByteString()).build();
    }

    static Message section(DocumentLink... cites) {
        var builder = DynamicMessage.newBuilder(SECTION);
        for (var cite : cites) builder.addRepeatedField(SECTION.findFieldByName("cites"), link(cite));
        return builder.build();
    }

    static Message link(DocumentLink link) {
        try {
            return DynamicMessage.parseFrom(DocumentLink.getDescriptor(), link.toByteString());
        } catch (InvalidProtocolBufferException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    static DocumentLink fixed(NodeAddress target, Long revision) {
        var fixed = StaticTarget.newBuilder().setAddress(target);
        if (revision != null) fixed.setRevision(revision);
        return DocumentLink.newBuilder().setStaticTarget(fixed).build();
    }

    static Document document(String doc, Message payload, String... readers) {
        return document(ACCOUNT, doc, payload, readers);
    }

    static Document document(String account, String doc, Message payload, String... readers) {
        var document = Document.newBuilder().setDocId(doc).setOwnership(ownership(account, readers))
                .setSearchMetadata(SearchMetadata.newBuilder().setTitle(doc));
        if (payload != null)
            document.setStructuredData(Any.newBuilder().setTypeUrl(CITATION_URL).setValue(payload.toByteString()));
        return document.build();
    }

    /** A creating save that writes CORE only. */
    static SaveDocumentRequest save(String operation, NodeAddress address, Document document) {
        return SaveDocumentRequest.newBuilder().setOperationId(operation).setTarget(DocumentTarget.newBuilder().setAddress(address))
                .setExpected(ExpectedRevision.newBuilder().setNone(true)).setDocument(document)
                .addWrites(put(DocumentPart.DOCUMENT_PART_CORE)).setOwner(intakeOwner()).build();
    }

    static RepositoryCaller reader(String name) {
        return new RepositoryCaller(name, false, Set.of(name.equals("stranger") ? "z" : ACCOUNT),
                Set.of(Principal.newBuilder().setIdentityType("service").setIdentity(name).build()));
    }

    private static RejectionReason recorded(AccessionFixture f, String operation) {
        return f.catalog.outcome(ACCOUNT, operation).orElseThrow().outcome().getRejection().getReason();
    }

    private static List<String> resolveAll(DocumentLinkReads reads, RepositoryCaller caller, ResolveLinkRequest.Builder request) {
        var found = new ArrayList<String>();
        var page = request.clone().setPageToken("");
        do {
            var response = reads.resolveLink(caller, page.build());
            response.getDocumentsList().forEach(summary -> found.add(summary.getAddress().getDocId()));
            page.setPageToken(response.getNextPageToken());
        } while (!page.getPageToken().isEmpty());
        return found;
    }

    private static long rows(AccessionFixture f, String sql) throws Exception {
        try (var connection = DriverManager.getConnection(f.url, POSTGRES.getUsername(), POSTGRES.getPassword());
             var result = connection.createStatement().executeQuery(sql)) {
            result.next();
            return result.getLong(1);
        }
    }
}
