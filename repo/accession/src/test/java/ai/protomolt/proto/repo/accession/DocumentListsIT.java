package ai.protomolt.proto.repo.accession;

import static ai.protomolt.proto.repo.accession.AccessionFixture.*;
import static ai.protomolt.proto.repo.accession.DocumentDeletesIT.assertCode;
import static org.assertj.core.api.Assertions.*;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.ParseStatus;
import ai.protomolt.proto.repo.v1.ParserResult;
import ai.protomolt.proto.repo.v1.Principal;
import ai.protomolt.proto.repo.v1.SearchMetadata;
import ai.protomolt.proto.repo.v2.*;
import ai.protomolt.proto.repo.catalog.CatalogRecords;
import io.grpc.Status;
import java.sql.DriverManager;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class DocumentListsIT {
    @Test void pagesFilterAclTombstonesAndScopeWithoutReadingObjects() throws Exception {
        try (var f = new AccessionFixture()) {
            for (var id : List.of("a", "b", "c", "d", "e"))
                f.saves.save(processAuthority(), create("save-" + id, address("account", id),
                        document("account", id, id, id.equals("b") || id.equals("d") ? "other" : "reader")).build());
            var deleted = address("account", "c");
            new DocumentDeletes(f.catalog, Clock.systemUTC()).delete(processAuthority(), DeleteDocumentRequest.newBuilder()
                    .setAddress(deleted).setExpected(at(1)).setOperationId("delete").build());
            f.saves.save(processAuthority(), create("other-account", address("other", "z"), document("other", "z", "z", "reader")).build());
            f.stack.reset();
            var reader = new RepositoryCaller("reader", false, Set.of("account"),
                    Set.of(Principal.newBuilder().setIdentityType("service").setIdentity("reader").build()));
            var lists = new DocumentLists(f.catalog);
            var request = ListDocumentsRequest.newBuilder().setAccountId("account").setPageSize(1).build();
            var page = lists.list(reader, request);
            assertThat(page.getDocumentsList()).extracting(d -> d.getAddress().getDocId()).containsExactly("a");
            assertThat(page.getNextPageToken()).isNotEmpty();
            var token = CatalogPageToken.parseFrom(Base64.getUrlDecoder().decode(page.getNextPageToken()));
            assertThat(token.getAfter().getDocId()).isEqualTo("a");
            var last = lists.list(reader, request.toBuilder().setPageToken(page.getNextPageToken()).build());
            assertThat(last.getDocumentsList()).extracting(d -> d.getAddress().getDocId()).containsExactly("e");
            assertThat(last.getNextPageToken()).isEmpty();
            assertThat(lists.list(reader, request.toBuilder().setPageSize(10).setIncludeTombstoned(true).build()).getDocumentsList())
                    .extracting(d -> d.getAddress().getDocId()).containsExactly("a", "c", "e");
            assertThat(lists.list(reader, request.toBuilder().setGraphId("absent").build()).getDocumentsCount()).isZero();
            assertThat(lists.list(reader, request.toBuilder().setGraphAddressId("absent").build()).getDocumentsCount()).isZero();
            assertCode(Status.Code.INVALID_ARGUMENT, () -> lists.list(reader, request.toBuilder().setIncludeTombstoned(true).setPageToken(page.getNextPageToken()).build()));
            assertCode(Status.Code.INVALID_ARGUMENT, () -> lists.list(reader, request.toBuilder().setPageToken("not a token").build()));
            assertCode(Status.Code.PERMISSION_DENIED, () -> lists.list(reader, request.toBuilder().setAccountId("other").build()));
            assertThat(f.stack.opens.get()).isZero();
            assertThat(f.stack.puts.get()).isZero();
            assertThat(DocumentSaves.VALIDATOR.validate(page).valid()).isTrue();
        }
    }

    @Test void addressOrderAndProjectionSurviveRestart() throws Exception {
        try (var f = new AccessionFixture()) {
            var addresses = List.of(address("a", "z").toBuilder().setGraphId("g2").build(),
                    address("a", "é").toBuilder().setGraphId("g1").build(),
                    address("a", "a").toBuilder().setGraphId("g1").build());
            for (int i = 0; i < addresses.size(); i++)
                f.saves.save(processAuthority(), create("save-" + i, addresses.get(i), document("a", addresses.get(i).getDocId(), "title", "reader")).build());
            try (var restarted = f.restarted()) {
                var lists = new DocumentLists(restarted.catalog);
                var request = ListDocumentsRequest.newBuilder().setAccountId("a").setPageSize(1);
                var seen = new ArrayList<ai.protomolt.proto.repo.v1.NodeAddress>();
                do {
                    var page = lists.list(processAuthority(), request.build());
                    page.getDocumentsList().forEach(d -> seen.add(d.getAddress()));
                    request.setPageToken(page.getNextPageToken());
                } while (!request.getPageToken().isEmpty());
                assertThat(seen).containsExactly(addresses.get(2), addresses.get(1), addresses.get(0));
                var rows = restarted.catalog.documents(CatalogRecords.Listing.of("a", "g1", "", false), Optional.empty(), 10);
                assertThat(rows).hasSize(2);
                assertThat(rows.getFirst().entry().getOwnership()).isEqualTo(ownership("a", "reader"));
                assertThat(rows.getFirst().entry().getDocId()).isEqualTo("a");
                assertThat(rows.getFirst().committedAt()).isEqualTo(restarted.catalog.manifest(addresses.get(2), 1).orElseThrow().getCommittedAt());
            }
        }
    }
    @Test void nextPageUsesCurrentAclRatherThanAnEarlierProjection() throws Exception {
        try (var f = new AccessionFixture()) {
            for (var id : List.of("a", "b")) f.saves.save(processAuthority(), create("save-" + id, address("a", id), document("a", id, id, "reader")).build());
            var reader = new RepositoryCaller("reader", false, Set.of("a"),
                    Set.of(Principal.newBuilder().setIdentityType("service").setIdentity("reader").build()));
            var lists = new DocumentLists(f.catalog);
            var request = ListDocumentsRequest.newBuilder().setAccountId("a").setPageSize(1).build();
            var first = lists.list(reader, request);
            var replacement = document("a", "b", "changed", "other").toBuilder().clearBlobBag().clearParserResults().build();
            f.saves.save(processAuthority(), SaveDocumentRequest.newBuilder().setOperationId("restrict")
                    .setTarget(DocumentTarget.newBuilder().setAddress(address("a", "b"))).setExpected(at(1))
                    .setDocument(replacement).addWrites(put(DocumentPart.DOCUMENT_PART_CORE)).setOwner(intakeOwner()).build());
            var next = lists.list(reader, request.toBuilder().setPageToken(first.getNextPageToken()).build());
            assertThat(next.getDocumentsCount()).isZero();
            assertThat(next.getNextPageToken()).isEmpty();
        }
    }

    @Test void lookupFieldsFiltersAndSortsComeFromTheCatalog() throws Exception {
        try (var f = new AccessionFixture()) {
            f.saves.save(processAuthority(), create("s-1", address("a", "d1"), described("a", "d1", "Annual report", "application/pdf", "en", "region", "emea")).build());
            f.saves.save(processAuthority(), create("s-2", address("a", "d2"), described("a", "d2", "Budget", "text/csv", "en", "region", "apac")).build());
            f.saves.save(processAuthority(), create("s-3", address("a", "d3"), described("a", "d3", "Charter", "application/pdf", "de", "region", "emea")).build());
            // A step owner needs a step writer: process authority saves only as intake.
            var credential = UUID.randomUUID();
            f.catalog.registerCredential(ISSUER, credential, "chunker");
            f.stepGrant("a", credential, "chunker", true);
            f.saves.save(writer("a", "chunker", credential, 1), create("s-4", address("a", "d4"),
                    described("a", "d4", "Draft", "text/plain", "fr", "stage", "draft")).setOwner(stepOwner("chunker")).build());
            f.stack.reset();
            var lists = new DocumentLists(f.catalog);
            var all = ListDocumentsRequest.newBuilder().setAccountId("a").setPageSize(10).build();

            var page = lists.list(processAuthority(), all);
            assertThat(page.getDocumentsList()).extracting(d -> d.getAddress().getDocId()).containsExactly("d1", "d2", "d3", "d4");
            var first = page.getDocuments(0);
            assertThat(first.getLookup().getTitle()).isEqualTo("Annual report");
            assertThat(first.getLookup().getSourceMimeType()).isEqualTo("application/pdf");
            assertThat(first.getLookup().getLanguage()).isEqualTo("en");
            assertThat(first.getLookup().getMetadataMap()).containsExactly(entry("region", "emea"));
            assertThat(first.getLookup().getDatasourceId()).isEqualTo("ds-1");
            assertThat(first.getLookup().hasAuthor()).isFalse();
            assertThat(first.getOwner()).isEqualTo(intakeOwner());
            assertThat(page.getDocuments(3).getOwner()).isEqualTo(stepOwner("chunker"));
            assertThat(DocumentSaves.VALIDATOR.validate(page).valid()).isTrue();
            assertThat(f.stack.opens.get()).as("listing reads no objects").isZero();

            assertThat(ids(lists, filtered(all, DocumentFilter.newBuilder().setTitle("Budget")))).containsExactly("d2");
            assertThat(ids(lists, filtered(all, DocumentFilter.newBuilder().setSourceMimeType("application/pdf")))).containsExactly("d1", "d3");
            assertThat(ids(lists, filtered(all, DocumentFilter.newBuilder().setSourceMimeType("application/pdf").setLanguage("de")))).containsExactly("d3");
            assertThat(ids(lists, filtered(all, DocumentFilter.newBuilder().addMetadata(pair("region", "emea"))))).containsExactly("d1", "d3");
            assertThat(ids(lists, filtered(all, DocumentFilter.newBuilder().addMetadata(pair("region", "emea")).addMetadata(pair("region", "emea"))))).containsExactly("d1", "d3");
            assertThat(ids(lists, filtered(all, DocumentFilter.newBuilder().addMetadata(pair("region", "nowhere"))))).isEmpty();
            assertThat(ids(lists, filtered(all, DocumentFilter.newBuilder().setOwnerModuleId("chunker")))).containsExactly("d4");
            assertThat(ids(lists, filtered(all, DocumentFilter.newBuilder().setOwnerDatasourceId("ds-1")))).containsExactly("d1", "d2", "d3");
            assertThat(ids(lists, filtered(all, DocumentFilter.newBuilder().setDatasourceId("ds-1").setDocId("d3")))).containsExactly("d3");
            var third = page.getDocuments(2).getCommittedAt();
            assertThat(ids(lists, filtered(all, DocumentFilter.newBuilder().setCommittedAfter(third)))).as("exact, inclusive").containsExactly("d3", "d4");
            assertThat(ids(lists, filtered(all, DocumentFilter.newBuilder().setCommittedBefore(third)))).as("exact, exclusive").containsExactly("d1", "d2");

            // Sorts: the field, then the address; a token resumes the same order.
            var newestSort = DocumentSort.newBuilder().setField(DocumentSortField.DOCUMENT_SORT_FIELD_COMMITTED_AT).setDescending(true).build();
            var newest = all.toBuilder().setSort(newestSort).build();
            assertThat(ids(lists, newest)).containsExactly("d4", "d3", "d2", "d1");
            assertThat(walk(lists, newest.toBuilder().setPageSize(1).build())).containsExactly("d4", "d3", "d2", "d1");
            var titleDescending = DocumentSort.newBuilder().setField(DocumentSortField.DOCUMENT_SORT_FIELD_TITLE).setDescending(true).build();
            assertThat(walk(lists, all.toBuilder().setPageSize(2).setSort(titleDescending).build())).containsExactly("d4", "d3", "d2", "d1");
            assertThat(walk(lists, all.toBuilder().setPageSize(2).setSort(titleDescending.toBuilder().setDescending(false)).build())).containsExactly("d1", "d2", "d3", "d4");

            // Refusals by name: contradictory metadata, a malformed media type, an empty range, a direction
            // without a field, a time outside the catalog's range, and a token from another listing.
            assertCode(Status.Code.INVALID_ARGUMENT, () -> lists.list(processAuthority(), filtered(all, DocumentFilter.newBuilder()
                    .addMetadata(pair("region", "emea")).addMetadata(pair("region", "apac")))));
            assertCode(Status.Code.INVALID_ARGUMENT, () -> lists.list(processAuthority(), filtered(all, DocumentFilter.newBuilder().setSourceMimeType("pdf"))));
            assertCode(Status.Code.INVALID_ARGUMENT, () -> lists.list(processAuthority(), filtered(all, DocumentFilter.newBuilder()
                    .setCommittedAfter(third).setCommittedBefore(third))));
            assertCode(Status.Code.INVALID_ARGUMENT, () -> lists.list(processAuthority(), all.toBuilder().setSort(DocumentSort.newBuilder().setDescending(true)).build()));
            assertCode(Status.Code.INVALID_ARGUMENT, () -> lists.list(processAuthority(), filtered(all, DocumentFilter.newBuilder()
                    .setCommittedAfter(com.google.protobuf.Timestamp.newBuilder().setSeconds(253402300799L)))));
            var tokened = lists.list(processAuthority(), newest.toBuilder().setPageSize(1).build());
            assertThat(tokened.getNextPageToken()).isNotEmpty();
            assertCode(Status.Code.INVALID_ARGUMENT, () -> lists.list(processAuthority(), all.toBuilder().setPageSize(1).setPageToken(tokened.getNextPageToken()).build()));
            assertCode(Status.Code.INVALID_ARGUMENT, () -> lists.list(processAuthority(), newest.toBuilder().setPageSize(1)
                    .setFilter(DocumentFilter.newBuilder().setLanguage("en")).setPageToken(tokened.getNextPageToken()).build()));
            assertThat(lists.list(processAuthority(), newest.toBuilder().setPageSize(1).setPageToken(tokened.getNextPageToken()).build()).getDocumentsList())
                    .extracting(d -> d.getAddress().getDocId()).containsExactly("d3");
            // An unknown field inside the filter is refused, not dropped.
            var widened = ListDocumentsRequest.parseFrom(all.toBuilder().setFilter(DocumentFilter.newBuilder().setUnknownFields(
                    com.google.protobuf.UnknownFieldSet.newBuilder().addField(99, com.google.protobuf.UnknownFieldSet.Field.newBuilder()
                            .addVarint(1).build()).build())).build().toByteArray());
            assertCode(Status.Code.INVALID_ARGUMENT, () -> lists.list(processAuthority(), widened));
        }
    }

    @Test void sortTiesBreakOnTheAddressAcrossPages() throws Exception {
        try (var f = new AccessionFixture()) {
            for (var id : List.of("d1", "d2", "d3", "d4", "d5"))
                f.saves.save(processAuthority(), create("s-" + id, address("a", id),
                        described("a", id, id.equals("d3") ? "Alpha" : "Same", "text/plain", "en", "k", "v")).build());
            var lists = new DocumentLists(f.catalog);
            var byTitle = ListDocumentsRequest.newBuilder().setAccountId("a").setPageSize(2)
                    .setSort(DocumentSort.newBuilder().setField(DocumentSortField.DOCUMENT_SORT_FIELD_TITLE)).build();
            assertThat(walk(lists, byTitle)).containsExactly("d3", "d1", "d2", "d4", "d5");
            assertThat(walk(lists, byTitle.toBuilder().setSort(byTitle.getSort().toBuilder().setDescending(true)).build()))
                    .containsExactly("d1", "d2", "d4", "d5", "d3");
            assertThat(walk(lists, byTitle.toBuilder().setPageSize(3).build())).containsExactly("d3", "d1", "d2", "d4", "d5");
            // A document without a title sorts as the empty string, first ascending and last descending.
            var untitled = document("a", "d0", "x", "reader").toBuilder();
            untitled.getSearchMetadataBuilder().clearTitle();
            f.saves.save(processAuthority(), create("s-d0", address("a", "d0"), untitled.build()).build());
            assertThat(walk(lists, byTitle)).containsExactly("d0", "d3", "d1", "d2", "d4", "d5");
            assertThat(walk(lists, byTitle.toBuilder().setSort(byTitle.getSort().toBuilder().setDescending(true)).build()))
                    .containsExactly("d1", "d2", "d4", "d5", "d3", "d0");
        }
    }

    @Test void filteredPagesNeverLeakRowsTheCallerCannotRead() throws Exception {
        try (var f = new AccessionFixture()) {
            f.saves.save(processAuthority(), create("s-1", address("a", "d1"), described("a", "d1", "Secret", "application/pdf", "en", "k", "v", "reader")).build());
            f.saves.save(processAuthority(), create("s-2", address("a", "d2"), described("a", "d2", "Secret", "application/pdf", "en", "k", "v", "other")).build());
            f.saves.save(processAuthority(), create("s-3", address("a", "d3"), described("a", "d3", "Secret", "application/pdf", "en", "k", "v", "reader")).build());
            f.saves.save(processAuthority(), create("s-4", address("a", "d4"), described("a", "d4", "Secret", "application/pdf", "en", "k", "v", "other")).build());
            var reader = new RepositoryCaller("reader", false, Set.of("a"),
                    Set.of(Principal.newBuilder().setIdentityType("service").setIdentity("reader").build()));
            var lists = new DocumentLists(f.catalog);
            var secret = ListDocumentsRequest.newBuilder().setAccountId("a").setPageSize(1)
                    .setFilter(DocumentFilter.newBuilder().setTitle("Secret")).build();
            var page = lists.list(reader, secret);
            assertThat(page.getDocumentsList()).extracting(d -> d.getAddress().getDocId()).containsExactly("d1");
            assertThat(page.getNextPageToken()).isNotEmpty();
            var token = CatalogPageToken.parseFrom(Base64.getUrlDecoder().decode(page.getNextPageToken()));
            assertThat(token.getAfter().getDocId()).as("a token names only a returned row").isEqualTo("d1");
            var rest = lists.list(reader, secret.toBuilder().setPageToken(page.getNextPageToken()).build());
            assertThat(rest.getDocumentsList()).extracting(d -> d.getAddress().getDocId()).containsExactly("d3");
            assertThat(rest.getNextPageToken()).as("the denied tail yields no token").isEmpty();
            // A filter that matches only denied rows returns nothing, with no token and no count.
            var none = lists.list(reader, filtered(secret, DocumentFilter.newBuilder().addMetadata(pair("k", "v")).setDocId("d2")));
            assertThat(none.getDocumentsCount()).isZero();
            assertThat(none.getNextPageToken()).isEmpty();
            assertThat(lists.list(processAuthority(), secret.toBuilder().setPageSize(10).build()).getDocumentsCount()).isEqualTo(4);
        }
    }

    @Test void lookupFollowsTheCorePartAndClearsOnTombstoneAndPurge() throws Exception {
        try (var f = new AccessionFixture()) {
            var address = address("a", "d1");
            f.saves.save(processAuthority(), create("s-1", address, described("a", "d1", "First", "text/plain", "en", "k", "v")).build());
            var parsed = Document.newBuilder().putParserResults("docling", ParserResult.newBuilder()
                    .setParserName("docling").setParserVersion("2").setStatus(ParseStatus.PARSE_STATUS_OK).build()).build();
            f.saves.save(processAuthority(), SaveDocumentRequest.newBuilder().setOperationId("s-2")
                    .setTarget(DocumentTarget.newBuilder().setAddress(address)).setExpected(at(1)).setDocument(parsed)
                    .addWrites(put(DocumentPart.DOCUMENT_PART_PARSED)).setOwner(intakeOwner()).build());
            var lists = new DocumentLists(f.catalog);
            var request = ListDocumentsRequest.newBuilder().setAccountId("a").setPageSize(10).setIncludeTombstoned(true).build();
            var carried = lists.list(processAuthority(), request).getDocuments(0);
            assertThat(carried.getRevision()).isEqualTo(2);
            assertThat(carried.getLookup().getTitle()).as("a PARSED write carries the CORE lookup").isEqualTo("First");
            assertThat(ids(lists, filtered(request, DocumentFilter.newBuilder().addMetadata(pair("k", "v"))))).containsExactly("d1");

            var renamed = described("a", "d1", "Second", "text/plain", "en", "k2", "v2").toBuilder().clearBlobBag().clearParserResults().build();
            f.saves.save(processAuthority(), SaveDocumentRequest.newBuilder().setOperationId("s-3")
                    .setTarget(DocumentTarget.newBuilder().setAddress(address)).setExpected(at(2)).setDocument(renamed)
                    .addWrites(put(DocumentPart.DOCUMENT_PART_CORE)).setOwner(intakeOwner()).build());
            assertThat(ids(lists, filtered(request, DocumentFilter.newBuilder().setTitle("Second")))).containsExactly("d1");
            assertThat(ids(lists, filtered(request, DocumentFilter.newBuilder().setTitle("First")))).isEmpty();
            assertThat(ids(lists, filtered(request, DocumentFilter.newBuilder().addMetadata(pair("k", "v"))))).isEmpty();

            new DocumentDeletes(f.catalog, Clock.systemUTC()).delete(processAuthority(), DeleteDocumentRequest.newBuilder()
                    .setAddress(address).setExpected(at(3)).setOperationId("delete").build());
            var tombstone = lists.list(processAuthority(), request).getDocuments(0);
            assertThat(tombstone.getTombstone()).isTrue();
            assertThat(tombstone.hasLookup()).isFalse();
            assertThat(tombstone.hasOwner()).isFalse();
            assertThat(ids(lists, filtered(request, DocumentFilter.newBuilder().setTitle("Second")))).isEmpty();
            assertThat(ids(lists, filtered(request, DocumentFilter.newBuilder().addMetadata(pair("k2", "v2"))))).isEmpty();
            assertThat(metadataRows(f)).isZero();

            // A purge retires the lookup with the content; identity and ownership stay in the entry.
            var purged = address("a", "d2");
            f.saves.save(processAuthority(), create("s-5", purged, described("a", "d2", "Gone", "text/plain", "en", "k3", "v3")).build());
            DocumentPurgesIT.purges(f).purge(DocumentPurgesIT.administrator(), DocumentPurgesIT.request("purge", purged));
            try (var connection = DriverManager.getConnection(f.url, POSTGRES.getUsername(), POSTGRES.getPassword());
                 var rows = connection.createStatement().executeQuery(
                         "SELECT lookup_title, catalog_entry, catalog_entry_sha256 FROM catalog_documents WHERE doc_id = 'd2'")) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getString(1)).isNull();
                var bytes = rows.getBytes(2);
                assertThat(sha256(bytes)).isEqualTo(rows.getString(3));
                var entry = CatalogEntry.parseFrom(bytes);
                assertThat(entry.hasLookup()).isFalse();
                assertThat(entry.getOwnership()).isEqualTo(ownership("a", "reader"));
                assertThat(entry.getOwner()).isEqualTo(intakeOwner());
            }
            assertThat(metadataRows(f)).isZero();
        }
    }

    @Test void lookupStringsTheCatalogCannotStoreAreRefusedByField() throws Exception {
        try (var f = new AccessionFixture()) {
            var longTitle = described("a", "d1", "t".repeat(2001), "text/plain", "en", "k", "v");
            var longValue = described("a", "d2", "Long value", "text/plain", "en", "k", "v".repeat(3000));
            var longKey = described("a", "d3", "Long key", "text/plain", "en", "k".repeat(201), "v");
            var nul = described("a", "d4", "nul\u0000", "text/plain", "en", "k", "v");
            f.saves.save(processAuthority(), create("s-2", address("a", "d2"), longValue).build());
            for (var refused : List.of(longTitle, longKey, nul)) {
                var error = catchThrowableOfType(() -> f.saves.save(processAuthority(),
                        create("s-" + refused.getDocId(), address("a", refused.getDocId()), refused).build()), AccessionException.class);
                assertThat(error.code()).isEqualTo(Status.Code.INVALID_ARGUMENT);
                var rejection = (SaveRejection) error.detail().orElseThrow();
                assertThat(rejection.getReason()).isEqualTo(RejectionReason.REJECTION_REASON_VALIDATION_FAILED);
                assertThat(rejection.getViolationsList()).extracting(FieldViolation::getRuleId).containsOnly("catalog-lookup-storable");
                assertThat(rejection.getViolationsList()).extracting(FieldViolation::getFieldPath).allMatch(path -> path.startsWith("search_metadata."));
            }
            var lists = new DocumentLists(f.catalog);
            var all = ListDocumentsRequest.newBuilder().setAccountId("a").setPageSize(10).build();
            assertThat(ids(lists, all)).containsExactly("d2");
            assertThat(ids(lists, filtered(all, DocumentFilter.newBuilder().addMetadata(pair("k", "v".repeat(3000)))))).containsExactly("d2");
        }
    }

    @Test void entriesRebuildFromStoredParts() throws Exception {
        try (var f = new AccessionFixture()) {
            f.saves.save(processAuthority(), create("s-1", address("a", "d1"), described("a", "d1", "Kept", "text/plain", "en", "k", "v")).build());
            f.saves.save(processAuthority(), create("s-2", address("a", "d2"), described("a", "d2", "Gone", "text/plain", "en", "k", "v")).build());
            new DocumentDeletes(f.catalog, Clock.systemUTC()).delete(processAuthority(), DeleteDocumentRequest.newBuilder()
                    .setAddress(address("a", "d2")).setExpected(at(1)).setOperationId("delete").build());
            var lists = new DocumentLists(f.catalog);
            var request = ListDocumentsRequest.newBuilder().setAccountId("a").setPageSize(10).setIncludeTombstoned(true).build();
            var before = lists.list(processAuthority(), request);

            // Damage the derived columns and rows, and strip the lookup from the entry itself (a valid
            // entry with a correct digest, as a catalog from before the lookup would hold).
            try (var connection = DriverManager.getConnection(f.url, POSTGRES.getUsername(), POSTGRES.getPassword())) {
                connection.createStatement().execute("UPDATE catalog_documents SET lookup_title = 'Wrong', lookup_source_mime_type = NULL, owner_datasource_id = NULL");
                connection.createStatement().execute("DELETE FROM catalog_document_metadata");
                try (var rows = connection.createStatement().executeQuery("SELECT document_id, catalog_entry FROM catalog_documents WHERE doc_id = 'd1'")) {
                    assertThat(rows.next()).isTrue();
                    long documentId = rows.getLong(1);
                    var stripped = CatalogEntry.parseFrom(rows.getBytes(2)).toBuilder().clearLookup().build().toByteArray();
                    try (var update = connection.prepareStatement("UPDATE catalog_documents SET catalog_entry = ?, catalog_entry_sha256 = ? WHERE document_id = ?")) {
                        update.setBytes(1, stripped); update.setString(2, sha256(stripped)); update.setLong(3, documentId);
                        update.executeUpdate();
                    }
                }
            }
            assertCode(Status.Code.DATA_LOSS, () -> lists.list(processAuthority(), request));
            assertThat(ids(lists, filtered(request, DocumentFilter.newBuilder().setTitle("Kept")))).isEmpty();

            f.stack.reset();
            assertThat(new CatalogEntryRebuild(f.catalog, f.reads).rebuild("a")).isEqualTo(2);
            assertThat(f.stack.opens.get()).as("one CORE read per live document").isEqualTo(1);
            assertThat(ids(lists, filtered(request, DocumentFilter.newBuilder().setTitle("Kept")))).containsExactly("d1");
            assertThat(ids(lists, filtered(request, DocumentFilter.newBuilder().setTitle("Wrong")))).isEmpty();
            assertThat(ids(lists, filtered(request, DocumentFilter.newBuilder().setSourceMimeType("text/plain")))).containsExactly("d1");
            assertThat(ids(lists, filtered(request, DocumentFilter.newBuilder().setOwnerDatasourceId("ds-1")))).containsExactly("d1");
            assertThat(ids(lists, filtered(request, DocumentFilter.newBuilder().addMetadata(pair("k", "v"))))).containsExactly("d1");
            assertThat(lists.list(processAuthority(), request)).isEqualTo(before);
        }
    }

    /** A document whose CORE carries lookup fields; {@code writers} default to "reader". */
    private static Document described(String account, String doc, String title, String mimeType, String language,
            String metadataKey, String metadataValue, String... writers) {
        var base = document(account, doc, title, writers.length == 0 ? new String[] {"reader"} : writers);
        return base.toBuilder().setSearchMetadata(base.getSearchMetadata().toBuilder().setSourceMimeType(mimeType)
                .setLanguage(language).putMetadata(metadataKey, metadataValue)).build();
    }

    private static MetadataMatch pair(String key, String value) {
        return MetadataMatch.newBuilder().setKey(key).setValue(value).build();
    }

    private static ListDocumentsRequest filtered(ListDocumentsRequest request, DocumentFilter.Builder filter) {
        return request.toBuilder().setFilter(filter).build();
    }

    private static List<String> ids(DocumentLists lists, ListDocumentsRequest request) {
        return lists.list(processAuthority(), request).getDocumentsList().stream().map(d -> d.getAddress().getDocId()).toList();
    }

    /** Every document, following tokens to the last page. */
    private static List<String> walk(DocumentLists lists, ListDocumentsRequest request) {
        var seen = new ArrayList<String>();
        var page = request.toBuilder().clearPageToken();
        do {
            var response = lists.list(processAuthority(), page.build());
            response.getDocumentsList().forEach(d -> seen.add(d.getAddress().getDocId()));
            page.setPageToken(response.getNextPageToken());
        } while (!page.getPageToken().isEmpty());
        return seen;
    }

    private static int metadataRows(AccessionFixture f) throws Exception {
        try (var connection = DriverManager.getConnection(f.url, POSTGRES.getUsername(), POSTGRES.getPassword());
             var rows = connection.createStatement().executeQuery("SELECT count(*) FROM catalog_document_metadata")) {
            rows.next();
            return rows.getInt(1);
        }
    }

    private static String sha256(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(bytes));
    }
}
