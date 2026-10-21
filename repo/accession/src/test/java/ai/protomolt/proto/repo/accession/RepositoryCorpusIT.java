package ai.protomolt.proto.repo.accession;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ai.protomolt.proto.repo.v1.NodeAddress;
import ai.protomolt.proto.repo.v2.DocumentFilter;
import ai.protomolt.proto.repo.v2.DocumentSort;
import ai.protomolt.proto.repo.v2.DocumentSortField;
import ai.protomolt.proto.repo.v2.DocumentSummary;
import ai.protomolt.proto.repo.v2.GetDocumentRequest;
import ai.protomolt.proto.repo.v2.ListDocumentsRequest;
import ai.protomolt.proto.repo.v2.ListRevisionsRequest;
import ai.protomolt.proto.repo.v2.PartSelection;
import ai.protomolt.proto.repo.v2.PartSelector;
import ai.protomolt.proto.repo.v2.ReadBlobRequest;
import ai.protomolt.proto.repo.v2.ReadBlobResponse;
import ai.protomolt.proto.repo.v2.RevisionSelector;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * The corpus loaded through the real save path into PostgreSQL and S3: two
 * fixtures load the same corpus and end in the same catalog state and object
 * digests, commit times come from the stepped clock, and what the console and
 * the catalog tests rely on holds: lookup fields, filters, sorts, tombstones,
 * account scoping, ACL filtering, revision history, and a ranged read of the
 * large blob. An integration suite: it runs in {@code integrationTest}, not {@code test}.
 */
class RepositoryCorpusIT {
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    static final LocalStackContainer S3 = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
            .withServices("s3");

    static V2RepositoryFixture first;
    static V2RepositoryFixture second;
    static RepositoryCorpus corpus;
    static CorpusLoader.Loaded loaded;
    static CorpusLoader.Loaded loadedSecond;

    @BeforeAll static void load() throws Exception {
        POSTGRES.start();
        S3.start();
        corpus = RepositoryCorpus.build();
        first = V2RepositoryFixture.builder(POSTGRES, S3, "corpus-first").isolatedDatabase().build();
        second = V2RepositoryFixture.builder(POSTGRES, S3, "corpus-second").isolatedDatabase().build();
        loaded = first.loadCorpus(corpus);
        loadedSecond = second.loadCorpus(corpus);
    }

    @AfterAll static void close() {
        if (first != null) first.close();
        if (second != null) second.close();
        S3.stop();
        POSTGRES.stop();
    }

    @Test void twoLoadsLeaveTheSameCatalogStateAndObjectDigests() {
        var firstState = CorpusLoader.normalizedState(first.catalog(), corpus);
        var secondState = CorpusLoader.normalizedState(second.catalog(), corpus);
        assertThat(firstState).isNotEmpty();
        assertThat(secondState).isEqualTo(firstState);
        int expectedSaves = 0;
        int expectedDeletes = 0;
        for (var script : corpus.documents()) {
            for (var step : script.steps()) {
                if (step instanceof RepositoryCorpus.Delete) expectedDeletes++;
                else expectedSaves++;
            }
        }
        assertThat(loaded.saves()).isEqualTo(expectedSaves);
        assertThat(loaded.deletes()).isEqualTo(expectedDeletes);
    }

    @Test void commitTimesComeFromTheSteppedClockAndSpreadAcrossDays() {
        // The save path reads the clock more than once per commit; the first commit lands a few steps in.
        assertThat(loaded.firstCommit()).isBetween(RepositoryCorpus.EPOCH, RepositoryCorpus.EPOCH.plus(RepositoryCorpus.STEP.multipliedBy(3)));
        assertThat(loaded.lastCommit()).isAfter(RepositoryCorpus.EPOCH.plus(java.time.Duration.ofDays(3)));
        var days = new HashSet<String>();
        for (var account : RepositoryCorpus.ACCOUNTS) {
            for (var row : CorpusLoader.listAll(first.catalog(), account)) {
                var committed = Instant.ofEpochSecond(row.committedAt().getSeconds(), row.committedAt().getNanos());
                days.add(committed.toString().substring(0, 10));
            }
        }
        assertThat(days.size()).isGreaterThan(3);
        assertThat(loadedSecond.firstCommit()).as("a fresh clock restarts at the epoch").isEqualTo(loaded.firstCommit());
        assertThat(loadedSecond.lastCommit()).isEqualTo(loaded.lastCommit());
        // Another seed writes the same addresses with other operations: into a loaded store that is a stale save, not a replay.
        var reseeded = RepositoryCorpus.build(new RepositoryCorpus.Parameters(99L, 1024));
        assertThatThrownBy(() -> second.loadCorpus(reseeded)).isInstanceOfSatisfying(AccessionException.class,
                failure -> assertThat(failure.code()).isEqualTo(Status.Code.ABORTED));
    }

    @Test void loadingTheSameCorpusTwiceIntoOneStoreIsRefusedAsAReplay() {
        assertThatThrownBy(() -> first.loadCorpus(corpus)).isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("replayed");
    }

    @Test void lookupFieldsFiltersAndSortsAreNonUniform() {
        var lists = new DocumentLists(first.catalog());
        var alpha = RepositoryCorpus.ACCOUNTS.get(0);
        var all = ListDocumentsRequest.newBuilder().setAccountId(alpha).setPageSize(50).build();
        var page = lists.list(CorpusLoader.processAuthority(), all);
        // Address order: graph address, then document ID.
        var expected = corpus.documents(alpha).stream().filter(script -> !script.tombstoned())
                .map(RepositoryCorpus.DocumentScript::address)
                .sorted(java.util.Comparator.comparing(NodeAddress::getGraphAddressId).thenComparing(NodeAddress::getDocId))
                .map(NodeAddress::getDocId).toList();
        assertThat(page.getDocumentsList()).extracting(summary -> summary.getAddress().getDocId()).containsExactlyElementsOf(expected);
        assertThat(page.getDocumentsList()).extracting(summary -> summary.getLookup().hasAuthor()).contains(true, false);
        assertThat(page.getDocumentsList()).extracting(summary -> summary.getLookup().getLanguage()).contains("en", "de", "fr", "ja");
        var report = page.getDocumentsList().stream().filter(summary -> summary.getAddress().getDocId().equals("annual-report-2025"))
                .findFirst().orElseThrow();
        assertThat(report.getRevision()).isEqualTo(3);
        assertThat(report.getLookup().getTitle()).as("the lookup follows the current revision").isEqualTo("Annual report, final");
        assertThat(report.getLookup().getMetadataMap()).containsEntry("status", "final").containsEntry("region", "DE");
        assertThat(report.getLookup().getConnectorId()).isEqualTo("file-crawler");
        assertThat(report.getOwner().getIntake().getDatasourceId()).isEqualTo("ds-crawler");

        var pdf = lists.list(CorpusLoader.processAuthority(), all.toBuilder()
                .setFilter(DocumentFilter.newBuilder().setSourceMimeType("application/pdf")).build());
        assertThat(pdf.getDocumentsList()).extracting(summary -> summary.getAddress().getDocId())
                .containsExactly("annual-report-2025", "anthologie");
        var finance = lists.list(CorpusLoader.processAuthority(), all.toBuilder()
                .setFilter(DocumentFilter.newBuilder().setCategory("finance")).build());
        assertThat(finance.getDocumentsList()).extracting(summary -> summary.getAddress().getDocId())
                .containsExactly("annual-report-2025", "ledger-restricted", "budget-q1");
        var byTitle = lists.list(CorpusLoader.processAuthority(), all.toBuilder()
                .setSort(DocumentSort.newBuilder().setField(DocumentSortField.DOCUMENT_SORT_FIELD_TITLE)).build());
        var titles = byTitle.getDocumentsList().stream().map(summary -> summary.getLookup().getTitle()).toList();
        assertThat(titles).isSorted();
        assertThat(new HashSet<>(titles)).hasSize(titles.size());

        var beta = RepositoryCorpus.ACCOUNTS.get(1);
        var tie = lists.list(CorpusLoader.processAuthority(), ListDocumentsRequest.newBuilder().setAccountId(beta).setPageSize(50)
                .setSort(DocumentSort.newBuilder().setField(DocumentSortField.DOCUMENT_SORT_FIELD_TITLE)).build());
        var quarterlies = tie.getDocumentsList().stream().filter(summary -> summary.getLookup().getTitle().equals("Quarterly update"))
                .map(summary -> summary.getAddress().getDocId()).toList();
        assertThat(quarterlies).as("a title tie breaks on the address").containsExactly("quarterly-q1", "quarterly-q2");
        var tombstones = lists.list(CorpusLoader.processAuthority(), ListDocumentsRequest.newBuilder().setAccountId(beta)
                .setPageSize(50).setIncludeTombstoned(true).build());
        assertThat(tombstones.getDocumentsList()).filteredOn(DocumentSummary::getTombstone)
                .extracting(summary -> summary.getAddress().getDocId()).containsExactly("draft-memo");
    }

    @Test void accountScopingAndAclFilterTheListing() {
        var lists = new DocumentLists(first.catalog());
        var alpha = RepositoryCorpus.ACCOUNTS.get(0);
        var reader = CorpusLoader.reader(alpha);
        var visible = lists.list(reader, ListDocumentsRequest.newBuilder().setAccountId(alpha).setPageSize(50).build());
        assertThat(visible.getDocumentsList()).extracting(summary -> summary.getAddress().getDocId())
                .doesNotContain("ledger-restricted").contains("annual-report-2025");
        var finance = lists.list(reader, ListDocumentsRequest.newBuilder().setAccountId(alpha).setPageSize(50)
                .setFilter(DocumentFilter.newBuilder().setCategory("finance")).build());
        assertThat(finance.getDocumentsList()).as("the ACL applies after the filter")
                .extracting(summary -> summary.getAddress().getDocId()).containsExactly("annual-report-2025", "budget-q1");
        var auditor = CorpusLoader.user(alpha, RepositoryCorpus.AUDITOR_UPN);
        assertThat(lists.list(auditor, ListDocumentsRequest.newBuilder().setAccountId(alpha).setPageSize(50).build()).getDocumentsList())
                .extracting(summary -> summary.getAddress().getDocId()).containsExactly("ledger-restricted");
        var beta = RepositoryCorpus.ACCOUNTS.get(1);
        assertThatThrownBy(() -> lists.list(reader, ListDocumentsRequest.newBuilder().setAccountId(beta).setPageSize(50).build()))
                .isInstanceOfSatisfying(AccessionException.class,
                        failure -> assertThat(failure.code()).isEqualTo(Status.Code.PERMISSION_DENIED));
        for (var account : RepositoryCorpus.ACCOUNTS) {
            var rows = CorpusLoader.listAll(first.catalog(), account);
            assertThat(rows).extracting(row -> row.address().getAccountId()).containsOnly(account);
            assertThat(rows).hasSize(corpus.documents(account).size());
        }
    }

    @Test void revisionHistoriesAndPartsReadBackThroughTheRealReads() {
        var alpha = RepositoryCorpus.ACCOUNTS.get(0);
        var charter = address(alpha, "charter");
        var revisions = first.reads().listRevisions(CorpusLoader.processAuthority(),
                ListRevisionsRequest.newBuilder().setAddress(charter).setPageSize(10).build());
        assertThat(revisions.getRevisionsList()).hasSize(3);
        assertThat(revisions.getRevisionsList()).extracting(revision -> revision.getOwner().hasStep()).contains(true, false);
        var current = first.reads().getDocument(CorpusLoader.processAuthority(),
                GetDocumentRequest.newBuilder().setAddress(charter).setSelection(PartSelection.newBuilder()
                        .addSelectors(PartSelector.newBuilder().setPart(ai.protomolt.proto.repo.v2.DocumentPart.DOCUMENT_PART_CORE))
                        .setAllSegmentations(true).setAllEmbeddingSets(true)).build());
        assertThat(current.getDocument().getSearchMetadata().getSegmentationsCount()).isEqualTo(2);
        assertThat(current.getDocument().getSearchMetadata().getEmbeddingSetsCount()).isEqualTo(3);
        assertThat(current.getManifest().getRevision()).isEqualTo(3);

        var earlier = first.reads().getDocument(CorpusLoader.processAuthority(), GetDocumentRequest.newBuilder()
                .setAddress(address(alpha, "product-sheet")).setRevision(RevisionSelector.newBuilder().setRevision(1)).build());
        var later = first.reads().getDocument(CorpusLoader.processAuthority(), GetDocumentRequest.newBuilder()
                .setAddress(address(alpha, "product-sheet")).build());
        assertThat(earlier.getValidation().getResolvedOccurrencesCount()).isZero();
        assertThat(later.getValidation().getResolvedOccurrencesCount()).isEqualTo(1);
        assertThat(later.getValidation().getResolvedOccurrences(0).getSchema().getTypeUrl())
                .isEqualTo(RepositoryCorpus.structuredDataTypeUrl());

        var deleted = address(alpha, "site-photo-0007");
        assertThatThrownBy(() -> first.reads().getDocument(CorpusLoader.reader(alpha),
                GetDocumentRequest.newBuilder().setAddress(deleted).build()))
                .isInstanceOfSatisfying(AccessionException.class,
                        failure -> assertThat(failure.code()).isIn(Status.Code.NOT_FOUND, Status.Code.FAILED_PRECONDITION));

        var anthology = first.reads().getDocument(CorpusLoader.processAuthority(), GetDocumentRequest.newBuilder()
                .setAddress(address(alpha, "anthologie")).build());
        assertThat(anthology.getDocument().getBlobBag().getBlobs().getBlobCount()).isEqualTo(4);
        var manifest = first.catalog().manifest(address(alpha, "anthologie"), 1).orElseThrow();
        for (var blob : anthology.getDocument().getBlobBag().getBlobs().getBlobList()) {
            var ref = manifest.getPartsList().stream().flatMap(part -> part.getBlobRefsList().stream())
                    .filter(candidate -> candidate.getBlobId().equals(blob.getBlobId())).findFirst().orElseThrow().getRef();
            byte[] bytes;
            try (var stream = first.pipelineStack.open(ref)) {
                bytes = stream.readAllBytes();
            } catch (java.io.IOException failure) {
                throw new AssertionError(failure);
            }
            assertThat(SyntheticBytes.sha256(bytes)).as(blob.getBlobId())
                    .isEqualTo(corpus.blobDigests().get(alpha + "/anthologie/" + blob.getBlobId()));
        }
    }

    @Test void theLargeBlobStreamsBackInRanges() throws Exception {
        var alpha = RepositoryCorpus.ACCOUNTS.get(0);
        var manual = address(alpha, "service-manual");
        var length = RepositoryCorpus.DEFAULT_LARGE_BLOB_BYTES;
        var whole = readBlob(manual, 0, 0);
        assertThat(whole.size()).isEqualTo(length);
        assertThat(SyntheticBytes.sha256(whole.toByteArray())).isEqualTo(corpus.blobDigests().get(alpha + "/service-manual/original"));
        long offset = 1_000_003L;
        int window = 64 * 1024 + 9;
        var range = readBlob(manual, offset, window);
        assertThat(range).isEqualTo(whole.substring((int) offset, (int) offset + window));
        var tail = readBlob(manual, length - 5, 5);
        assertThat(tail).isEqualTo(whole.substring((int) length - 5));
    }

    private static ByteString readBlob(NodeAddress address, long offset, long length) throws Exception {
        var done = new CompletableFuture<ByteString>();
        var out = new ByteArrayOutputStream();
        var frames = new ArrayList<ReadBlobResponse>();
        first.reads().readBlob(CorpusLoader.processAuthority(), ReadBlobRequest.newBuilder().setAddress(address)
                .setBlobId("original").setOffset(offset).setLength(length).build(), new StreamObserver<>() {
                    @Override public void onNext(ReadBlobResponse response) {
                        frames.add(response);
                        if (response.hasData()) out.writeBytes(response.getData().toByteArray());
                    }
                    @Override public void onError(Throwable failure) { done.completeExceptionally(failure); }
                    @Override public void onCompleted() { done.complete(ByteString.copyFrom(out.toByteArray())); }
                });
        var bytes = done.get();
        assertThat(frames.getFirst().hasHeader()).isTrue();
        assertThat(frames.getFirst().getHeader().getMimeType()).isEqualTo("application/octet-stream");
        return bytes;
    }

    private static NodeAddress address(String account, String docId) {
        return corpus.documents(account).stream().filter(script -> script.address().getDocId().equals(docId))
                .findFirst().orElseThrow().address();
    }

    static List<String> ids(List<DocumentSummary> summaries) {
        return summaries.stream().map(summary -> summary.getAddress().getDocId()).toList();
    }
}
