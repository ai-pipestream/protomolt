package ai.pipestream.proto.repo.service;

import ai.pipestream.proto.repo.v1.Access;
import ai.pipestream.proto.repo.v1.AccessRule;
import ai.pipestream.proto.repo.v1.Blob;
import ai.pipestream.proto.repo.v1.BlobBag;
import ai.pipestream.proto.repo.v1.CreateDriveRequest;
import ai.pipestream.proto.repo.v1.DeleteDocumentByReferenceCommand;
import ai.pipestream.proto.repo.v1.DeleteDocumentOutcome;
import ai.pipestream.proto.repo.v1.DeleteDocumentRequest;
import ai.pipestream.proto.repo.v1.DeleteDocumentResponse;
import ai.pipestream.proto.repo.v1.DeleteLogicalDocumentCommand;
import ai.pipestream.proto.repo.v1.Document;
import ai.pipestream.proto.repo.v1.DocumentManifest;
import ai.pipestream.proto.repo.v1.DocumentPart;
import ai.pipestream.proto.repo.v1.DocumentReference;
import ai.pipestream.proto.repo.v1.DocumentSecurity;
import ai.pipestream.proto.repo.v1.DocumentServiceGrpc;
import ai.pipestream.proto.repo.v1.Drive;
import ai.pipestream.proto.repo.v1.DriveServiceGrpc;
import ai.pipestream.proto.repo.v1.DriveType;
import ai.pipestream.proto.repo.v1.FileStorageReference;
import ai.pipestream.proto.repo.v1.GetBlobRequest;
import ai.pipestream.proto.repo.v1.GetDocumentByReferenceRequest;
import ai.pipestream.proto.repo.v1.GetDocumentManifestRequest;
import ai.pipestream.proto.repo.v1.GetDocumentManifestResponse;
import ai.pipestream.proto.repo.v1.GetDocumentRequest;
import ai.pipestream.proto.repo.v1.GetDocumentResponse;
import ai.pipestream.proto.repo.v1.GetDriveRequest;
import ai.pipestream.proto.repo.v1.ListDocumentsRequest;
import ai.pipestream.proto.repo.v1.ListDocumentsResponse;
import ai.pipestream.proto.repo.v1.ListDrivesRequest;
import ai.pipestream.proto.repo.v1.OwnershipContext;
import ai.pipestream.proto.repo.v1.ParsedMetadata;
import ai.pipestream.proto.repo.v1.PartManifestEntry;
import ai.pipestream.proto.repo.v1.PartState;
import ai.pipestream.proto.repo.v1.SaveDocumentRequest;
import ai.pipestream.proto.repo.v1.SaveDocumentResponse;
import ai.pipestream.proto.repo.v1.SearchMetadata;
import ai.pipestream.proto.repo.v1.SemanticChunk;
import ai.pipestream.proto.repo.v1.SemanticProcessingResult;
import ai.pipestream.proto.repo.v1.WriteProvenance;
import ai.pipestream.proto.repo.container.blob.BlobStore;
import ai.pipestream.proto.repo.container.codec.DocumentPartCodec;
import ai.pipestream.proto.repo.container.ledger.DocumentRecord;
import ai.pipestream.proto.repo.container.ledger.DocumentStatus;
import ai.pipestream.proto.repo.container.ledger.LedgerConfig;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.StringValue;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end integration test of repo-service against REAL infrastructure:
 * shared testcontainers PostgreSQL 17 (Flyway-migrated ledger) and LocalStack
 * S3 (part objects), with the full service stack booted through
 * {@link RepoServiceConfig} + {@link RepoServices} over the gRPC in-process
 * transport — proving the same-JVM embedding path, with no mocks anywhere.
 */
@Testcontainers(disabledWithoutDocker = true)
class RepoServiceIT {

    private static final String CONNECTOR = "connector-1";

    @Container
    static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:17-alpine");

    @Container
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8"))
                    .withServices("s3");

    static RepoServices services;
    static ManagedChannel channel;
    static DocumentServiceGrpc.DocumentServiceBlockingStub documents;
    static DriveServiceGrpc.DriveServiceBlockingStub drives;

    @BeforeAll
    static void boot() throws Exception {
        RepoServiceConfig config = new RepoServiceConfig(
                0, // unused on the in-process transport
                new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                LOCALSTACK.getEndpoint().toString(),
                LOCALSTACK.getRegion(),
                LOCALSTACK.getAccessKey(),
                LOCALSTACK.getSecretKey(),
                "it-docs");
        services = RepoServices.build(config);
        services.startInProcess("it");
        channel = InProcessChannelBuilder.forName("it").build();
        documents = DocumentServiceGrpc.newBlockingStub(channel);
        drives = DriveServiceGrpc.newBlockingStub(channel);
    }

    @AfterAll
    static void tearDown() {
        channel.shutdownNow();
        services.close();
    }

    // ------------------------------------------------------------- fixtures

    /** A fully-populated document: blob bag, parsed metadata, two chunk sets. */
    private static Document fixture(String docId, String accountId, String datasourceId) {
        return Document.newBuilder()
                .setDocId(docId)
                .setOwnership(OwnershipContext.newBuilder()
                        .setAccountId(accountId)
                        .setDatasourceId(datasourceId)
                        .setConnectorId(CONNECTOR)
                        .setSecurity(DocumentSecurity.newBuilder()
                                .setInheritanceEnabled(true)
                                .addPermissions(AccessRule.newBuilder()
                                        .setIdentity("alice")
                                        .setIdentityType("user-principal-name")
                                        .setDisplayName("Alice")
                                        .setAccess(Access.ACCESS_READ))))
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .setTitle("Quarterly Report")
                        .setBody("the full text body of the document")
                        .setDocumentType("PDF")
                        .setSourceUri("s3://source/" + docId)
                        .addSemanticResults(chunkSet("chunks-a", 2))
                        .addSemanticResults(chunkSet("chunks-b", 1)))
                .setBlobBag(BlobBag.newBuilder().setBlob(Blob.newBuilder()
                        .setBlobId("blob-1")
                        .setData(ByteString.copyFromUtf8("raw-pdf-bytes-of-" + docId))
                        .setMimeType("application/pdf")
                        .setFilename("report.pdf")))
                .putParsedMetadata("tika", ParsedMetadata.newBuilder()
                        .setParserName("tika")
                        .setData(Any.pack(StringValue.of("tika-exhaust")))
                        .build())
                .build();
    }

    private static SemanticProcessingResult chunkSet(String resultId, int chunkCount) {
        SemanticProcessingResult.Builder set = SemanticProcessingResult.newBuilder()
                .setResultId(resultId)
                .setChunkerConfigId("chunker-v1");
        for (int i = 0; i < chunkCount; i++) {
            set.addChunks(SemanticChunk.newBuilder()
                    .setChunkId(resultId + "-" + i)
                    .setChunkNumber(i)
                    .setText("chunk text " + i + " of " + resultId));
        }
        return set.build();
    }

    private static SaveDocumentRequest.Builder intakeSave(Document doc, String drive, String accountId) {
        return SaveDocumentRequest.newBuilder()
                .setDocument(doc)
                .setDrive(drive)
                .setConnectorId(CONNECTOR)
                .setUseDatasourceId(true)
                .setGraphId("intake:" + accountId);
    }

    private static DocumentReference reference(String docId, String graphAddressId,
            String accountId, String graphId) {
        return DocumentReference.newBuilder()
                .setDocId(docId)
                .setGraphAddressId(graphAddressId)
                .setAccountId(accountId)
                .setGraphId(graphId)
                .build();
    }

    private static Drive createDrive(String name, String accountId) {
        return drives.createDrive(CreateDriveRequest.newBuilder()
                        .setName(name)
                        .setAccountId(accountId)
                        .setDriveType(DriveType.DRIVE_TYPE_INTAKE)
                        .build())
                .getDrive();
    }

    // ---------------------------------------------------------------- tests

    @Test
    void createGetListDriveRoundTrip() {
        Drive created = createDrive("intake", "acct-drive");
        assertThat(created.getDriveId()).isNotBlank();
        assertThat(created.getBucket()).isEqualTo("it-docs-acct-drive-intake");
        assertThat(created.getPrefix()).isEqualTo("intake");
        assertThat(created.getDriveType()).isEqualTo(DriveType.DRIVE_TYPE_INTAKE);
        assertThat(created.getCreatedAt().getSeconds()).isPositive();

        // The bucket actually exists in LocalStack.
        services.s3Client().headBucket(b -> b.bucket(created.getBucket()));

        // timestamptz stores micros, so the create response (in-memory
        // Instant, full nanos) and re-fetched rows can differ below a micro:
        // compare everything except created_at, which is asserted separately.
        Drive expected = created.toBuilder().clearCreatedAt().build();
        Drive byId = drives.getDrive(GetDriveRequest.newBuilder()
                .setDriveId(created.getDriveId()).build()).getDrive();
        assertThat(byId.toBuilder().clearCreatedAt().build()).isEqualTo(expected);
        assertThat(byId.getCreatedAt().getSeconds()).isEqualTo(created.getCreatedAt().getSeconds());
        Drive byName = drives.getDrive(GetDriveRequest.newBuilder()
                .setName("intake").setAccountId("acct-drive").build()).getDrive();
        assertThat(byName.toBuilder().clearCreatedAt().build()).isEqualTo(expected);

        assertThat(drives.listDrives(ListDrivesRequest.newBuilder()
                .setAccountId("acct-drive").build()).getDrivesList())
                .containsExactly(byId);

        // Deterministic id ⇒ re-create is idempotent.
        Drive recreated = createDrive("intake", "acct-drive");
        assertThat(recreated.getDriveId()).isEqualTo(created.getDriveId());
    }

    @Test
    void fullIntakeSaveRoundTripsByteExact() {
        String account = "acct-full";
        // Unique drive name: GetBlob's storage_ref lookup is by bare name
        // (v1 trusts the caller), so this test's drive must be unambiguous
        // across the whole IT suite.
        createDrive("full-docs", account);
        Document doc = fixture("doc-full-1", account, "ds-1");

        SaveDocumentResponse saved = documents.saveDocument(
                intakeSave(doc, "full-docs", account).build());
        assertThat(saved.getNodeId()).isNotBlank();
        assertThat(saved.getDeduplicated()).isFalse();
        assertThat(saved.getChecksum()).hasSize(64);
        assertThat(saved.getDrive()).isEqualTo("full-docs");
        assertThat(saved.getSizeBytes()).isPositive();
        assertThat(saved.getStoragePrefix())
                .startsWith("full-docs/documents/" + account + "/")
                .endsWith(saved.getNodeId());

        GetDocumentResponse got = documents.getDocument(
                GetDocumentRequest.newBuilder().setNodeId(saved.getNodeId()).build());
        assertThat(got.getDocument().toByteArray()).isEqualTo(doc.toByteArray());
        assertThat(got.getNodeId()).isEqualTo(saved.getNodeId());
        assertThat(got.getSizeBytes()).isEqualTo(got.getDocument().getSerializedSize());

        GetDocumentResponse byRef = documents.getDocumentByReference(
                GetDocumentByReferenceRequest.newBuilder()
                        .setDocumentRef(reference("doc-full-1", "ds-1", account, "intake:" + account))
                        .build());
        assertThat(byRef.getDocument().toByteArray()).isEqualTo(doc.toByteArray());
        assertThat(byRef.getNodeId()).isEqualTo(saved.getNodeId());

        // Manifest: CORE, BLOBS, two CHUNKS sub-entries, PARSED — all PRESENT
        // with sha256s, version 1.
        GetDocumentManifestResponse manifest = documents.getDocumentManifest(
                GetDocumentManifestRequest.newBuilder().setNodeId(saved.getNodeId()).build());
        assertThat(manifest.getDrive()).isEqualTo("full-docs");
        DocumentManifest m = manifest.getManifest();
        assertThat(m.getDocVersion()).isEqualTo(1);
        assertThat(m.getPartsList()).allSatisfy(e -> {
            assertThat(e.getState()).isEqualTo(PartState.PART_STATE_PRESENT);
            assertThat(e.getSha256()).hasSize(64);
            assertThat(e.getSizeBytes()).isPositive();
        });
        assertThat(m.getPartsList().stream().map(PartManifestEntry::getPart))
                .containsExactly(DocumentPart.DOCUMENT_PART_CORE, DocumentPart.DOCUMENT_PART_BLOBS,
                        DocumentPart.DOCUMENT_PART_CHUNKS, DocumentPart.DOCUMENT_PART_CHUNKS,
                        DocumentPart.DOCUMENT_PART_PARSED);
        assertThat(m.getPartsList().stream()
                .filter(e -> e.getPart() == DocumentPart.DOCUMENT_PART_CHUNKS)
                .map(PartManifestEntry::getSubKey))
                .containsExactly("chunks-a", "chunks-b");

        // GetBlob: raw object fetch by storage_ref (here: the CORE part object).
        String coreKey = m.getPartsList().stream()
                .filter(e -> e.getPart() == DocumentPart.DOCUMENT_PART_CORE)
                .findFirst().orElseThrow().getObjectKey();
        var blob = documents.getBlob(GetBlobRequest.newBuilder()
                .setStorageRef(FileStorageReference.newBuilder()
                        .setDriveName("full-docs").setObjectKey(coreKey))
                .build());
        assertThat(DocumentPartCodec.sha256Hex(blob.getData().toByteArray()))
                .isEqualTo(m.getPartsList().get(0).getSha256());
        assertThat(blob.getSizeBytes()).isEqualTo(blob.getData().size());
    }

    @Test
    void dedupeSkipsSecondIdenticalIntakeSaveAndForceSaveBypasses() {
        String account = "acct-dedupe";
        createDrive("docs", account);
        Document doc = fixture("doc-dedupe-1", account, "ds-1");

        SaveDocumentResponse first = documents.saveDocument(intakeSave(doc, "docs", account).build());
        SaveDocumentResponse second = documents.saveDocument(intakeSave(doc, "docs", account).build());

        assertThat(second.getDeduplicated()).isTrue();
        assertThat(second.getNodeId()).isEqualTo(first.getNodeId());
        assertThat(second.getChecksum()).isEqualTo(first.getChecksum());

        DocumentRecord row = services.documentLedger()
                .findByNodeId(UUID.fromString(first.getNodeId())).orElseThrow();
        assertThat(row.reprocessCount).isEqualTo(1);
        assertThat(row.lastReprocessedAt).isNotNull();

        // force_save bypasses dedupe: the body is rewritten, no reprocess mark.
        SaveDocumentResponse forced = documents.saveDocument(
                intakeSave(doc, "docs", account).setForceSave(true).build());
        assertThat(forced.getDeduplicated()).isFalse();
        assertThat(forced.getNodeId()).isEqualTo(first.getNodeId());
        row = services.documentLedger().findByNodeId(UUID.fromString(first.getNodeId())).orElseThrow();
        assertThat(row.reprocessCount).isEqualTo(1);
        assertThat(row.readManifest().getDocVersion()).isEqualTo(2);
    }

    @Test
    void partialReadCoreOnlyAndChunkSetFilter() {
        String account = "acct-pread";
        createDrive("docs", account);
        Document doc = fixture("doc-pread-1", account, "ds-1");
        SaveDocumentResponse saved = documents.saveDocument(intakeSave(doc, "docs", account).build());

        // CORE only: everything but blob_bag / parsed_metadata / semantic_results.
        Document coreOnly = doc.toBuilder()
                .clearBlobBag()
                .clearParsedMetadata()
                .setSearchMetadata(doc.getSearchMetadata().toBuilder().clearSemanticResults())
                .build();
        GetDocumentResponse core = documents.getDocument(GetDocumentRequest.newBuilder()
                .setNodeId(saved.getNodeId())
                .addParts(DocumentPart.DOCUMENT_PART_CORE)
                .build());
        assertThat(core.getDocument().toByteArray()).isEqualTo(coreOnly.toByteArray());
        // The response still carries the FULL manifest (all parts, all states).
        assertThat(core.getManifest().getPartsCount()).isEqualTo(5);

        // CHUNKS narrowed to one chunk set: only doc_id + that set survives.
        Document chunksAOnly = Document.newBuilder()
                .setDocId(doc.getDocId())
                .setSearchMetadata(SearchMetadata.newBuilder()
                        .addSemanticResults(chunkSet("chunks-a", 2)))
                .build();
        GetDocumentResponse chunks = documents.getDocument(GetDocumentRequest.newBuilder()
                .setNodeId(saved.getNodeId())
                .addParts(DocumentPart.DOCUMENT_PART_CHUNKS)
                .addChunkSets("chunks-a")
                .build());
        assertThat(chunks.getDocument().toByteArray()).isEqualTo(chunksAOnly.toByteArray());
    }

    @Test
    void partialSaveCarriesUnwrittenPartsForwardWithOriginalProvenance() {
        String account = "acct-psave";
        createDrive("pipe", account);
        Document doc = fixture("doc-psave-1", account, "ds-2");
        WriteProvenance hop0Writer = WriteProvenance.newBuilder()
                .setModuleId("parser").setNodeId("hop-0").setGraphId("graph-a")
                .setGraphVersion(7).build();

        // hop-1: full pipeline save of the parsed document.
        SaveDocumentResponse hop1 = documents.saveDocument(SaveDocumentRequest.newBuilder()
                .setDocument(doc)
                .setDrive("pipe")
                .setConnectorId(CONNECTOR)
                .setGraphLocationId("hop-1")
                .setGraphId("graph-a")
                .setWrittenBy(hop0Writer)
                .build());
        DocumentReference hop1Ref = reference("doc-psave-1", "hop-1", account, "graph-a");
        DocumentManifest hop1Manifest = documents.getDocumentManifest(
                GetDocumentManifestRequest.newBuilder().setDocumentRef(hop1Ref).build())
                .getManifest();

        // hop-2: the chunker re-stages with ONLY new chunks; the rest copies.
        Document doc2 = doc.toBuilder()
                .setSearchMetadata(doc.getSearchMetadata().toBuilder()
                        .clearSemanticResults()
                        .addSemanticResults(chunkSet("chunks-c", 2)))
                .build();
        WriteProvenance hop1Writer = WriteProvenance.newBuilder()
                .setModuleId("chunker").setNodeId("hop-1").setGraphId("graph-a")
                .setGraphVersion(7).build();
        SaveDocumentResponse hop2 = documents.saveDocument(SaveDocumentRequest.newBuilder()
                .setDocument(doc2)
                .setDrive("pipe")
                .setConnectorId(CONNECTOR)
                .setGraphLocationId("hop-2")
                .setGraphId("graph-a")
                .setWrittenBy(hop1Writer)
                .addPartsWritten(DocumentPart.DOCUMENT_PART_CHUNKS)
                .setCopyUnwrittenPartsFrom(hop1Ref)
                .build());
        assertThat(hop2.getNodeId()).isNotEqualTo(hop1.getNodeId());

        DocumentManifest hop2Manifest = documents.getDocumentManifest(
                GetDocumentManifestRequest.newBuilder().setNodeId(hop2.getNodeId()).build())
                .getManifest();
        Map<DocumentPart, PartManifestEntry> byPart = new java.util.EnumMap<>(DocumentPart.class);
        hop2Manifest.getPartsList().forEach(e -> byPart.putIfAbsent(e.getPart(), e));

        // Carried entries keep the ORIGINAL sha256/size/updated_at/written_by
        // stamps; only the object key moved to the hop-2 address.
        for (DocumentPart part : List.of(DocumentPart.DOCUMENT_PART_CORE,
                DocumentPart.DOCUMENT_PART_BLOBS, DocumentPart.DOCUMENT_PART_PARSED)) {
            PartManifestEntry carriedEntry = byPart.get(part);
            PartManifestEntry sourceEntry = hop1Manifest.getPartsList().stream()
                    .filter(e -> e.getPart() == part).findFirst().orElseThrow();
            assertThat(carriedEntry.getState()).isEqualTo(PartState.PART_STATE_PRESENT);
            assertThat(carriedEntry.getWrittenBy()).isEqualTo(hop0Writer);
            assertThat(carriedEntry.getSha256()).isEqualTo(sourceEntry.getSha256());
            assertThat(carriedEntry.getSizeBytes()).isEqualTo(sourceEntry.getSizeBytes());
            assertThat(carriedEntry.getUpdatedAt()).isEqualTo(sourceEntry.getUpdatedAt());
            assertThat(carriedEntry.getObjectKey())
                    .startsWith(hop2.getStoragePrefix() + "/")
                    .isNotEqualTo(sourceEntry.getObjectKey());
        }
        // The chunk set this save wrote carries THIS save's provenance.
        PartManifestEntry chunks = byPart.get(DocumentPart.DOCUMENT_PART_CHUNKS);
        assertThat(chunks.getSubKey()).isEqualTo("chunks-c");
        assertThat(chunks.getWrittenBy()).isEqualTo(hop1Writer);

        // The hop-2 state reassembles byte-exact (carried bytes + new chunks).
        GetDocumentResponse reassembled = documents.getDocument(
                GetDocumentRequest.newBuilder().setNodeId(hop2.getNodeId()).build());
        assertThat(reassembled.getDocument().toByteArray()).isEqualTo(doc2.toByteArray());

        // A copy source that is gone fails FAILED_PRECONDITION.
        assertThatThrownBy(() -> documents.saveDocument(SaveDocumentRequest.newBuilder()
                .setDocument(doc2)
                .setDrive("pipe")
                .setConnectorId(CONNECTOR)
                .setGraphLocationId("hop-3")
                .setGraphId("graph-a")
                .addPartsWritten(DocumentPart.DOCUMENT_PART_CHUNKS)
                .setCopyUnwrittenPartsFrom(reference("doc-psave-1", "nowhere", account, "graph-a"))
                .build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode())
                                .isEqualTo(Status.Code.FAILED_PRECONDITION));
    }

    @Test
    void deleteTombstonesThenPurgesRowsAndObjects() {
        String account = "acct-del";
        Drive drive = createDrive("docs", account);
        Document doc = fixture("doc-del-1", account, "ds-3");
        SaveDocumentResponse saved = documents.saveDocument(intakeSave(doc, "docs", account).build());
        UUID nodeId = UUID.fromString(saved.getNodeId());
        DocumentReference ref = reference("doc-del-1", "ds-3", account, "intake:" + account);

        DocumentRecord before = services.documentLedger().findByNodeId(nodeId).orElseThrow();
        List<String> partKeys = before.readManifest().getPartsList().stream()
                .filter(e -> e.getState() == PartState.PART_STATE_PRESENT)
                .map(PartManifestEntry::getObjectKey)
                .toList();
        assertThat(partKeys).hasSize(5);

        // Metadata-only delete: tombstone to PENDING_PURGE; updated_at must
        // NOT move (the staleness guard only trusts body rewrites).
        DeleteDocumentResponse tombstoned = documents.deleteDocument(DeleteDocumentRequest.newBuilder()
                .setByReference(DeleteDocumentByReferenceCommand.newBuilder().setDocumentRef(ref))
                .build());
        assertThat(tombstoned.getOutcome())
                .isEqualTo(DeleteDocumentOutcome.DELETE_DOCUMENT_OUTCOME_REMOVED);
        assertThat(tombstoned.getDocumentsRemoved()).isEqualTo(1);
        assertThat(tombstoned.getRemovedNodesList().stream().map(n -> n.getNodeId()))
                .containsExactly(saved.getNodeId());
        DocumentRecord after = services.documentLedger().findByNodeId(nodeId).orElseThrow();
        assertThat(after.status).isEqualTo(DocumentStatus.PENDING_PURGE);
        assertThat(after.updatedAt).isEqualTo(before.updatedAt);
        // Objects survive the tombstone.
        for (String key : partKeys) {
            services.blobStore().headObject(drive.getBucket(), key);
        }

        // purge_storage=true: rows AND their part objects go away.
        DeleteDocumentResponse purged = documents.deleteDocument(DeleteDocumentRequest.newBuilder()
                .setLogicalDocument(DeleteLogicalDocumentCommand.newBuilder()
                        .setDocId("doc-del-1").setAccountId(account).setDatasourceId("ds-3"))
                .setPurgeStorage(true)
                .build());
        assertThat(purged.getOutcome())
                .isEqualTo(DeleteDocumentOutcome.DELETE_DOCUMENT_OUTCOME_REMOVED);
        assertThat(services.documentLedger().findByNodeId(nodeId)).isEmpty();
        for (String key : partKeys) {
            assertThatThrownBy(() -> services.blobStore().headObject(drive.getBucket(), key))
                    .isInstanceOf(BlobStore.BlobNotFoundException.class);
        }

        // Idempotent: deleting again matches nothing.
        DeleteDocumentResponse again = documents.deleteDocument(DeleteDocumentRequest.newBuilder()
                .setLogicalDocument(DeleteLogicalDocumentCommand.newBuilder()
                        .setDocId("doc-del-1").setAccountId(account).setDatasourceId("ds-3"))
                .setPurgeStorage(true)
                .build());
        assertThat(again.getOutcome())
                .isEqualTo(DeleteDocumentOutcome.DELETE_DOCUMENT_OUTCOME_NOTHING_TO_REMOVE);
        assertThat(again.getDocumentsRemoved()).isZero();
    }

    @Test
    void listDocumentsFiltersAndPaginates() {
        String account = "acct-list";
        createDrive("docs", account);
        for (int i = 0; i < 3; i++) {
            documents.saveDocument(intakeSave(fixture("doc-list-" + i, account, "ds-4"), "docs", account)
                    .setCrawlId("crawl-1")
                    .build());
        }
        documents.saveDocument(intakeSave(fixture("doc-list-other", account, "ds-4"), "docs", account)
                .setConnectorId("other-connector")
                .build());

        ListDocumentsResponse page1 = documents.listDocuments(ListDocumentsRequest.newBuilder()
                .setAccountId(account).setConnectorId(CONNECTOR).setLimit(2).build());
        assertThat(page1.getDocumentsCount()).isEqualTo(2);
        assertThat(page1.getTotalCount()).isEqualTo(3);
        assertThat(page1.getNextContinuationToken()).isNotBlank();

        ListDocumentsResponse page2 = documents.listDocuments(ListDocumentsRequest.newBuilder()
                .setAccountId(account).setConnectorId(CONNECTOR).setLimit(2)
                .setContinuationToken(page1.getNextContinuationToken()).build());
        assertThat(page2.getDocumentsCount()).isEqualTo(1);
        assertThat(page2.getNextContinuationToken()).isEmpty();

        assertThat(page1.getDocumentsList().stream().map(d -> d.getNodeId()).toList())
                .doesNotContainAnyElementsOf(
                        page2.getDocumentsList().stream().map(d -> d.getNodeId()).toList());
        assertThat(page1.getDocumentsList()).allSatisfy(d -> {
            assertThat(d.getConnectorId()).isEqualTo(CONNECTOR);
            assertThat(d.getCrawlId()).isEqualTo("crawl-1");
            assertThat(d.getTitle()).isEqualTo("Quarterly Report");
        });

        assertThat(documents.listDocuments(ListDocumentsRequest.newBuilder()
                .setAccountId(account).setConnectorId("other-connector").build())
                .getDocumentsCount()).isEqualTo(1);
        assertThat(documents.listDocuments(ListDocumentsRequest.newBuilder()
                .setAccountId(account).build()).getTotalCount()).isEqualTo(4);
        assertThat(documents.listDocuments(ListDocumentsRequest.newBuilder()
                .setAccountId(account).setCrawlId("crawl-1").build()).getTotalCount()).isEqualTo(3);
    }

    @Test
    void errorMappingSpotChecks() {
        String account = "acct-err";
        createDrive("docs", account);
        Document doc = fixture("doc-err-1", account, "ds-5");

        // Save without graph_id → INVALID_ARGUMENT naming the field.
        assertThatThrownBy(() -> documents.saveDocument(SaveDocumentRequest.newBuilder()
                .setDocument(doc).setDrive("docs").setConnectorId(CONNECTOR)
                .setUseDatasourceId(true).build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e -> {
                    assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT);
                    assertThat(e.getStatus().getDescription()).contains("graph_id");
                });

        // Intake save with a contradicting graph_id → INVALID_ARGUMENT.
        assertThatThrownBy(() -> documents.saveDocument(intakeSave(doc, "docs", account)
                .setGraphId("some-other-graph").build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));

        // Intake save carrying cluster_id → INVALID_ARGUMENT.
        assertThatThrownBy(() -> documents.saveDocument(intakeSave(doc, "docs", account)
                .setClusterId("cluster-1").build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));

        // Save to an unknown drive → NOT_FOUND.
        assertThatThrownBy(() -> documents.saveDocument(intakeSave(doc, "no-such-drive", account)
                .build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND));

        // Get unknown node_id → NOT_FOUND; malformed node_id → INVALID_ARGUMENT.
        assertThatThrownBy(() -> documents.getDocument(
                GetDocumentRequest.newBuilder().setNodeId(UUID.randomUUID().toString()).build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.NOT_FOUND));
        assertThatThrownBy(() -> documents.getDocument(
                GetDocumentRequest.newBuilder().setNodeId("not-a-uuid").build()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));

        // Manifest without a coordinate → INVALID_ARGUMENT.
        assertThatThrownBy(() -> documents.getDocumentManifest(
                GetDocumentManifestRequest.getDefaultInstance()))
                .isInstanceOfSatisfying(StatusRuntimeException.class, e ->
                        assertThat(e.getStatus().getCode()).isEqualTo(Status.Code.INVALID_ARGUMENT));
    }
}
