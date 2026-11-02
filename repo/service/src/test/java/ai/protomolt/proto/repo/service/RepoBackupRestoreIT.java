package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.codec.SegmentationDigests;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.semantic.v1.*;
import ai.protomolt.proto.repo.v1.*;
import ai.protomolt.proto.repo.v2.*;
import ai.protomolt.proto.repo.v2.DocumentPart;
import ai.protomolt.proto.repo.v2.PartManifestEntry;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.*;
import com.google.protobuf.Descriptors;
import com.google.protobuf.StringValue;
import com.google.protobuf.UnknownFieldSet;
import io.grpc.ManagedChannel;
import io.grpc.inprocess.InProcessChannelBuilder;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Offline operator backup/restore drill: all destructive operations target these disposable containers. */
@Testcontainers
class RepoBackupRestoreIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    @Container static final LocalStackContainer S3 = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");
    private static final String ACCOUNT = "restore-drill";
    private static final String TYPE = "restore.v1.Record";
    private static final Descriptors.FileDescriptor SCHEMA = schema();
    private static final ByteString RAW = ByteString.copyFrom(new byte[] {0, 1, -1, 127, -128, 10, 0, 42});
    private static final List<PartSelector> PARTS = List.of(
            PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE).build(),
            PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_BLOBS).build(),
            PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_PARSED).build(),
            PartSelector.newBuilder().setSegmentationId("sentences").build(),
            PartSelector.newBuilder().setEmbeddingSet(EmbeddingSetKey.newBuilder()
                    .setSegmentationId("sentences").setEmbeddingSetId("model")).build());
    @TempDir Path directory;

    @Test void restoresAllPartsHistoryDescriptorsAndOutcomesIntoAnEmptyStoreAtTheRecordedNamespace() throws Exception {
        var first = request(1);
        var second = request(2);
        SaveDocumentResponse savedFirst;
        SaveDocumentResponse savedSecond;
        List<RevisionEvidence> before;
        try (var live = new Live(true)) {
            DriveServiceGrpc.newBlockingStub(live.channel).createDrive(CreateDriveRequest.newBuilder()
                    .setAccountId(ACCOUNT).setName("intake").setDriveType(DriveType.DRIVE_TYPE_INTAKE).build());
            savedFirst = live.documents.saveDocument(first);
            savedSecond = live.documents.saveDocument(second);
            before = List.of(evidence(live.documents, 1), evidence(live.documents, 2));
            assertThat(before.get(0).document().getDocument().getSearchMetadata().getTitle()).isEqualTo("Title 1");
            assertThat(before.get(1).document().getDocument().getSearchMetadata().getTitle()).isEqualTo("Title 2");
            for (var selector : PARTS.subList(1, PARTS.size())) {
                assertThat(before.get(1).parts().get(selector)).isEqualTo(before.get(0).parts().get(selector));
            }
        }
        // Services are closed: this is a coordinated offline backup, not a live consistency algorithm.
        var catalogBefore = catalogEvidence();
        Path dump = directory.resolve("repository.sql");
        postgres("pg_dump", "-U", POSTGRES.getUsername(), "-d", POSTGRES.getDatabaseName(),
                "--no-owner", "--no-privileges", "-f", "/tmp/repository-backup.sql");
        POSTGRES.copyFileFromContainer("/tmp/repository-backup.sql", dump.toString());
        assertThat(dump).isNotEmptyFile();
        try (var objects = objectClient()) {
            var backup = objectBackup(objects);
            assertThat(backup).hasSizeGreaterThanOrEqualTo(7); // five initial parts, raw source, changed CORE
            // Delete every object in this test's private LocalStack and drop its whole database.
            emptyObjects(objects);
            assertThat(objectBackup(objects)).isEmpty();
            postgres("dropdb", "-U", POSTGRES.getUsername(), POSTGRES.getDatabaseName());
            postgres("createdb", "-U", POSTGRES.getUsername(), "-T", "template0", POSTGRES.getDatabaseName());
            try (var connection = connection(); var rows = connection.createStatement().executeQuery(
                    "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'repo_catalog'")) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getLong(1)).isZero();
            }
            // Copy the exported dump back independently, then use PostgreSQL's standard restore path.
            POSTGRES.copyFileToContainer(MountableFile.forHostPath(dump), "/tmp/repository-restore.sql");
            postgres("psql", "-X", "-v", "ON_ERROR_STOP=1", "-U", POSTGRES.getUsername(),
                    "-d", POSTGRES.getDatabaseName(), "-f", "/tmp/repository-restore.sql");
            assertThat(catalogEvidence()).isEqualTo(catalogBefore);
            // SQL alone must not pass verification when the referenced bytes are still absent.
            assertThatThrownBy(() -> verifyCore(1)).hasMessageContaining("missing");
            for (var object : backup) objects.putObject(b -> b.bucket(object.bucket()).key(object.key())
                    .metadata(object.metadata()).contentType(object.contentType()), RequestBody.fromBytes(object.bytes().toByteArray()));
            assertThat(objectBackup(objects)).containsExactlyElementsOf(backup);
        }
        // Fresh composition, no mounted custom schema: decoding uses restored retained descriptors.
        try (var restored = new Live(false)) {
            assertThat(List.of(evidence(restored.documents, 1), evidence(restored.documents, 2))).isEqualTo(before);
            assertThat(restored.documents.getDocument(GetDocumentRequest.newBuilder().setAddress(address()).build()))
                    .isEqualTo(before.get(1).document());
            assertThat(restored.documents.saveDocument(first)).isEqualTo(savedFirst.toBuilder().setReplayed(true).build());
            assertThat(restored.documents.saveDocument(second)).isEqualTo(savedSecond.toBuilder().setReplayed(true).build());
        }
        for (int revision : List.of(1, 2)) assertThat(verifyCore(revision))
                .contains("core=VERIFIED_WITH_RETAINED_SCHEMAS", "current_entry=NOT_REQUESTED");
        // Replay and maintenance neither invent a new stack identity nor rewrite evidence/outcomes/events.
        assertThat(catalogEvidence()).isEqualTo(catalogBefore);
    }

    private static RevisionEvidence evidence(DocumentServiceGrpc.DocumentServiceBlockingStub documents, int revision) throws Exception {
        var selected = RevisionSelector.newBuilder().setRevision(revision);
        var document = documents.getDocument(GetDocumentRequest.newBuilder().setAddress(address()).setRevision(selected).build());
        var manifest = documents.getManifest(GetManifestRequest.newBuilder().setAddress(address()).setRevision(selected).build());
        assertThat(manifest.getManifest().getPartsList().stream().map(PartManifestEntry::getSelector).toList())
                .containsExactlyInAnyOrderElementsOf(PARTS);
        assertThat(manifest.getValidation().getStatus()).isEqualTo(ValidationStatus.VALIDATION_STATUS_TYPED_VALID);
        assertThat(StringValue.parseFrom(document.getDocument().getStructuredData().getValue())
                .getUnknownFields().getField(99).getVarintList()).containsExactly(Long.MAX_VALUE);
        var parts = new LinkedHashMap<PartSelector, ByteString>();
        for (var part : PARTS) {
            var frames = documents.readDocument(ReadDocumentRequest.newBuilder().setAddress(address()).setRevision(selected)
                    .setSelection(PartSelection.newBuilder().addSelectors(part)).build());
            var bytes = ByteString.newOutput();
            boolean complete = false;
            while (frames.hasNext()) {
                var frame = frames.next();
                if (frame.hasPartData()) frame.getPartData().getData().writeTo(bytes);
                if (frame.hasComplete()) complete = true;
            }
            assertThat(complete).as("selected part stream completes: %s", part).isTrue();
            assertThat(bytes.size()).isPositive();
            parts.put(part, bytes.toByteString());
        }
        var blobFrames = documents.readBlob(ReadBlobRequest.newBuilder().setAddress(address()).setRevision(selected).setBlobId("source").build());
        var blob = ByteString.newOutput();
        int headers = 0;
        while (blobFrames.hasNext()) {
            var frame = blobFrames.next();
            if (frame.hasHeader()) { headers++; assertThat(frame.getHeader().getTotalLength()).isEqualTo(RAW.size()); }
            if (frame.hasData()) frame.getData().writeTo(blob);
        }
        assertThat(headers).isEqualTo(1);
        assertThat(blob.toByteString()).isEqualTo(RAW);
        var descriptors = new TreeMap<String, GetSchemaDescriptorResponse>();
        var digests = new TreeSet<String>();
        manifest.getValidation().getPartSchemasList().forEach(s -> digests.add(s.getFragmentSchema().getDescriptorSha256()));
        manifest.getValidation().getResolvedOccurrencesList().forEach(s -> digests.add(s.getSchema().getDescriptorSha256()));
        assertThat(manifest.getValidation().getResolvedOccurrencesList()).isNotEmpty();
        for (var digest : digests) descriptors.put(digest, documents.getSchemaDescriptor(GetSchemaDescriptorRequest.newBuilder()
                .setAddress(address()).setRevision(selected).setDescriptorSha256(digest).build()));
        var outcome = documents.getOperationOutcome(GetOperationOutcomeRequest.newBuilder().setAccountId(ACCOUNT)
                .setOperationId("restore-save-" + revision).build());
        assertThat(outcome.hasCommitted()).isTrue();
        return new RevisionEvidence(document, manifest, parts, blob.toByteString(), descriptors, outcome);
    }

    private record RevisionEvidence(GetDocumentResponse document, GetManifestResponse manifest,
            Map<PartSelector, ByteString> parts, ByteString raw, Map<String, GetSchemaDescriptorResponse> descriptors,
            GetOperationOutcomeResponse outcome) {}
    private record ObjectBackup(String bucket, String key, ByteString bytes, Map<String, String> metadata, String contentType) {}

    private static void emptyObjects(S3Client client) {
        for (var bucket : client.listBuckets().buckets()) {
            // Versioned DELETE alone only hides bytes behind a marker. Remove every physical version.
            var versions = new ArrayList<ObjectIdentifier>();
            for (var page : client.listObjectVersionsPaginator(b -> b.bucket(bucket.name()))) {
                page.versions().forEach(v -> versions.add(ObjectIdentifier.builder().key(v.key()).versionId(v.versionId()).build()));
                page.deleteMarkers().forEach(v -> versions.add(ObjectIdentifier.builder().key(v.key()).versionId(v.versionId()).build()));
            }
            for (var version : versions) client.deleteObject(b -> b.bucket(bucket.name()).key(version.key()).versionId(version.versionId()));
            for (var page : client.listObjectVersionsPaginator(b -> b.bucket(bucket.name()))) {
                assertThat(page.versions()).isEmpty();
                assertThat(page.deleteMarkers()).isEmpty();
            }
        }
    }

    private static List<ObjectBackup> objectBackup(S3Client client) {
        var backup = new ArrayList<ObjectBackup>();
        for (var bucket : client.listBuckets().buckets()) {
            for (var page : client.listObjectsV2Paginator(b -> b.bucket(bucket.name()))) {
                for (var object : page.contents()) {
                    var data = client.getObjectAsBytes(b -> b.bucket(bucket.name()).key(object.key()));
                    backup.add(new ObjectBackup(bucket.name(), object.key(), ByteString.copyFrom(data.asByteArray()),
                            data.response().metadata(), data.response().contentType()));
                }
            }
        }
        backup.sort(Comparator.comparing(ObjectBackup::bucket).thenComparing(ObjectBackup::key));
        return List.copyOf(backup);
    }

    private static List<List<String>> catalogEvidence() throws Exception {
        var result = new ArrayList<List<String>>();
        try (var connection = connection()) {
            for (String query : List.of(
                    "SELECT stack_id, provider, endpoint, namespace, record FROM repo_catalog.catalog_stacks ORDER BY stack_id",
                    "SELECT document_id, current_revision, catalog_entry, catalog_entry_sha256 FROM repo_catalog.catalog_documents ORDER BY document_id",
                    "SELECT document_id, revision, manifest, manifest_sha256, intent_sha256 FROM repo_catalog.catalog_revisions ORDER BY document_id, revision",
                    "SELECT stack_id, object_key, sha256, length, purged FROM repo_catalog.catalog_objects ORDER BY stack_id, object_key",
                    "SELECT sha256, content FROM repo_catalog.catalog_schema_artifacts ORDER BY sha256",
                    "SELECT operation_id, outcome FROM repo_catalog.catalog_operation_outcomes ORDER BY account_id, operation_id",
                    "SELECT event_id, payload FROM repo_catalog.catalog_outbox ORDER BY sequence")) {
                result.add(List.of(query));
                try (var rows = connection.createStatement().executeQuery(query)) {
                    while (rows.next()) {
                        var row = new ArrayList<String>();
                        for (int i = 1; i <= rows.getMetaData().getColumnCount(); i++) row.add(rows.getString(i));
                        result.add(row);
                    }
                }
            }
        }
        return result;
    }

    private static void postgres(String... args) throws Exception {
        var result = POSTGRES.execInContainer(args);
        assertThat(result.getExitCode()).withFailMessage("PostgreSQL %s failed: %s", args[0], result.getStderr()).isZero();
    }

    private static java.sql.Connection connection() throws Exception {
        return DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }

    private static S3Client objectClient() {
        return S3Client.builder().endpointOverride(S3.getEndpoint()).region(Region.of(S3.getRegion())).forcePathStyle(true)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(S3.getAccessKey(), S3.getSecretKey())))
                .httpClient(UrlConnectionHttpClient.create()).build();
    }

    private static String verifyCore(int revision) {
        var output = new ByteArrayOutputStream();
        RepoCatalogMaintenance.run(new String[] {"verify-core", ACCOUNT, "intake:" + ACCOUNT, "ds", "doc", Integer.toString(revision)},
                () -> RepoMaintenanceComposition.open(config()), new PrintStream(output));
        return output.toString(StandardCharsets.UTF_8);
    }

    private static final class Live implements AutoCloseable {
        final RepoServices services;
        final ManagedChannel channel;
        final DocumentServiceGrpc.DocumentServiceBlockingStub documents;
        Live(boolean mountSchema) throws Exception {
            services = RepoServices.build(config());
            if (mountSchema) services.withAdmittedSchemas(List.of(SCHEMA));
            String name = "restore-" + UUID.randomUUID();
            services.startInProcess(name);
            channel = InProcessChannelBuilder.forName(name).directExecutor().build();
            documents = DocumentServiceGrpc.newBlockingStub(channel);
        }
        @Override public void close() throws Exception { channel.shutdownNow(); services.close(); }
    }

    private static SaveDocumentRequest request(int revision) {
        var payload = StringValue.newBuilder().setValue("record").setUnknownFields(UnknownFieldSet.newBuilder().addField(99,
                UnknownFieldSet.Field.newBuilder().addVarint(Long.MAX_VALUE).build()).build()).build().toByteString();
        var document = Document.newBuilder().setDocId("doc")
                .setOwnership(OwnershipContext.newBuilder().setAccountId(ACCOUNT).setDatasourceId("ds")
                        .setSecurity(DocumentSecurity.newBuilder().addPermissions(AccessRule.newBuilder()
                                .setIdentityType("public").setIdentity("public").setAccess(Access.ACCESS_READ))))
                .setSearchMetadata(SearchMetadata.newBuilder().setTitle("Title " + revision).setBody("body"))
                .setStructuredData(Any.newBuilder().setTypeUrl("type.googleapis.com/" + TYPE).setValue(payload));
        var expected = ExpectedRevision.newBuilder();
        if (revision == 1) {
            expected.setNone(true);
            var segmentation = Segmentation.newBuilder().setSegmentationId("sentences")
                    .setKind(SegmentationKind.SEGMENTATION_KIND_SEMANTIC_BOUNDARY)
                    .setSource(TextSource.newBuilder().setDerived(DerivedText.newBuilder().setDerivationId("identity").setText("body"))
                            .setBinding(SourceTextBinding.newBuilder().setUtf8Length(4)
                                    .setResolvedTextSha256(DocumentPartCodec.sha256Hex(ByteString.copyFromUtf8("body")))))
                    .setOffsetUnit(OffsetUnit.OFFSET_UNIT_UTF8_BYTE)
                    .addSegments(Segment.newBuilder().setSegmentId("one").setText("body").setStart(0).setEnd(4)).build();
            document.getSearchMetadataBuilder().addSegmentations(segmentation).addEmbeddingSets(EmbeddingSet.newBuilder()
                    .setEmbeddingSetId("model").setSegmentationId("sentences").setSegmentationDigest(SegmentationDigests.digest(segmentation))
                    .setModel("test-v1").setDimensions(2).setSimilarity(Similarity.SIMILARITY_COSINE)
                    .addVectors(SegmentVector.newBuilder().setSegmentId("one").addValues(0.25f).addValues(-0.5f)));
            document.setBlobBag(BlobBag.newBuilder().setBlob(Blob.newBuilder().setBlobId("source").setSizeBytes(RAW.size()).setData(RAW)))
                    .putParserResults("parser", ParserResult.newBuilder().setParserName("parser").setParserVersion("1")
                            .setStatus(ParseStatus.PARSE_STATUS_OK).build());
        } else expected.setRevision(revision - 1);
        var request = SaveDocumentRequest.newBuilder().setOperationId("restore-save-" + revision)
                .setTarget(DocumentTarget.newBuilder().setAddress(address())).setExpected(expected).setDocument(document)
                .setOwner(SaveOwner.newBuilder().setIntake(IntakeOrigin.newBuilder().setDatasourceId("ds")));
        for (var selector : revision == 1 ? PARTS : PARTS.subList(0, 1)) request.addWrites(PartWrite.newBuilder()
                .setSelector(selector).setAction(WriteAction.WRITE_ACTION_PUT));
        return request.build();
    }

    private static NodeAddress address() {
        return NodeAddress.newBuilder().setAccountId(ACCOUNT).setGraphId("intake:" + ACCOUNT).setGraphAddressId("ds").setDocId("doc").build();
    }

    private static RepoServiceConfig config() {
        return new RepoServiceConfig(0, new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                S3.getEndpoint().toString(), S3.getRegion(), S3.getAccessKey(), S3.getSecretKey(), "it-restore", 0,
                null, null, 0, 0L, false, 0L, 0L, false, true, 0L, null, null, null, null, null, null,
                false, ManagedStoragePolicy.disabled(), 0);
    }

    private static Descriptors.FileDescriptor schema() {
        try {
            return Descriptors.FileDescriptor.buildFrom(FileDescriptorProto.newBuilder().setName("restore.proto")
                    .setPackage("restore.v1").setSyntax("proto3").addMessageType(DescriptorProto.newBuilder().setName("Record")
                            .addField(FieldDescriptorProto.newBuilder().setName("title").setNumber(1)
                                    .setType(FieldDescriptorProto.Type.TYPE_STRING))
                            .addField(FieldDescriptorProto.newBuilder().setName("exact_value").setNumber(99)
                                    .setType(FieldDescriptorProto.Type.TYPE_INT64))).build(), new Descriptors.FileDescriptor[0]);
        } catch (Descriptors.DescriptorValidationException failure) { throw new ExceptionInInitializerError(failure); }
    }
}
