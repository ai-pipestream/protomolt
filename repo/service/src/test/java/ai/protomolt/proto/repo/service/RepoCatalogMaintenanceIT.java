package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.v1.*;
import ai.protomolt.proto.repo.v2.*;
import ai.protomolt.proto.repo.v2.DocumentPart;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos.*;
import com.google.protobuf.Descriptors;
import com.google.protobuf.StringValue;
import com.google.protobuf.UnknownFieldSet;
import io.grpc.inprocess.InProcessChannelBuilder;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real host, catalog and S3 path; each maintenance command reopens the catalog-and-storage composition. */
@Testcontainers
class RepoCatalogMaintenanceIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    @Container static final LocalStackContainer S3 = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");
    private static final String TYPE = "maintenance.v1.Record";
    private static final Descriptors.FileDescriptor SCHEMA = schema();
    @TempDir Path directory;

    @Test void productionMainRunsAnExplicitLocalCommandAndExitsWithoutServing() throws Exception {
        String account = "maint-main";
        save(account, 1);
        damageDerived(account);
        Path log = directory.resolve("maintenance.log");
        var builder = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx256m", "-cp", System.getProperty("protomolt.test.runtimeClasspath"), RepoServiceMain.class.getName(),
                "rebuild-entry", account, "intake:" + account, "ds", "doc", "1")
                .redirectErrorStream(true).redirectOutput(log.toFile());
        var env = builder.environment();
        env.keySet().removeIf(key -> key.startsWith("DOCUMENT_PLATFORM_") || key.equals("PROTOMOLT_API_TOKEN"));
        env.put(LedgerConfig.ENV_JDBC_URL, POSTGRES.getJdbcUrl());
        env.put(LedgerConfig.ENV_USERNAME, POSTGRES.getUsername());
        env.put(LedgerConfig.ENV_PASSWORD, POSTGRES.getPassword());
        env.put(RepoServiceConfig.ENV_S3_ENDPOINT, S3.getEndpoint().toString());
        env.put(RepoServiceConfig.ENV_S3_REGION, S3.getRegion());
        env.put(RepoServiceConfig.ENV_S3_ACCESS_KEY, S3.getAccessKey());
        env.put(RepoServiceConfig.ENV_S3_SECRET_KEY, S3.getSecretKey());
        env.put(RepoServiceConfig.ENV_DEFAULT_BUCKET_BASE, "it-maint");
        var process = builder.start();
        try {
            assertThat(process.waitFor(45, TimeUnit.SECONDS)).as("maintenance exits instead of serving").isTrue();
            String text = Files.readString(log);
            assertThat(process.exitValue()).withFailMessage(text).isZero();
            assertThat(text).contains("revision_type=REBUILT current_entry=REBUILT").doesNotContain("repo-service listening");
            assertThat(derived(account)).contains("Title 1", TYPE).doesNotContain("damaged");
        } finally {
            if (process.isAlive()) { process.destroyForcibly(); process.waitFor(10, TimeUnit.SECONDS); }
        }
    }

    @Test void auditDoesNotRepairAndExplicitRepairPreservesRetainedBytesAndEvidence() throws Exception {
        String account = "maint-audit";
        save(account, 1);
        var authoritative = authoritative();
        var originalCore = coreBytes(account, 1);
        var originalRead = read(account, 1);
        // Field 99 is unknown to the generated StringValue used to encode this payload,
        // but declared by the retained Record schema and admitted typed at save.
        assertThat(StringValue.parseFrom(originalRead.getDocument().getStructuredData().getValue())
                .getUnknownFields().getField(99).getVarintList()).containsExactly(Long.MAX_VALUE);
        damageDerived(account);
        var damaged = derived(account);
        assertThat(command("verify-core", account, 1, 0)).contains("core=VERIFIED_WITH_RETAINED_SCHEMAS")
                .contains("current_entry=NOT_REQUESTED");
        assertThat(derived(account)).isEqualTo(damaged);
        assertThat(authoritative()).isEqualTo(authoritative);
        assertThat(command("rebuild-entry", account, 1, 0)).contains("revision_type=REBUILT current_entry=REBUILT");
        assertThat(derived(account)).contains("Title 1", TYPE).doesNotContain("damaged");
        assertThat(authoritative()).isEqualTo(authoritative);
        assertThat(coreBytes(account, 1)).isEqualTo(originalCore);
        assertThat(read(account, 1)).isEqualTo(originalRead);
        // Retry is safe and emits no new accession/outbox/outcome.
        command("rebuild-entry", account, 1, 0);
        assertThat(authoritative()).isEqualTo(authoritative);
    }

    @Test void historicalRevisionRepairCannotReplaceTheCurrentEntry() throws Exception {
        String account = "maint-history";
        save(account, 1);
        save(account, 2);
        damageDerived(account);
        var authoritative = authoritative();
        assertThat(command("rebuild-entry", account, 1, 0)).contains("current_entry=NOT_CURRENT").doesNotContain("PURGED");
        assertThat(derived(account)).contains("damaged", TYPE);
        assertThat(command("rebuild-entry", account, 2, 0)).contains("current_entry=REBUILT");
        assertThat(derived(account)).contains("Title 2").doesNotContain("Title 1", "damaged");
        assertThat(authoritative()).isEqualTo(authoritative);
        assertThat(read(account, 1).getDocument().getSearchMetadata().getTitle()).isEqualTo("Title 1");
    }

    @Test void missingRetainedDescriptorRefusesAuditAndRepairWithoutProjectionWrites() throws Exception {
        String account = "maint-corrupt";
        save(account, 1);
        damageDerived(account);
        var before = derived(account);
        // Simulate restored catalog damage in the disposable database. The normal
        // immutable-artifact trigger prevents this in production.
        String digest;
        byte[] bytes;
        try (var services = RepoServices.build(config(0))) {
            var catalog = services.v2Composition().catalog();
            var manifest = catalog.manifest(address(account), 1).orElseThrow();
            digest = manifest.getValidation().getPartSchemas(0).getFragmentSchema().getDescriptorSha256();
            bytes = catalog.artifact(digest).orElseThrow().toByteArray();
        }
        try (var connection = connection()) {
            connection.createStatement().execute("ALTER TABLE repo_catalog.catalog_schema_artifacts DISABLE TRIGGER catalog_schema_artifacts_immutable");
            try (var delete = connection.prepareStatement("DELETE FROM repo_catalog.catalog_schema_artifacts WHERE sha256 = ?")) {
                delete.setString(1, digest);
                assertThat(delete.executeUpdate()).isEqualTo(1);
            } finally {
                connection.createStatement().execute("ALTER TABLE repo_catalog.catalog_schema_artifacts ENABLE TRIGGER catalog_schema_artifacts_immutable");
            }
        }
        try {
            for (String mode : List.of("verify-core", "rebuild-entry")) {
                assertThatThrownBy(() -> command(mode, account, 1, 0)).hasMessageContaining("retained");
                assertThat(derived(account)).isEqualTo(before);
            }
        } finally {
            try (var connection = connection(); var insert = connection.prepareStatement(
                    "INSERT INTO repo_catalog.catalog_schema_artifacts (sha256, content) VALUES (?, ?)")) {
                insert.setString(1, digest); insert.setBytes(2, bytes); insert.executeUpdate();
            }
        }
        assertThat(command("rebuild-entry", account, 1, 0)).contains("current_entry=REBUILT");
    }

    @Test void exhaustedReadBudgetAndMissingRevisionDoNotRepair() throws Exception {
        String account = "maint-budget";
        save(account, 1);
        damageDerived(account);
        var before = derived(account);
        assertThatThrownBy(() -> command("rebuild-entry", account, 1, 1)).hasMessageContaining("Read buffer budget exhausted");
        assertThatThrownBy(() -> command("rebuild-entry", account, 100, 0)).hasMessageContaining("missing or purged");
        assertThat(derived(account)).isEqualTo(before);
    }

    private static String command(String mode, String account, int revision, long budget) {
        var output = new ByteArrayOutputStream();
        RepoCatalogMaintenance.run(new String[] {mode, account, "intake:" + account, "ds", "doc", Integer.toString(revision)},
                () -> RepoMaintenanceComposition.open(config(budget)), new PrintStream(output));
        return output.toString(java.nio.charset.StandardCharsets.UTF_8);
    }

    private static void save(String account, int revision) throws Exception {
        try (var services = RepoServices.build(config(0)).withAdmittedSchemas(List.of(SCHEMA))) {
            String name = "maintenance-save-" + UUID.randomUUID();
            services.startInProcess(name);
            var channel = InProcessChannelBuilder.forName(name).directExecutor().build();
            try {
                if (revision == 1) DriveServiceGrpc.newBlockingStub(channel).createDrive(CreateDriveRequest.newBuilder()
                        .setAccountId(account).setName("intake").setDriveType(DriveType.DRIVE_TYPE_INTAKE).build());
                var unknown = UnknownFieldSet.newBuilder().addField(99,
                        UnknownFieldSet.Field.newBuilder().addVarint(Long.MAX_VALUE).build()).build();
                var payload = StringValue.newBuilder().setValue("record").setUnknownFields(unknown).build().toByteString();
                var document = Document.newBuilder().setDocId("doc")
                        .setOwnership(OwnershipContext.newBuilder().setAccountId(account).setDatasourceId("ds")
                                .setSecurity(DocumentSecurity.newBuilder().addPermissions(AccessRule.newBuilder()
                                        .setIdentityType("public").setIdentity("public").setAccess(Access.ACCESS_READ))))
                        .setSearchMetadata(SearchMetadata.newBuilder().setTitle("Title " + revision))
                        .setStructuredData(Any.newBuilder().setTypeUrl("type.googleapis.com/" + TYPE).setValue(payload));
                var expected = ExpectedRevision.newBuilder();
                if (revision == 1) expected.setNone(true); else expected.setRevision(revision - 1);
                DocumentServiceGrpc.newBlockingStub(channel).saveDocument(SaveDocumentRequest.newBuilder()
                        .setOperationId("save-" + revision).setTarget(DocumentTarget.newBuilder().setAddress(address(account)))
                        .setExpected(expected).setDocument(document)
                        .setOwner(SaveOwner.newBuilder().setIntake(IntakeOrigin.newBuilder().setDatasourceId("ds")))
                        .addWrites(PartWrite.newBuilder().setSelector(PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE))
                                .setAction(WriteAction.WRITE_ACTION_PUT)).build());
            } finally { channel.shutdownNow(); }
        }
    }

    private static GetDocumentResponse read(String account, int revision) throws Exception {
        // The custom Record schema is not mounted after restart. Exact retained artifacts must suffice.
        try (var services = RepoServices.build(config(0))) {
            String name = "maintenance-read-" + UUID.randomUUID();
            services.startInProcess(name);
            var channel = InProcessChannelBuilder.forName(name).directExecutor().build();
            try {
                return DocumentServiceGrpc.newBlockingStub(channel).getDocument(GetDocumentRequest.newBuilder()
                        .setAddress(address(account)).setRevision(RevisionSelector.newBuilder().setRevision(revision)).build());
            } finally { channel.shutdownNow(); }
        }
    }

    private static ByteString coreBytes(String account, long revision) throws Exception {
        try (var services = RepoServices.build(config(0))) {
            String name = "maintenance-bytes-" + UUID.randomUUID();
            services.startInProcess(name);
            var channel = InProcessChannelBuilder.forName(name).directExecutor().build();
            try {
                var frames = DocumentServiceGrpc.newBlockingStub(channel).readDocument(ReadDocumentRequest.newBuilder()
                        .setAddress(address(account)).setRevision(RevisionSelector.newBuilder().setRevision(revision))
                        .setSelection(PartSelection.newBuilder().addSelectors(PartSelector.newBuilder()
                                .setPart(DocumentPart.DOCUMENT_PART_CORE))).build());
                var bytes = ByteString.newOutput();
                boolean complete = false;
                while (frames.hasNext()) {
                    var frame = frames.next();
                    if (frame.hasPartData()) frame.getPartData().getData().writeTo(bytes);
                    if (frame.hasComplete()) complete = true;
                }
                assertThat(complete).as("stream completion is required, not only a prefix").isTrue();
                return bytes.toByteString();
            } finally { channel.shutdownNow(); }
        }
    }

    private static void damageDerived(String account) throws Exception {
        try (var connection = connection(); var update = connection.prepareStatement(
                "UPDATE repo_catalog.catalog_documents SET lookup_title = 'damaged' WHERE account_id = ?")) {
            update.setString(1, account); assertThat(update.executeUpdate()).isEqualTo(1);
            try (var types = connection.prepareStatement("UPDATE repo_catalog.catalog_revisions SET structured_data_type = NULL "
                    + "WHERE document_id IN (SELECT document_id FROM repo_catalog.catalog_documents WHERE account_id = ?)")) {
                types.setString(1, account); types.executeUpdate();
            }
        }
    }

    private static String derived(String account) throws Exception {
        try (var connection = connection(); var query = connection.prepareStatement(
                "SELECT lookup_title, revision, structured_data_type FROM repo_catalog.catalog_documents d "
                        + "JOIN repo_catalog.catalog_revisions r USING (document_id) WHERE account_id = ? ORDER BY revision")) {
            query.setString(1, account);
            try (var rows = query.executeQuery()) {
                var text = new StringBuilder();
                while (rows.next()) text.append(rows.getString(1)).append(':').append(rows.getLong(2))
                        .append(':').append(rows.getString(3)).append('\n');
                return text.toString();
            }
        }
    }

    private static List<String> authoritative() throws Exception {
        var result = new ArrayList<String>();
        try (var connection = connection()) {
            for (String query : List.of(
                    "SELECT manifest, manifest_sha256 FROM repo_catalog.catalog_revisions ORDER BY document_id, revision",
                    "SELECT sha256, content FROM repo_catalog.catalog_schema_artifacts ORDER BY sha256",
                    "SELECT operation_id, outcome FROM repo_catalog.catalog_operation_outcomes ORDER BY account_id, operation_id",
                    "SELECT event_id, payload FROM repo_catalog.catalog_outbox ORDER BY sequence")) {
                try (var rows = connection.createStatement().executeQuery(query)) {
                    while (rows.next()) result.add(rows.getString(1) + ":" + rows.getString(2));
                }
            }
        }
        return List.copyOf(result);
    }

    private static java.sql.Connection connection() throws Exception {
        return DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
    }

    private static NodeAddress address(String account) {
        return NodeAddress.newBuilder().setAccountId(account).setGraphId("intake:" + account)
                .setGraphAddressId("ds").setDocId("doc").build();
    }

    private static RepoServiceConfig config(long readBudget) {
        return new RepoServiceConfig(0, new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                S3.getEndpoint().toString(), S3.getRegion(), S3.getAccessKey(), S3.getSecretKey(), "it-maint", 0,
                null, null, 0, 0L, false, 0L, 0L, false, true, 0L, null, null, null, null, null, null,
                false, ManagedStoragePolicy.disabled(), readBudget);
    }

    private static Descriptors.FileDescriptor schema() {
        try {
            return Descriptors.FileDescriptor.buildFrom(FileDescriptorProto.newBuilder().setName("maintenance.proto")
                    .setPackage("maintenance.v1").setSyntax("proto3").addMessageType(DescriptorProto.newBuilder().setName("Record")
                            .addField(FieldDescriptorProto.newBuilder().setName("title").setNumber(1)
                                    .setType(FieldDescriptorProto.Type.TYPE_STRING))
                            .addField(FieldDescriptorProto.newBuilder().setName("exact_value").setNumber(99)
                                    .setType(FieldDescriptorProto.Type.TYPE_INT64))).build(), new Descriptors.FileDescriptor[0]);
        } catch (Descriptors.DescriptorValidationException failure) { throw new ExceptionInInitializerError(failure); }
    }
}
