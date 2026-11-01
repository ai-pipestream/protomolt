package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.catalog.CatalogConfig;
import ai.protomolt.proto.repo.catalog.PostgresCatalog;
import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import ai.protomolt.proto.repo.v1.*;
import ai.protomolt.proto.repo.v2.DocumentPart;
import ai.protomolt.proto.repo.v2.DocumentServiceGrpc;
import ai.protomolt.proto.repo.v2.ExpectedRevision;
import ai.protomolt.proto.repo.v2.PartSelector;
import ai.protomolt.proto.repo.v2.PartWrite;
import ai.protomolt.proto.repo.v2.SaveDocumentRequest;
import ai.protomolt.proto.repo.v2.SaveOwner;
import ai.protomolt.proto.repo.v2.WriteAction;
import io.grpc.inprocess.InProcessChannelBuilder;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.management.ObjectName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The maintenance commands open the catalog and the stores only. With Kafka eventing
 * configured and a concept-scheme path that cannot load, the serving composition
 * constructs a producer or fails its boot, while the maintenance composition does
 * neither: no producer MBean, no producer or transport thread, and no internal
 * credential registration, before and after a verified read.
 */
@Testcontainers
class RepoMaintenanceCompositionIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    @Container static final LocalStackContainer S3 = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");
    private static final String UNREACHABLE_KAFKA = "127.0.0.1:1";
    @TempDir Path directory;

    @Test void maintenanceOpensNoProducerListenerOrCredentialWhereTheHostWould() throws Exception {
        String account = "narrow-" + UUID.randomUUID();
        // A database of its own, so no other test's host build has registered the credential.
        String database = "narrow_" + UUID.randomUUID().toString().replace('-', '_');
        try (var connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                var statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE " + database);
        }
        String jdbcUrl = POSTGRES.getJdbcUrl().replace("/" + POSTGRES.getDatabaseName(), "/" + database);
        Path missingScheme = directory.resolve("absent-scheme.pb");
        var hostile = config(jdbcUrl, UNREACHABLE_KAFKA).withConceptSchemePaths(List.of(missingScheme));
        var before = threadNames();

        // A fresh catalog: the command opens, finds no revision, and leaves no host footprint.
        assertThatThrownBy(() -> command("verify-core", account, 1, hostile)).hasMessageContaining("missing or purged");
        assertNoHostFootprint(before);
        try (var catalog = catalog(jdbcUrl)) {
            assertThat(catalog.credential("repo-service", RepoStepSaveRebinding.INTERNAL_STEP_CREDENTIAL_ID))
                    .as("maintenance registers no internal step credential").isEmpty();
        }

        // The serving composition with the same Kafka setting constructs the producer;
        // that is the footprint the assertions above look for.
        try (var services = RepoServices.build(config(jdbcUrl, UNREACHABLE_KAFKA))) {
            assertThat(producerBeans()).as("the host constructs a Kafka producer").isNotEmpty();
            save(services, account);
        }
        assertThat(producerBeans()).as("closing the host releases its producer").isEmpty();
        // The missing concept-scheme file stops the serving composition before the catalog.
        assertThatThrownBy(() -> RepoServices.build(hostile).close())
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Concept scheme path does not exist");

        // The same hostile configuration verifies the saved revision through the narrow
        // composition. gRPC keeps shared timer threads after the host above closed, so the
        // baseline is taken again here.
        var afterHost = threadNames();
        try (var composition = RepoMaintenanceComposition.open(hostile)) {
            var result = composition.rebuild().revision(address(account), 1, false);
            assertThat(result.tombstone()).isFalse();
            assertNoHostFootprint(afterHost);
        }
        assertThat(command("verify-core", account, 1, hostile))
                .contains("core=VERIFIED_WITH_RETAINED_SCHEMAS").contains("current_entry=NOT_REQUESTED");
        assertNoHostFootprint(afterHost);
    }

    @Test void productionMainVerifiesUnderKafkaAndUnloadableConceptSchemeEnvironment() throws Exception {
        String account = "narrow-main-" + UUID.randomUUID();
        try (var services = RepoServices.build(config(POSTGRES.getJdbcUrl(), null))) {
            save(services, account);
        }
        Path log = directory.resolve("maintenance.log");
        var builder = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Xmx256m", "-cp", System.getProperty("protomolt.test.runtimeClasspath"), RepoServiceMain.class.getName(),
                "verify-core", account, "intake:" + account, "ds", "doc", "1")
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
        env.put(RepoServiceConfig.ENV_DEFAULT_BUCKET_BASE, "it-narrow");
        env.put(RepoServiceConfig.ENV_KAFKA_BOOTSTRAP_SERVERS, UNREACHABLE_KAFKA);
        env.put(RepoServiceConfig.ENV_CONCEPT_SCHEMES, directory.resolve("absent-scheme.pb").toString());
        var process = builder.start();
        try {
            assertThat(process.waitFor(45, TimeUnit.SECONDS)).as("maintenance exits instead of serving").isTrue();
            String text = Files.readString(log);
            assertThat(process.exitValue()).withFailMessage(text).isZero();
            assertThat(text).contains("revision=1 core=VERIFIED_WITH_RETAINED_SCHEMAS")
                    .doesNotContain("repo-service listening").doesNotContain("ProducerConfig values");
        } finally {
            if (process.isAlive()) { process.destroyForcibly(); process.waitFor(10, TimeUnit.SECONDS); }
        }
    }

    private static void assertNoHostFootprint(Set<String> threadsBefore) {
        assertThat(producerBeans()).as("no Kafka producer is constructed").isEmpty();
        var added = threadNames();
        added.removeAll(threadsBefore);
        assertThat(added.stream().filter(RepoMaintenanceCompositionIT::hostThread).collect(Collectors.toList()))
                .as("no producer, transport or lifecycle thread is started").isEmpty();
    }

    private static boolean hostThread(String name) {
        String lower = name.toLowerCase(java.util.Locale.ROOT);
        return lower.contains("kafka") || lower.contains("grpc") || lower.contains("netty")
                || lower.contains("outbox") || lower.contains("lifecycle") || lower.contains("repo-service");
    }

    private static Set<String> threadNames() {
        return Thread.getAllStackTraces().keySet().stream().map(Thread::getName)
                .collect(Collectors.toCollection(java.util.HashSet::new));
    }

    private static Set<ObjectName> producerBeans() {
        try {
            return ManagementFactory.getPlatformMBeanServer().queryNames(new ObjectName("kafka.producer:*"), null);
        } catch (javax.management.MalformedObjectNameException impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static String command(String mode, String account, int revision, RepoServiceConfig config) {
        var output = new ByteArrayOutputStream();
        RepoCatalogMaintenance.run(new String[] {mode, account, "intake:" + account, "ds", "doc", Integer.toString(revision)},
                () -> RepoMaintenanceComposition.open(config), new PrintStream(output));
        return output.toString(StandardCharsets.UTF_8);
    }

    private static void save(RepoServices services, String account) throws Exception {
        String name = "narrow-save-" + UUID.randomUUID();
        services.startInProcess(name);
        var channel = InProcessChannelBuilder.forName(name).directExecutor().build();
        try {
            DriveServiceGrpc.newBlockingStub(channel).createDrive(CreateDriveRequest.newBuilder()
                    .setAccountId(account).setName("intake").setDriveType(DriveType.DRIVE_TYPE_INTAKE).build());
            var document = Document.newBuilder().setDocId("doc")
                    .setOwnership(OwnershipContext.newBuilder().setAccountId(account).setDatasourceId("ds")
                            .setSecurity(DocumentSecurity.newBuilder().addPermissions(AccessRule.newBuilder()
                                    .setIdentityType("public").setIdentity("public").setAccess(Access.ACCESS_READ))))
                    .setSearchMetadata(SearchMetadata.newBuilder().setTitle("Title 1"));
            DocumentServiceGrpc.newBlockingStub(channel).saveDocument(SaveDocumentRequest.newBuilder()
                    .setOperationId("save-1").setTarget(ai.protomolt.proto.repo.v2.DocumentTarget.newBuilder().setAddress(address(account)))
                    .setExpected(ExpectedRevision.newBuilder().setNone(true)).setDocument(document)
                    .setOwner(SaveOwner.newBuilder().setIntake(ai.protomolt.proto.repo.v2.IntakeOrigin.newBuilder().setDatasourceId("ds")))
                    .addWrites(PartWrite.newBuilder().setSelector(PartSelector.newBuilder().setPart(DocumentPart.DOCUMENT_PART_CORE))
                            .setAction(WriteAction.WRITE_ACTION_PUT)).build());
        } finally { channel.shutdownNow(); }
    }

    private static PostgresCatalog catalog(String jdbcUrl) {
        return new PostgresCatalog(new CatalogConfig(jdbcUrl, POSTGRES.getUsername(), POSTGRES.getPassword(), 2),
                RepoMaintenanceComposition.CATALOG_SCHEMA);
    }

    private static NodeAddress address(String account) {
        return NodeAddress.newBuilder().setAccountId(account).setGraphId("intake:" + account)
                .setGraphAddressId("ds").setDocId("doc").build();
    }

    private static RepoServiceConfig config(String jdbcUrl, String kafkaBootstrapServers) {
        return new RepoServiceConfig(0, new LedgerConfig(jdbcUrl, POSTGRES.getUsername(), POSTGRES.getPassword()),
                S3.getEndpoint().toString(), S3.getRegion(), S3.getAccessKey(), S3.getSecretKey(), "it-narrow", 0,
                null, null, 0, 0L, false, 0L, 0L, false, true, 0L, kafkaBootstrapServers,
                RepoServiceConfig.DEFAULT_KAFKA_TOPIC, null, null, null, null,
                false, ManagedStoragePolicy.disabled(), 0L);
    }
}
