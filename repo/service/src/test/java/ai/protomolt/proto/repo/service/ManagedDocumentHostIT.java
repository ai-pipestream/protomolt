package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import java.sql.DriverManager;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;

/** Real host ownership and restart of the native publication reader incarnation. */
@Testcontainers
class ManagedDocumentHostIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    @Container static final LocalStackContainer S3 = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");

    @Test void hostOwnsFreshPublicationResourcesAndQuiescesThemBeforeDatabaseClose() throws Exception {
        var config = new RepoServiceConfig(0,
                new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                S3.getEndpoint().toString(), S3.getRegion(), S3.getAccessKey(), S3.getSecretKey(),
                "document-host", 0, null, null, null, null, 0, 0L)
                .withManagedStorage(new ManagedStoragePolicy("document-host-original", "document-host-realm", true));
        try (var database = new ai.protomolt.proto.repo.container.ledger.LedgerDatabase(config.ledger())) {
            // Invalid writer capability configuration must fail before registering
            // an archive reader, even though the backing client is a real adapter.
            var providers = ai.protomolt.proto.repo.blob.spi.BlobStores.discover();
            var options = java.util.Map.of("endpoint", S3.getEndpoint().toString(), "region", S3.getRegion(),
                    "path-style", "true", "conditional-writes", "false", "access-key", S3.getAccessKey(), "secret-key", S3.getSecretKey());
            try (var backing = providers.open("s3", options)) {
                var tx = new ai.protomolt.proto.repo.container.ledger.Tx(database.entityManagerFactory());
                var profile = new ai.protomolt.proto.repo.container.ledger.ManagedBackendLedger.Profile(
                        providers.managedIdentity("s3", options), "rejected-writer-realm");
                new ai.protomolt.proto.repo.container.ledger.ManagedBackendLedger(tx).bind("rejected-writer", profile);
                assertThatThrownBy(() -> new ManagedArchiveServices(tx,
                        new ai.protomolt.proto.repo.container.archive.ArchiveLedger(tx), backing.store(), java.util.Set.of(),
                        "rejected-writer", profile, backing.reclaimer())).isInstanceOf(UnsupportedOperationException.class);
                assertThat(count("ACTIVE")).isZero();
                assertThat(count("QUIESCED")).isZero();
            }
        }
        sql("""
                CREATE FUNCTION test_fail_second_host_reader() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                  IF EXISTS(SELECT 1 FROM repository_reader_incarnations WHERE state='ACTIVE') THEN
                    RAISE EXCEPTION 'deliberate second host reader registration failure';
                  END IF;
                  RETURN NEW;
                END $$
                """);
        sql("CREATE TRIGGER test_fail_second_host_reader BEFORE INSERT ON repository_reader_incarnations FOR EACH ROW EXECUTE FUNCTION test_fail_second_host_reader()");
        try {
            assertThatThrownBy(() -> RepoServices.build(config))
                    .hasStackTraceContaining("deliberate second host reader registration failure");
            assertThat(count("ACTIVE")).isZero();
            assertThat(count("QUIESCED")).isEqualTo(1);
        } finally {
            sql("DROP TRIGGER test_fail_second_host_reader ON repository_reader_incarnations");
            sql("DROP FUNCTION test_fail_second_host_reader()");
        }
        for (int restart = 0; restart < 2; restart++) {
            try (var host = RepoServices.build(config)) {
                assertThat(host.services()).noneMatch(service -> service instanceof DocumentHistoryGrpcService);
                host.startInProcess("document-host-" + UUID.randomUUID());
                assertThat(host.documentPublication()).isNotNull();
                assertThat(host.documentHistory()).isSameAs(host.documentHistory());
                // Managed archive and document readers share the incarnation ledger.
                assertThat(count("ACTIVE")).isEqualTo(2);
                assertThat(count("QUIESCED")).isEqualTo(1 + 2 * restart);
            }
            assertThat(count("ACTIVE")).isZero();
            assertThat(count("QUIESCED")).isEqualTo(1 + 2 * (restart + 1));
        }
        var bindings = new java.util.concurrent.atomic.AtomicInteger();
        var historical = new HistoricalReadAccess(caller -> {
            assertThat(caller.name()).isEqualTo("history-owner");
            assertThat(caller.unrestricted()).isFalse();
            bindings.incrementAndGet();
            return new ai.protomolt.proto.repo.spi.RepositoryCaller(caller.name(), false,
                    java.util.Set.of("host-account"), java.util.Set.of());
        }, 32L * 1024 * 1024, 4);
        ai.protomolt.proto.authz.CallerResolver credentials = token -> "synthetic-owner-key".equals(token)
                ? java.util.Optional.of(ai.protomolt.proto.actions.Caller.scoped("history-owner", java.util.Set.of()))
                : java.util.Optional.empty();
        for (boolean serialized : new boolean[] {false, true}) {
            try (var host = RepoServices.build(config, ai.protomolt.proto.asset.bridge.BridgeEngine.standard(), historical)) {
                assertThat(host.services()).filteredOn(service -> service instanceof DocumentHistoryGrpcService).hasSize(1);
                assertThat(host.historicalRepository()).isSameAs(host.documentHistory());
                assertThatThrownBy(() -> host.startNetty(0)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("API token");
                assertThatThrownBy(() -> host.startNetty(0, " ", null)).isInstanceOf(IllegalArgumentException.class);
                assertThatThrownBy(() -> host.startInProcess("missing-token")).isInstanceOf(IllegalArgumentException.class);
                String name = "authenticated-history-" + UUID.randomUUID();
                var server = serialized ? host.startNetty(0, "synthetic-operator-key", credentials)
                        : host.startInProcess(name, "synthetic-operator-key", credentials);
                io.grpc.ManagedChannel channel = serialized
                        ? io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder.forAddress("127.0.0.1", server.getPort()).usePlaintext().build()
                        : io.grpc.inprocess.InProcessChannelBuilder.forName(name).build();
                try {
                    var stub = ai.protomolt.proto.repo.v1.DocumentHistoryServiceGrpc.newBlockingStub(channel)
                            .withDeadlineAfter(5, java.util.concurrent.TimeUnit.SECONDS);
                    var request = ai.protomolt.proto.repo.v1.ReadRevisionRequest.newBuilder()
                            .setAddress(ai.protomolt.proto.repo.v1.NodeAddress.newBuilder().setAccountId("host-account")
                                    .setDocId("missing").setGraphId("graph").setGraphAddressId("source"))
                            .setRevisionId(UUID.randomUUID().toString())
                            .setMode(ai.protomolt.proto.repo.v1.HistoricalDocumentReadMode.HISTORICAL_DOCUMENT_READ_MODE_RAW).build();
                    int before = bindings.get();
                    assertThatThrownBy(() -> stub.readRevision(request)).isInstanceOfSatisfying(io.grpc.StatusRuntimeException.class,
                            error -> assertThat(error.getStatus().getCode()).isEqualTo(io.grpc.Status.Code.UNAUTHENTICATED));
                    assertThat(bindings.get()).isEqualTo(before);
                    var metadata = new io.grpc.Metadata();
                    metadata.put(io.grpc.Metadata.Key.of("api_token", io.grpc.Metadata.ASCII_STRING_MARSHALLER), "synthetic-owner-key");
                    var authenticated = stub.withInterceptors(io.grpc.stub.MetadataUtils.newAttachHeadersInterceptor(metadata));
                    assertThatThrownBy(() -> authenticated.readRevision(request)).isInstanceOfSatisfying(io.grpc.StatusRuntimeException.class,
                            error -> assertThat(error.getStatus().getCode()).isEqualTo(io.grpc.Status.Code.NOT_FOUND));
                    assertThat(bindings.get()).isEqualTo(before + 1);
                } finally {
                    channel.shutdownNow();
                    assertThat(channel.awaitTermination(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                }
                assertThat(count("ACTIVE")).isEqualTo(2);
            }
            assertThat(count("ACTIVE")).isZero();
        }
    }

    private static long count(String state) throws Exception {
        try (var connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                var query = connection.prepareStatement("SELECT count(*) FROM repository_reader_incarnations WHERE state=?")) {
            query.setString(1, state);
            try (var result = query.executeQuery()) { result.next(); return result.getLong(1); }
        }
    }

    private static void sql(String statement) throws Exception {
        try (var connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                var query = connection.createStatement()) {
            query.execute(statement);
        }
    }
}
