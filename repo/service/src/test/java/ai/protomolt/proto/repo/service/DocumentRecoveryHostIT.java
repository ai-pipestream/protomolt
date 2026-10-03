package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.blob.spi.BlobStores;
import ai.protomolt.proto.repo.container.ledger.LedgerConfig;
import java.sql.DriverManager;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
class DocumentRecoveryHostIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    @Container static final LocalStackContainer STORAGE = new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");

    @Test void rebuiltLibraryHostRecoversDurableAbandonedAttempt() throws Exception {
        var config = new RepoServiceConfig(0,
                new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()),
                STORAGE.getEndpoint().toString(), STORAGE.getRegion(), STORAGE.getAccessKey(), STORAGE.getSecretKey(),
                "recovery-host", 0, null, null, null, null, 0, 0L)
                .withManagedStorage(new ManagedStoragePolicy("original", "realm", true));
        UUID attempt = UUID.randomUUID(), node = UUID.randomUUID();
        String namespace = "recovery-host";
        String key = "documents/account/" + node + "/attempts/" + attempt + "/core";
        try (var observer = BlobStores.discover().open("s3", Map.of(
                "endpoint", STORAGE.getEndpoint().toString(), "region", STORAGE.getRegion(),
                "access-key", STORAGE.getAccessKey(), "secret-key", STORAGE.getSecretKey(), "path-style", "true", "conditional-writes", "false"));
             var connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())) {
            observer.ensureNamespace(namespace);
            try (var firstHost = RepoServices.build(config)) {
                // SQL admission fixture uses the real immutable-plan guards. No public writer exists yet.
                connection.setAutoCommit(false);
                try (var insert = connection.prepareStatement("""
                        INSERT INTO document_part_attempts(attempt_id,node_id,account_id,sampled_revision,
                            backend_generation,storage_realm,storage_namespace,planned_count,source_count,lease_token,lease_until,state)
                        VALUES (?,?,'account',0,'original','realm',?,1,0,?,clock_timestamp()+interval '2 seconds','PLANNING')
                        """)) {
                    insert.setObject(1, attempt); insert.setObject(2, node); insert.setString(3, namespace);
                    insert.setObject(4, UUID.randomUUID()); insert.executeUpdate();
                }
                try (var insert = connection.prepareStatement("""
                        INSERT INTO document_part_attempt_objects(attempt_id,ordinal,part,sub_key,storage_realm,
                            storage_namespace,object_key,expected_size,expected_sha256,content_type)
                        VALUES (?,0,1,'','realm',?,?,3,?,'application/x-protobuf')
                        """)) {
                    insert.setObject(1, attempt); insert.setString(2, namespace); insert.setString(3, key);
                    insert.setString(4, "ab".repeat(32)); insert.executeUpdate();
                }
                try (var statement = connection.createStatement()) {
                    statement.executeUpdate("UPDATE document_part_attempts SET state='STAGING'");
                }
                connection.commit(); connection.setAutoCommit(true);
                observer.store().put(new BlobStore.PutSpec(namespace, key, "application/x-protobuf", Map.of(), null), new byte[] {1,2,3});
                // Leave unverified bytes as if the writer lost its acknowledgement, then close this host.
            }
            try (var wait = connection.prepareStatement("""
                    SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM lease_until-clock_timestamp()))+0.05)
                    FROM document_part_attempts WHERE attempt_id=?
                    """)) { wait.setObject(1, attempt); wait.execute(); }
            assertThat(observer.store().get(namespace, key).data()).containsExactly((byte) 1, (byte) 2, (byte) 3);
            try (var secondHost = RepoServices.build(config)) {
                secondHost.repository(); // Library entry point must start qualified recovery too.
                boolean absent = false;
                long deadline = System.nanoTime() + java.time.Duration.ofSeconds(15).toNanos();
                while (System.nanoTime() < deadline) {
                    try (var query = connection.prepareStatement("SELECT state FROM document_part_attempt_cleanup WHERE attempt_id=?")) {
                        query.setObject(1, attempt);
                        try (var rows = query.executeQuery()) { absent = rows.next() && "ABSENT".equals(rows.getString(1)); }
                    }
                    if (absent) break;
                    Thread.sleep(50);
                }
                assertThat(absent).as("host recovery records observed absence").isTrue();
                assertThatThrownBy(() -> observer.store().get(namespace, key)).isInstanceOf(BlobStore.BlobNotFoundException.class);
                try (var query = connection.createStatement(); var rows = query.executeQuery("SELECT count(*) FROM documents")) {
                    assertThat(rows.next()).isTrue(); assertThat(rows.getLong(1)).isZero();
                }
            }
        }
    }
}
