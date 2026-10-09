package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.BackendIdentity;
import com.google.protobuf.ByteString;
import jakarta.persistence.EntityManager;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

/** SQL lifecycle only. Synthetic byte declarations are not provider verification or command admission. */
@Testcontainers
class OperationBoundDocumentAttemptIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static LedgerDatabase database;
    private static Tx tx;
    private static RepositoryOperationLedger operations;
    private static final String GENERATION = "bound-attempt-backend";
    private static final String SHA = "a".repeat(64);

    @BeforeAll static void open() {
        database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        tx = new Tx(database.entityManagerFactory()); operations = new RepositoryOperationLedger(tx);
        new ManagedBackendLedger(tx).bind(GENERATION, new ManagedBackendLedger.Profile(
                new BackendIdentity("test-location", "test-location/v1", Map.of("endpoint", "synthetic-sql-fixture")), "realm"));
    }
    @AfterAll static void close() { if (database != null) database.close(); }

    private record Fixture(RepositoryOperationLedger.Owner owner, UUID id, UUID node, UUID token) {
        String key(int ordinal) { return "documents/account/" + node + "/attempts/" + id + "/part-" + ordinal; }
    }

    @ParameterizedTest @ValueSource(booleans = {false, true})
    void sparseNewContentWithZeroOrOneUploadedCorePreservesCatalogAndSource(boolean core) {
        var f = fixture(Duration.ofMinutes(1));
        admit(f, core, 60);
        var attempt = new DocumentPartAttemptLedger(tx).find(f.id).orElseThrow();
        assertThat(attempt.planKind()).isEqualTo("NEW_CONTENT");
        assertThat(attempt.state()).isEqualTo("STAGING");
        java.util.List<?> ordinals = tx.readOnly(em -> em.createNativeQuery(
                "SELECT revision_ordinal FROM document_part_attempt_objects WHERE attempt_id=:id ORDER BY ordinal")
                .setParameter("id", f.id).getResultList());
        assertThat(ordinals.toArray()).containsExactly(core ? 0 : 2, 5);
        assertThat(count("repository_physical_locations", "source_id", f.id)).isEqualTo(2);
        assertThat(count("document_part_key_reservations", "attempt_id", f.id)).isEqualTo(2);
        assertThat(count("document_part_attempt_sources", "attempt_id", f.id)).isEqualTo(1);
        select(f);
        tx.inTransaction(em -> {
            RepositoryOperationLedger.fenceLiveOwner(em, f.owner);
            em.createNativeQuery("UPDATE document_part_attempt_objects SET verified=true WHERE attempt_id=:id")
                    .setParameter("id", f.id).executeUpdate();
            em.createNativeQuery("UPDATE document_part_attempts SET state='VERIFIED' WHERE attempt_id=:id")
                    .setParameter("id", f.id).executeUpdate();
        });
        assertThat(new DocumentPartAttemptLedger(tx).find(f.id).orElseThrow().state()).isEqualTo("VERIFIED");
    }

    @Test void unfencedAdmissionAndFailedBatchLeaveNoAttemptOrLocations() {
        var f = fixture(Duration.ofMinutes(1));
        assertThatThrownBy(() -> tx.inTransaction(em -> { insert(em, f, false, 60); }))
                .hasStackTraceContaining("requires a live owner write fence");
        assertThatThrownBy(() -> tx.inTransaction((java.util.function.Consumer<EntityManager>) em -> {
            RepositoryOperationLedger.fenceLiveOwner(em, f.owner);
            insert(em, f, false, 60);
            throw new IllegalStateException("cancel all admission");
        })).hasMessage("cancel all admission");
        assertThat(new DocumentPartAttemptLedger(tx).find(f.id)).isEmpty();
        assertThat(count("repository_physical_locations", "source_id", f.id)).isZero();
        assertThat(count("document_part_key_reservations", "attempt_id", f.id)).isZero();
    }

    @ParameterizedTest @ValueSource(strings = {"renew", "state", "verify", "source"})
    void directWritesRequireCurrentTransactionProof(String mutation) {
        var f = fixture(Duration.ofMinutes(1)); admit(f, false, 60);
        String sql = switch (mutation) {
            case "renew" -> "UPDATE document_part_attempts SET lease_until=lease_until+interval '1 second' WHERE attempt_id=:id";
            case "state" -> "UPDATE document_part_attempts SET state='VERIFIED' WHERE attempt_id=:id";
            case "verify" -> "UPDATE document_part_attempt_objects SET verified=true WHERE attempt_id=:id";
            default -> "UPDATE document_part_attempt_sources SET revision=revision+1 WHERE attempt_id=:id";
        };
        assertThatThrownBy(() -> tx.inTransaction(em -> { em.createNativeQuery(sql).setParameter("id", f.id).executeUpdate(); }))
                .hasStackTraceContaining("requires a live owner write fence");
    }

    @ParameterizedTest @ValueSource(strings = {"kind", "member", "drive", "generation", "ordinal"})
    void bindingAndOriginalOrdinalCannotChangeEvenWithOwnerProof(String mutation) {
        var f = fixture(Duration.ofMinutes(1)); admit(f, false, 60);
        String sql = switch (mutation) {
            case "kind" -> "UPDATE document_part_attempts SET plan_kind='FULL_REVISION' WHERE attempt_id=:id";
            case "member" -> "UPDATE document_part_attempts SET member_id='other' WHERE attempt_id=:id";
            case "drive" -> "UPDATE document_part_attempts SET drive_id=gen_random_uuid() WHERE attempt_id=:id";
            case "generation" -> "UPDATE document_part_attempts SET operation_generation=operation_generation+1 WHERE attempt_id=:id";
            default -> "UPDATE document_part_attempt_objects SET revision_ordinal=revision_ordinal+1 WHERE attempt_id=:id";
        };
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            RepositoryOperationLedger.fenceLiveOwner(em, f.owner);
            em.createNativeQuery(sql).setParameter("id", f.id).executeUpdate();
        })).hasStackTraceContaining("immutable");
    }

    @ParameterizedTest @ValueSource(strings = {"history", "current"})
    void partialAttemptCannotUseLegacyPublication(String target) throws Exception {
        var f = fixture(Duration.ofMinutes(1)); admit(f, true, 60);
        String sql = target.equals("history")
                ? "INSERT INTO document_part_publication_history(attempt_id,node_id,publication_revision,body) VALUES (:id,:node,1,'{}')"
                : "INSERT INTO document_part_publications(attempt_id,node_id) VALUES (:id,:node)";
        try (var held = database.dataSource().getConnection()) {
            held.setAutoCommit(false);
            try (var statement = held.prepareStatement("SELECT attempt_id FROM document_part_attempts WHERE attempt_id=? FOR UPDATE")) {
                statement.setObject(1, f.id); statement.executeQuery().close();
            }
            try {
                assertThatThrownBy(() -> tx.inTransaction(em -> {
                    em.createNativeQuery("SET LOCAL lock_timeout='250ms'").executeUpdate();
                    RepositoryOperationLedger.fenceLiveOwner(em, f.owner);
                    em.createNativeQuery(sql).setParameter("id", f.id).setParameter("node", f.node).executeUpdate();
                })).hasStackTraceContaining("NEW_CONTENT requires complete revision publication");
            } finally { held.rollback(); }
        }
        var legacy = new DocumentPartAttemptLedger(tx);
        assertThatThrownBy(() -> legacy.renew(f.id, f.token, Duration.ofMinutes(1)))
                .hasMessageContaining("operation-bound entry point");
        assertThatThrownBy(() -> legacy.verify(f.id, f.token, f.key(0), 1, SHA, null, null))
                .hasMessageContaining("operation-bound entry point");
    }

    @Test void takeoverDoesNotAdoptOldAttemptAndSameGenerationCanReplaceAttempt() {
        var f = fixture(Duration.ofSeconds(1)); admit(f, false, 60);
        expire("repository_operation_owners", "operation_id", f.owner.key().operationId());
        var next = operations.takeOver(f.owner.key(), 1, UUID.randomUUID(), Duration.ofMinutes(1));
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            RepositoryOperationLedger.fenceLiveOwner(em, next);
            em.createNativeQuery("UPDATE document_part_attempt_objects SET verified=true WHERE attempt_id=:id")
                    .setParameter("id", f.id).executeUpdate();
        })).hasStackTraceContaining("requires a live owner write fence");
        var replacement = new Fixture(next, UUID.randomUUID(), f.node, UUID.randomUUID());
        var another = new Fixture(next, UUID.randomUUID(), f.node, UUID.randomUUID());
        admit(replacement, false, 60); admit(another, false, 60);
        assertThat(new DocumentPartAttemptLedger(tx).find(replacement.id)).isPresent();
        assertThat(new DocumentPartAttemptLedger(tx).find(another.id)).isPresent();
    }

    @Test void cleanupOfExpiredAttemptDoesNotNeedOperationLeaseToExpire() {
        var f = fixture(Duration.ofMinutes(1)); admit(f, false, 1);
        expire("document_part_attempts", "attempt_id", f.id);
        tx.inTransaction(em -> {
            em.createNativeQuery("""
                    INSERT INTO document_part_attempt_cleanup(attempt_id,cleanup_token,claim_until,state)
                    VALUES (:id,:token,clock_timestamp()+interval '1 minute','DELETING')
                    """).setParameter("id", f.id).setParameter("token", UUID.randomUUID()).executeUpdate();
        });
        int reclaiming = tx.readOnly(em -> ((Number) em.createNativeQuery("""
                SELECT count(*) FROM repository_object_retention r JOIN document_part_attempt_objects o ON o.physical_object_id=r.object_id
                WHERE o.attempt_id=:id AND r.reclaiming
                """).setParameter("id", f.id).getSingleResult()).intValue());
        assertThat(reclaiming).isEqualTo(2);
        for (String sql : new String[]{
                "UPDATE document_part_attempt_objects SET verified=true WHERE attempt_id=:id",
                "UPDATE document_part_attempts SET lease_until=clock_timestamp()+interval '1 minute' WHERE attempt_id=:id"}) {
            assertThatThrownBy(() -> tx.inTransaction(em -> {
                RepositoryOperationLedger.fenceLiveOwner(em, f.owner);
                em.createNativeQuery(sql).setParameter("id", f.id).executeUpdate();
            })).hasStackTraceContaining(sql.contains("attempt_objects")
                    ? "verification requires a live staging lease" : "lease is expired or shortened");
        }
    }

    @Test void migrationPreservesLegacyAttemptAndPhysicalIdentity() throws Exception {
        String schema = "bound_attempt_migration";
        org.flywaydb.core.Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .schemas(schema).defaultSchema(schema).locations("classpath:db/migration/repo").target("35").load().migrate();
        UUID id = UUID.randomUUID(); UUID node = UUID.randomUUID(); UUID token = UUID.randomUUID(); UUID physical;
        String key = "documents/account/" + node + "/attempts/" + id + "/core";
        try (var connection = database.dataSource().getConnection()) {
            connection.setAutoCommit(false);
            try (var statement = connection.createStatement()) {
                statement.execute("SET LOCAL search_path=bound_attempt_migration");
                statement.execute("""
                        INSERT INTO managed_backend_profiles(generation,provider,identity_schema,identity_json,storage_realm)
                        VALUES ('legacy','test-location','test-location/v1','{"endpoint":"synthetic-migration"}','realm')
                        """);
            }
            try (var statement = connection.prepareStatement("""
                    INSERT INTO document_part_attempts(attempt_id,node_id,account_id,sampled_revision,backend_generation,
                        storage_realm,storage_namespace,planned_count,source_count,lease_token,lease_until,state)
                    VALUES (?,?,'account',0,'legacy','realm','namespace',1,0,?,clock_timestamp()+interval '1 minute','PLANNING')
                    """)) {
                statement.setObject(1, id); statement.setObject(2, node); statement.setObject(3, token); statement.executeUpdate();
            }
            try (var statement = connection.prepareStatement("""
                    INSERT INTO document_part_attempt_objects(attempt_id,ordinal,part,sub_key,storage_realm,storage_namespace,
                        object_key,expected_size,expected_sha256,content_type)
                    VALUES (?,0,1,'','realm','namespace',?,1,?,'application/protobuf') RETURNING physical_object_id
                    """)) {
                statement.setObject(1, id); statement.setString(2, key); statement.setString(3, SHA);
                try (var result = statement.executeQuery()) { assertThat(result.next()).isTrue(); physical = (UUID) result.getObject(1); }
            }
            try (var statement = connection.prepareStatement("UPDATE document_part_attempts SET state='STAGING' WHERE attempt_id=?")) {
                statement.setObject(1, id); statement.executeUpdate();
            }
            connection.commit();
        }
        org.flywaydb.core.Flyway.configure().dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .schemas(schema).defaultSchema(schema).locations("classpath:db/migration/repo").load().migrate();
        try (var connection = database.dataSource().getConnection(); var statement = connection.createStatement();
                var result = statement.executeQuery("""
                        SELECT a.plan_kind,a.operation_id,a.lease_token,o.ordinal,o.revision_ordinal,o.object_key,o.physical_object_id,l.object_id
                        FROM bound_attempt_migration.document_part_attempts a
                        JOIN bound_attempt_migration.document_part_attempt_objects o USING(attempt_id)
                        JOIN bound_attempt_migration.repository_physical_locations l ON l.object_id=o.physical_object_id
                        """)) {
            assertThat(result.next()).isTrue(); assertThat(result.getString(1)).isEqualTo("FULL_REVISION");
            assertThat(result.getObject(2)).isNull(); assertThat(result.getObject(3)).isEqualTo(token);
            assertThat(result.getInt(4)).isZero(); assertThat(result.getInt(5)).isZero();
            assertThat(result.getString(6)).isEqualTo(key); assertThat(result.getObject(7)).isEqualTo(physical);
            assertThat(result.getObject(8)).isEqualTo(physical); assertThat(result.next()).isFalse();
        }
    }

    private static Fixture fixture(Duration lease) {
        var key = new RepositoryOperationLedger.Key("account", "principal", UUID.randomUUID());
        var owner = operations.admit(key, new RepositoryOperationLedger.EncodedCommand("test.fixture", 1, ByteString.copyFromUtf8("unexecuted")),
                UUID.randomUUID(), lease).owner().orElseThrow();
        return new Fixture(owner, UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID());
    }

    private static void select(Fixture f) {
        var drive = new DriveRecord();
        drive.driveId = tx.readOnly(em -> (UUID) em.createNativeQuery("SELECT drive_id FROM document_part_attempts WHERE attempt_id=:id")
                .setParameter("id",f.id).getSingleResult());
        drive.accountId="account"; drive.name="bound-"+drive.driveId; drive.driveType="CUSTOM";
        drive.provider="test-location"; drive.bucket="namespace";
        new DriveLedger(tx).insert(drive);
        tx.inTransaction(em -> {
            RepositoryOperationLedger.fenceLiveOwner(em,f.owner);
            em.createNativeQuery("""
                    INSERT INTO document_operation_selections(account_id,principal,operation_id,owner_generation,
                        member_id,node_id,sampled_revision,drive_id,drive_snapshot,drive_sha256,backend_generation,
                        storage_realm,storage_namespace,upload_count,attempt_id)
                    SELECT a.account_id,a.operation_principal,a.operation_id,a.operation_generation,a.member_id,
                        a.node_id,a.sampled_revision,a.drive_id,document_operation_drive_snapshot(d),document_operation_drive_digest_v1(d),
                        a.backend_generation,a.storage_realm,a.storage_namespace,a.planned_count,a.attempt_id
                    FROM document_part_attempts a JOIN drives d ON d.drive_id=a.drive_id WHERE a.attempt_id=:id
                    """).setParameter("id",f.id).executeUpdate();
        });
    }

    private static void admit(Fixture f, boolean core, int seconds) {
        tx.inTransaction(em -> { RepositoryOperationLedger.fenceLiveOwner(em, f.owner); insert(em, f, core, seconds); });
    }

    private static void insert(EntityManager em, Fixture f, boolean core, int seconds) {
        em.createNativeQuery("""
                INSERT INTO document_part_attempts(attempt_id,node_id,account_id,sampled_revision,backend_generation,
                    storage_realm,storage_namespace,planned_count,source_count,lease_token,lease_until,state,
                    plan_kind,operation_principal,operation_id,operation_generation,member_id,drive_id)
                VALUES (:id,:node,'account',0,:backend,'realm','namespace',2,1,:token,
                    clock_timestamp()+(:seconds * interval '1 second'),'PLANNING','NEW_CONTENT','principal',:operation,:generation,'member',:drive)
                """).setParameter("id", f.id).setParameter("node", f.node).setParameter("backend", GENERATION)
                .setParameter("token", f.token).setParameter("seconds", seconds).setParameter("operation", f.owner.key().operationId())
                .setParameter("generation", f.owner.generation()).setParameter("drive", UUID.randomUUID()).executeUpdate();
        for (int i = 0; i < 2; i++) {
            int original = i == 0 ? (core ? 0 : 2) : 5;
            em.createNativeQuery("""
                    INSERT INTO document_part_attempt_objects(attempt_id,ordinal,revision_ordinal,part,sub_key,storage_realm,storage_namespace,
                        object_key,expected_size,expected_sha256,content_type)
                    VALUES (:id,:ordinal,:original,:part,:sub,'realm','namespace',:key,1,:sha,'application/protobuf')
                    """).setParameter("id", f.id).setParameter("ordinal", i).setParameter("original", original)
                    .setParameter("part", core && i == 0 ? 1 : 3).setParameter("sub", core && i == 0 ? "" : "chunk-" + original)
                    .setParameter("key", f.key(original)).setParameter("sha", SHA).executeUpdate();
        }
        em.createNativeQuery("INSERT INTO document_part_attempt_sources(attempt_id,source_node_id,revision) VALUES (:id,:source,7)")
                .setParameter("id", f.id).setParameter("source", UUID.randomUUID()).executeUpdate();
        em.createNativeQuery("UPDATE document_part_attempts SET state='STAGING' WHERE attempt_id=:id").setParameter("id", f.id).executeUpdate();
    }

    private static long count(String table, String column, UUID id) {
        return tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table + " WHERE " + column + "=:id")
                .setParameter("id", id).getSingleResult()).longValue());
    }
    private static void expire(String table, String column, UUID id) {
        tx.readOnly(em -> em.createNativeQuery("SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM lease_until-clock_timestamp()))+0.02) FROM "
                + table + " WHERE " + column + "=:id").setParameter("id", id).getSingleResult());
    }
}
