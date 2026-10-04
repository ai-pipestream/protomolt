package ai.protomolt.proto.repo.container.ledger;

import com.google.protobuf.ByteString;
import com.google.protobuf.DescriptorProtos;
import jakarta.persistence.EntityManager;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

/** Real SQL staging tests. Schema and payload validation run before this storage boundary. */
@Testcontainers
class RepositorySchemaArtifactsIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static LedgerDatabase database;
    private static Tx tx;
    private static RepositoryOperationLedger ledger;
    private static RepositorySchemaArtifacts artifacts;

    @BeforeAll static void open() {
        database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        tx = new Tx(database.entityManagerFactory());
        ledger = new RepositoryOperationLedger(tx);
        artifacts = new RepositorySchemaArtifacts(tx);
    }
    @AfterAll static void close() { database.close(); }

    @Test void retainsExactDescriptorBytesAndReplaysWithoutDuplicateClaims() {
        var owner = owner();
        var bytes = descriptor("record.proto");
        var hashes = artifacts.stage(owner, List.of(bytes, bytes), () -> {});
        assertThat(artifacts.stage(owner, List.of(bytes), () -> {})).isEqualTo(hashes);
        byte[] stored = tx.readOnly(em -> (byte[]) em.createNativeQuery(
                "SELECT artifact_bytes FROM repository_schema_artifacts WHERE account_id=:account")
                .setParameter("account", owner.key().account()).getSingleResult());
        assertThat(stored).isEqualTo(bytes.toByteArray());
        assertThat(count(owner, "repository_schema_artifacts")).isEqualTo(1);
        assertThat(count(owner, "repository_schema_artifact_claims")).isEqualTo(1);
    }

    @Test void identicalBytesAreSharedWithinAccountButClaimsBelongToEachOperation() {
        var first = owner();
        var second = owner(first.key().account());
        var otherAccount = owner();
        var bytes = descriptor("shared.proto");
        assertThat(artifacts.stage(first, List.of(bytes), () -> {}))
                .isEqualTo(artifacts.stage(second, List.of(bytes), () -> {}))
                .isEqualTo(artifacts.stage(otherAccount, List.of(bytes), () -> {}));
        assertThat(count(first, "repository_schema_artifacts")).isEqualTo(1);
        assertThat(count(first, "repository_schema_artifact_claims")).isEqualTo(2);
        assertThat(count(otherAccount, "repository_schema_artifacts")).isEqualTo(1);
    }

    @Test void generationCountLimitIncludesEarlierCallsAndAllowsReplayAtLimit() {
        var owner = owner();
        var bytes = IntStream.range(0, 64).mapToObj(i -> descriptor("file" + i + ".proto")).toList();
        artifacts.stage(owner, bytes.subList(0, 32), () -> {});
        artifacts.stage(owner, bytes.subList(32, 64), () -> {});
        assertThat(artifacts.stage(owner, bytes, () -> {})).hasSize(64);
        assertThatThrownBy(() -> artifacts.stage(owner, List.of(descriptor("excess.proto")), () -> {}))
                .hasStackTraceContaining("operation generation limits");
        assertThat(count(owner, "repository_schema_artifacts")).isEqualTo(64);
        assertThat(count(owner, "repository_schema_artifact_claims")).isEqualTo(64);
    }

    @Test void generationByteLimitIncludesEarlierCallsAndAllowsReplayAtLimit() {
        var owner = owner();
        // Synthetic storage-sized bytes: SQL does not claim protobuf validation.
        var input = IntStream.range(0, 4).mapToObj(i -> {
            byte[] bytes = new byte[RepositorySchemaArtifacts.MAX_ARTIFACT_BYTES];
            bytes[0] = (byte) i;
            return ByteString.copyFrom(bytes);
        }).toList();
        for (var bytes : input) artifacts.stage(owner, List.of(bytes), () -> {});
        artifacts.stage(owner, List.of(input.getFirst()), () -> {});
        assertThatThrownBy(() -> artifacts.stage(owner, List.of(ByteString.copyFromUtf8("excess")), () -> {}))
                .hasStackTraceContaining("operation generation limits");
        assertThat(count(owner, "repository_schema_artifacts")).isEqualTo(4);
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void directClaimInsertCannotBypassAccumulatedLimits(boolean byteLimit) {
        var owner = owner();
        if (byteLimit) {
            for (int i = 0; i < 4; i++) {
                byte[] bytes = new byte[RepositorySchemaArtifacts.MAX_ARTIFACT_BYTES];
                bytes[0] = (byte) i;
                artifacts.stage(owner, List.of(ByteString.copyFrom(bytes)), () -> {});
            }
        } else {
            artifacts.stage(owner, IntStream.range(0, 64).mapToObj(i -> descriptor("direct" + i + ".proto")).toList(), () -> {});
        }
        var other = owner(owner.key().account());
        String extra = artifacts.stage(other, List.of(descriptor("extra.proto")), () -> {}).getFirst();
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            RepositoryOperationLedger.fenceLiveOwner(em, owner);
            em.createNativeQuery("""
                    INSERT INTO repository_schema_artifact_claims(account_id,principal,operation_id,owner_generation,artifact_sha256)
                    VALUES(:account,'principal',:operation,:generation,:sha)
                    """).setParameter("account", owner.key().account()).setParameter("operation", owner.key().operationId())
                    .setParameter("generation", owner.generation()).setParameter("sha", java.util.HexFormat.of().parseHex(extra)).executeUpdate();
        })).hasStackTraceContaining("operation generation limits");
        assertThat(count(owner, "repository_schema_artifact_claims")).isEqualTo(byteLimit ? 5 : 65);
    }

    @Test void newCatalogRowCannotCommitWithoutItsCreatorsClaim() {
        var owner = owner();
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            RepositoryOperationLedger.fenceLiveOwner(em, owner);
            insertCatalog(em, owner);
        })).hasStackTraceContaining("requires an atomic staging claim");
        assertThat(count(owner, "repository_schema_artifacts")).isZero();
    }

    @Test void anotherOperationsClaimDoesNotSatisfyCreatorClaim() {
        var owner = owner();
        var other = owner(owner.key().account());
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            RepositoryOperationLedger.fenceLiveOwner(em, owner);
            RepositoryOperationLedger.fenceLiveOwner(em, other);
            insertCatalog(em, owner);
            claim(em, other);
        })).hasStackTraceContaining("requires an atomic staging claim");
        assertThat(count(owner, "repository_schema_artifacts")).isZero();
    }

    @Test void directSqlRequiresOwnerFenceAndMatchingHash() {
        var owner = owner();
        assertThatThrownBy(() -> tx.inTransaction(em -> { insertCatalog(em, owner); }))
                .hasStackTraceContaining("requires a live owner write fence");
        assertThatThrownBy(() -> tx.inTransaction(em -> {
            RepositoryOperationLedger.fenceLiveOwner(em, owner);
            em.createNativeQuery("""
                    SELECT stage_repository_schema_artifact(:account,'principal',:operation,1,
                    sha256(decode('01','hex')),decode('02','hex'))
                    """).setParameter("account", owner.key().account()).setParameter("operation", owner.key().operationId()).getSingleResult();
        })).hasStackTraceContaining("Invalid schema artifact size or digest");
        assertThat(count(owner, "repository_schema_artifacts")).isZero();
    }

    @Test void cancellationAfterFirstInsertRollsBackWholeBatch() {
        var owner = owner();
        var calls = new AtomicInteger();
        assertThatThrownBy(() -> artifacts.stage(owner, List.of(descriptor("a.proto"), descriptor("b.proto")), () -> {
            if (calls.incrementAndGet() == 7) throw new CancellationException("cancel after first insert");
        })).isInstanceOf(CancellationException.class);
        assertThat(count(owner, "repository_schema_artifacts")).isZero();
        assertThat(count(owner, "repository_schema_artifact_claims")).isZero();
    }

    @Test void staleOwnerCannotStageEvenIdenticalBytes() {
        var owner = owner();
        var wrong = new RepositoryOperationLedger.Owner(owner.key(), owner.generation() + 1, owner.token(), owner.leaseUntil());
        var bytes = descriptor("same.proto");
        artifacts.stage(owner, List.of(bytes), () -> {});
        assertThatThrownBy(() -> artifacts.stage(wrong, List.of(bytes), () -> {}))
                .isInstanceOf(RepositoryOperationLedger.OwnerFencedException.class);
        assertThat(count(owner, "repository_schema_artifact_claims")).isEqualTo(1);
    }

    @Test void expiredAndReplacedOwnersCannotStage() {
        var owner = ledger.admit(new RepositoryOperationLedger.Key(UUID.randomUUID().toString(), "principal", UUID.randomUUID()),
                new RepositoryOperationLedger.EncodedCommand("test.fixture", 1, ByteString.copyFromUtf8("command")),
                UUID.randomUUID(), Duration.ofSeconds(1)).owner().orElseThrow();
        tx.readOnly(em -> em.createNativeQuery("""
                SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM lease_until-clock_timestamp()))+0.02)
                FROM repository_operation_owners WHERE operation_id=:operation
                """).setParameter("operation", owner.key().operationId()).getSingleResult());
        var bytes = descriptor("expired.proto");
        assertThatThrownBy(() -> artifacts.stage(owner, List.of(bytes), () -> {}))
                .isInstanceOf(RepositoryOperationLedger.OwnerFencedException.class);
        var replacement = ledger.takeOver(owner.key(), owner.generation(), UUID.randomUUID(), Duration.ofMinutes(5));
        artifacts.stage(replacement, List.of(bytes), () -> {});
        assertThatThrownBy(() -> artifacts.stage(owner, List.of(bytes), () -> {}))
                .isInstanceOf(RepositoryOperationLedger.OwnerFencedException.class);
        assertThat(count(owner, "repository_schema_artifact_claims")).isEqualTo(1);
    }

    @Test void catalogAndClaimsCannotBeMutatedBeforeCleanupProtocolExists() {
        var owner = owner();
        artifacts.stage(owner, List.of(descriptor("immutable.proto")), () -> {});
        for (String table : List.of("repository_schema_artifacts", "repository_schema_artifact_claims")) {
            for (String statement : List.of("DELETE FROM " + table,
                    "UPDATE " + table + " SET account_id=account_id")) {
                assertThatThrownBy(() -> tx.inTransaction(em -> {
                    RepositoryOperationLedger.fenceLiveOwner(em, owner);
                    em.createNativeQuery(statement + " WHERE account_id=:account")
                            .setParameter("account", owner.key().account()).executeUpdate();
                })).hasStackTraceContaining("future retention cleanup protocol");
            }
        }
        assertThat(count(owner, "repository_schema_artifacts")).isEqualTo(1);
        assertThat(count(owner, "repository_schema_artifact_claims")).isEqualTo(1);
    }

    @Test void rejectsEmptyAndOversizedRequestsBeforeSql() {
        var owner = owner();
        assertThatThrownBy(() -> artifacts.stage(owner, List.of(), () -> {})).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> artifacts.stage(owner, List.of(ByteString.EMPTY), () -> {})).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> artifacts.stage(owner, java.util.Collections.nCopies(65, descriptor("a.proto")), () -> {}))
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(count(owner, "repository_schema_artifacts")).isZero();
    }

    private static void insertCatalog(EntityManager em, RepositoryOperationLedger.Owner owner) {
        em.createNativeQuery("""
                INSERT INTO repository_schema_artifacts(account_id,artifact_sha256,artifact_bytes,
                creator_principal,creator_operation,creator_generation)
                VALUES(:account,sha256(decode('01','hex')),decode('01','hex'),'principal',:operation,1)
                """).setParameter("account", owner.key().account()).setParameter("operation", owner.key().operationId()).executeUpdate();
    }
    private static void claim(EntityManager em, RepositoryOperationLedger.Owner owner) {
        em.createNativeQuery("""
                INSERT INTO repository_schema_artifact_claims VALUES(:account,'principal',:operation,1,sha256(decode('01','hex')))
                """).setParameter("account", owner.key().account()).setParameter("operation", owner.key().operationId()).executeUpdate();
    }
    private static long count(RepositoryOperationLedger.Owner owner, String table) {
        return tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table + " WHERE account_id=:account")
                .setParameter("account", owner.key().account()).getSingleResult()).longValue());
    }
    private static RepositoryOperationLedger.Owner owner() { return owner(UUID.randomUUID().toString()); }
    private static RepositoryOperationLedger.Owner owner(String account) {
        return ledger.admit(new RepositoryOperationLedger.Key(account, "principal", UUID.randomUUID()),
                new RepositoryOperationLedger.EncodedCommand("test.fixture", 1, ByteString.copyFromUtf8("command")),
                UUID.randomUUID(), Duration.ofMinutes(5)).owner().orElseThrow();
    }
    private static ByteString descriptor(String name) {
        return DescriptorProtos.FileDescriptorSet.newBuilder().addFile(DescriptorProtos.FileDescriptorProto.newBuilder()
                .setName(name).setSyntax("proto3").addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("Record")))
                .build().toByteString();
    }
}
