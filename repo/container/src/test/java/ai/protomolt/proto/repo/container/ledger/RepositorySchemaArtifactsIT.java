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
                })).hasStackTraceContaining(table.endsWith("claims") && statement.startsWith("DELETE")
                        ? "recovery fence" : "future retention cleanup protocol");
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

    @Test void releasesOnlyClaimsReplacedByCurrentGenerationInBoundedBatches() {
        var first = shortOwner();
        var input = IntStream.range(0, 33).mapToObj(i -> descriptor("replace" + i + ".proto")).toList();
        artifacts.stage(first, input, () -> {});
        var next = takeover(first);
        assertThat(release(next, 10)).isZero();
        artifacts.stage(next, input, () -> {});
        assertThat(release(next, 10)).isEqualTo(10);
        assertThat(count(next, "repository_schema_artifact_claims")).isEqualTo(56);
        assertThat(release(next, 256)).isEqualTo(23);
        assertThat(release(next, 256)).isZero();
        assertThat(count(next, "repository_schema_artifact_claims")).isEqualTo(33);
        assertThat(count(next, "repository_schema_artifacts")).isEqualTo(33);
    }

    @Test void directDeleteRequiresExactCurrentOperationReplacementAndRecoveryProof() {
        var first = shortOwner();
        var bytes = descriptor("unreplaced.proto");
        artifacts.stage(first, List.of(bytes), () -> {});
        var next = takeover(first);
        var other = owner(first.key().account());
        artifacts.stage(other, List.of(bytes), () -> {});
        assertThatThrownBy(() -> tx.inTransaction(em -> { deleteGeneration(em, first); }))
                .hasStackTraceContaining("recovery fence");
        assertThatThrownBy(() -> tx.inTransaction(em -> { recovery(em, next); deleteGeneration(em, first); }))
                .hasStackTraceContaining("current-generation replacement claim");
        artifacts.stage(next, List.of(bytes), () -> {});
        assertThatThrownBy(() -> tx.inTransaction(em -> { recovery(em, next); deleteGeneration(em, next); }))
                .hasStackTraceContaining("current-generation replacement claim");
        assertThat(release(next, 256)).isEqualTo(1);
        assertThat(count(next, "repository_schema_artifact_claims")).isEqualTo(2);
    }

    @Test void releaseRollsBackAndRejectsInvalidBounds() {
        var first = shortOwner();
        var bytes = descriptor("rollback.proto");
        artifacts.stage(first, List.of(bytes), () -> {});
        var next = takeover(first);
        artifacts.stage(next, List.of(bytes), () -> {});
        var failure = new IllegalStateException("abort after release");
        assertThatThrownBy(() -> tx.inTransaction((java.util.function.Consumer<EntityManager>) em -> {
            assertThat(release(em, next, 256)).isEqualTo(1);
            throw failure;
        })).isSameAs(failure);
        assertThat(count(next, "repository_schema_artifact_claims")).isEqualTo(2);
        for (int limit : new int[]{0, -1, 257}) {
            assertThatThrownBy(() -> release(next, limit)).hasStackTraceContaining("batch limit");
        }
        assertThat(release(next, 256)).isEqualTo(1);
    }

    @Test void expiredCurrentClaimStillProtectsArtifactWhenObsoleteClaimIsReleased() {
        var first = shortOwner();
        var bytes = descriptor("expired-replacement.proto");
        artifacts.stage(first, List.of(bytes), () -> {});
        awaitExpiry(first);
        var next = ledger.takeOver(first.key(), 1, UUID.randomUUID(), Duration.ofSeconds(1));
        artifacts.stage(next, List.of(bytes), () -> {});
        awaitExpiry(next);
        assertThat(release(next, 256)).isEqualTo(1);
        assertThat(release(next, 256)).isZero();
        assertThat(count(next, "repository_schema_artifact_claims")).isEqualTo(1);
        assertThat(count(next, "repository_schema_artifacts")).isEqualTo(1);
    }

    @Test void intermediateGenerationCannotSubstituteForCurrentReplacement() {
        var first = shortOwner();
        var bytes = descriptor("three-generations.proto");
        artifacts.stage(first, List.of(bytes), () -> {});
        awaitExpiry(first);
        var second = ledger.takeOver(first.key(), 1, UUID.randomUUID(), Duration.ofSeconds(1));
        artifacts.stage(second, List.of(bytes), () -> {});
        var third = takeover(second);
        assertThat(release(third, 256)).isZero();
        assertThatThrownBy(() -> tx.inTransaction(em -> { recovery(em, third); deleteGeneration(em, first); }))
                .hasStackTraceContaining("current-generation replacement claim");
        artifacts.stage(third, List.of(bytes), () -> {});
        assertThat(release(third, 1)).isEqualTo(1);
        assertThat(release(third, 256)).isEqualTo(1);
        assertThat(release(third, 256)).isZero();
        assertThat(count(third, "repository_schema_artifact_claims")).isEqualTo(1);
        assertThat(count(third, "repository_schema_artifacts")).isEqualTo(1);
    }

    @Test void takeoverReadsClaimedSchemaAndDecodesWithoutRegistryOrGeneratedClass() throws Exception {
        var first = shortOwner();
        var file = DescriptorProtos.FileDescriptorProto.newBuilder().setName("archived.proto").setPackage("archived")
                .setSyntax("proto3").addMessageType(DescriptorProtos.DescriptorProto.newBuilder().setName("Record")
                        .addField(DescriptorProtos.FieldDescriptorProto.newBuilder().setName("value").setNumber(1)
                                .setType(DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING))).build();
        var bytes = DescriptorProtos.FileDescriptorSet.newBuilder().addFile(file).build().toByteString();
        String hash = artifacts.stage(first, List.of(bytes), () -> {}).getFirst();
        var next = takeover(first);
        assertThatThrownBy(() -> artifacts.readRetained(first, hash, () -> {})).hasMessageContaining("unavailable");
        var reopened = new RepositorySchemaArtifacts(new Tx(database.entityManagerFactory()));
        ByteString retained = reopened.readRetained(next, hash, () -> {});
        assertThat(retained).isEqualTo(bytes);
        var restoredFile = com.google.protobuf.Descriptors.FileDescriptor.buildFrom(
                DescriptorProtos.FileDescriptorSet.parseFrom(retained).getFile(0),
                new com.google.protobuf.Descriptors.FileDescriptor[0]);
        var type = restoredFile.findMessageTypeByName("Record");
        var payload = ByteString.copyFrom(new byte[]{10, 2, 'o', 'k'});
        var decoded = com.google.protobuf.DynamicMessage.parseFrom(type, payload);
        assertThat(decoded.getField(type.findFieldByName("value"))).isEqualTo("ok");
        assertThat(decoded.toByteString()).isEqualTo(payload);
        // Re-stage the retained bytes before releasing the prior generation's claim.
        assertThat(reopened.stage(next, List.of(retained), () -> {})).containsExactly(hash);
        assertThat(release(next, 256)).isEqualTo(1);
        assertThat(reopened.readRetained(next, hash, () -> {})).isEqualTo(bytes);
    }

    @Test void retainedReadRequiresOwnerIdentityAndAnOperationClaim() {
        var owner = owner();
        String hash = artifacts.stage(owner, List.of(descriptor("private.proto")), () -> {}).getFirst();
        var unclaimed = owner(owner.key().account());
        var otherAccount = owner();
        for (var wrong : List.of(unclaimed, otherAccount,
                new RepositoryOperationLedger.Owner(owner.key(), owner.generation(), UUID.randomUUID(), owner.leaseUntil()),
                new RepositoryOperationLedger.Owner(new RepositoryOperationLedger.Key(owner.key().account(), "other", owner.key().operationId()),
                        owner.generation(), owner.token(), owner.leaseUntil()))) {
            assertThatThrownBy(() -> artifacts.readRetained(wrong, hash, () -> {})).hasMessageContaining("unavailable");
        }
        assertThatThrownBy(() -> artifacts.readRetained(owner, "0".repeat(64), () -> {})).hasMessageContaining("unavailable");
        assertThatThrownBy(() -> artifacts.readRetained(owner, hash.toUpperCase(java.util.Locale.ROOT), () -> {}))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test void retainedReadRejectsExpiredOwnerAndCancellation() {
        var owner = shortOwner();
        String hash = artifacts.stage(owner, List.of(descriptor("expired-read.proto")), () -> {}).getFirst();
        var stop = new CancellationException("cancel retained read");
        var calls = new AtomicInteger();
        assertThatThrownBy(() -> artifacts.readRetained(owner, hash, () -> {
            if (calls.incrementAndGet() == 2) throw stop;
        })).isSameAs(stop);
        awaitExpiry(owner);
        assertThatThrownBy(() -> artifacts.readRetained(owner, hash, () -> {})).hasMessageContaining("unavailable");
    }

    private static int release(RepositoryOperationLedger.Owner owner, int limit) {
        return tx.inTransaction(em -> { return release(em, owner, limit); });
    }
    private static int release(EntityManager em, RepositoryOperationLedger.Owner owner, int limit) {
        return ((Number) em.createNativeQuery("SELECT release_repository_replaced_schema_claims(:account,:principal,:id,:limit)")
                .setParameter("account", owner.key().account()).setParameter("principal", owner.key().principal())
                .setParameter("id", owner.key().operationId()).setParameter("limit", limit).getSingleResult()).intValue();
    }
    private static void recovery(EntityManager em, RepositoryOperationLedger.Owner owner) {
        em.createNativeQuery("SELECT fence_repository_operation_recovery(:account,:principal,:id)")
                .setParameter("account", owner.key().account()).setParameter("principal", owner.key().principal())
                .setParameter("id", owner.key().operationId()).getSingleResult();
    }
    private static void deleteGeneration(EntityManager em, RepositoryOperationLedger.Owner owner) {
        em.createNativeQuery("DELETE FROM repository_schema_artifact_claims WHERE operation_id=:id AND owner_generation=:generation")
                .setParameter("id", owner.key().operationId()).setParameter("generation", owner.generation()).executeUpdate();
    }
    private static RepositoryOperationLedger.Owner shortOwner() {
        return ledger.admit(new RepositoryOperationLedger.Key(UUID.randomUUID().toString(), "principal", UUID.randomUUID()),
                new RepositoryOperationLedger.EncodedCommand("test.fixture", 1, ByteString.copyFromUtf8("command")),
                UUID.randomUUID(), Duration.ofSeconds(2)).owner().orElseThrow();
    }
    private static RepositoryOperationLedger.Owner takeover(RepositoryOperationLedger.Owner owner) {
        awaitExpiry(owner);
        return ledger.takeOver(owner.key(), owner.generation(), UUID.randomUUID(), Duration.ofMinutes(1));
    }
    private static void awaitExpiry(RepositoryOperationLedger.Owner owner) {
        tx.readOnly(em -> em.createNativeQuery("""
                SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM lease_until-clock_timestamp()))+0.02)
                FROM repository_operation_owners WHERE operation_id=:id
                """).setParameter("id", owner.key().operationId()).getSingleResult());
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
