package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.blob.spi.*;
import ai.protomolt.proto.repo.container.ledger.*;
import ai.protomolt.proto.repo.engine.RawObjectRecovery;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.*;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;

/** Durable worker recovery against actual PostgreSQL and S3, including lost acknowledgements. */
@Testcontainers
class RawObjectRecoveryIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    @Container static final LocalStackContainer S3 = new LocalStackContainer(
            DockerImageName.parse("localstack/localstack:3.8")).withServices("s3");
    static final String GENERATION = "recovery-test";
    static final String BUCKET = "recovery-test";
    static LedgerDatabase database;
    static Tx tx;
    static RawObjectLedger raw;
    static ManagedBackendLedger backends;
    static OpenedBlobStore opened;
    static ManagedBackendLedger.Profile profile;

    @BeforeAll static void boot() {
        database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        tx = new Tx(database.entityManagerFactory());
        raw = new RawObjectLedger(tx);
        backends = new ManagedBackendLedger(tx);
        profile = new ManagedBackendLedger.Profile(ai.protomolt.proto.repo.blob.s3.S3BackendIdentity.of(S3.getEndpoint().toString(), S3.getRegion(), true), "test-realm");
        backends.bind(GENERATION, profile);
        opened = BlobStores.discover().open("s3", Map.ofEntries(
                Map.entry("endpoint", S3.getEndpoint().toString()),
                Map.entry("region", S3.getRegion()),
                Map.entry("access-key", S3.getAccessKey()),
                Map.entry("secret-key", S3.getSecretKey()),
                Map.entry("path-style", "true"),
                Map.entry("conditional-writes", "false"),
                Map.entry("credentials-mode", "static"),
                Map.entry("api-call-timeout-ms", "300000"),
                Map.entry("api-attempt-timeout-ms", "60000"),
                Map.entry("connection-timeout-ms", "10000"),
                Map.entry("socket-timeout-ms", "60000")));
        opened.ensureNamespace(BUCKET);
    }
    @AfterAll static void close() throws Exception {
        if (opened != null) opened.close();
        if (database != null) database.close();
    }

    @Test void expiredCandidateIsReclaimedAndLatePutIsRemovedAfterWorkerRestart() {
        var candidate = candidate(GENERATION);
        expire(candidate);
        assertThat(worker().recover(candidate.rawId, Instant.now())).isEqualTo(RawObjectRecovery.Outcome.RECLAIMED);
        assertThat(raw.find(candidate.rawId).orElseThrow().state).isEqualTo(RawObjectRecord.DELETED);
        assertMissing(candidate);
        put(candidate); // a provider request completes after the old cleanup pass
        assertThat(worker().recover(candidate.rawId, Instant.now())).isEqualTo(RawObjectRecovery.Outcome.RECLAIMED);
        assertMissing(candidate);
        assertThat(raw.find(candidate.rawId).orElseThrow().cleanupAttempts).isEqualTo(2);
    }

    @Test void activeLeaseIsSkippedBeforeResolvingBackend() {
        var candidate = candidate(GENERATION);
        var worker = new RawObjectRecovery(raw, backends, (generation, physical) -> {
            throw new AssertionError("Active upload must not resolve a cleanup backend");
        });
        assertThat(worker.recover(candidate.rawId, Instant.now())).isEqualTo(RawObjectRecovery.Outcome.SKIPPED);
        assertThat(opened.store().get(BUCKET, candidate.objectKey).data()).containsExactly((byte) 7);
    }

    @Test void unknownHistoricalGenerationIsDurableFailureAndNeverUsesCurrentStore() {
        var candidate = candidate("missing-generation");
        expire(candidate);
        var worker = new RawObjectRecovery(raw, backends, (generation, physical) -> {
            throw new AssertionError("Missing profile must not fall back to current storage");
        });
        assertThatThrownBy(() -> worker.recover(candidate.rawId, Instant.now()))
                .isInstanceOf(IllegalStateException.class).hasRootCauseMessage("Original managed backend profile is unavailable");
        var failed = raw.find(candidate.rawId).orElseThrow();
        assertThat(failed.state).isEqualTo(RawObjectRecord.DELETING);
        assertThat(failed.cleanupError).isEqualTo("BACKEND_RECLAMATION_FAILED");
        assertThat(opened.store().get(BUCKET, candidate.objectKey).data()).containsExactly((byte) 7);
    }

    @Test void lostDeleteAcknowledgementRemainsRetryableAcrossWorkerRestart() {
        var candidate = candidate(GENERATION);
        expire(candidate);
        var injected = new IllegalStateException("injected acknowledgement loss");
        var uncertain = new RawObjectRecovery(raw, backends, (generation, physical) -> (bucket, key) -> {
            opened.reclaimer().reclaim(bucket, key);
            throw injected;
        });
        assertThatThrownBy(() -> uncertain.recover(candidate.rawId, Instant.now()))
                .isInstanceOf(IllegalStateException.class).hasCause(injected);
        assertMissing(candidate);
        assertThat(raw.find(candidate.rawId).orElseThrow().state).isEqualTo(RawObjectRecord.DELETING);
        assertThat(worker().recover(candidate.rawId, Instant.now())).isEqualTo(RawObjectRecovery.Outcome.RECLAIMED);
        assertThat(raw.find(candidate.rawId).orElseThrow().cleanupError).isNull();
    }

    @Test void supersededCleanupCannotCompleteAnotherWorkersClaim() {
        var candidate = candidate(GENERATION);
        expire(candidate);
        var successor = new java.util.concurrent.atomic.AtomicReference<RawObjectRecord>();
        var superseded = new RawObjectRecovery(raw, backends, (generation, physical) -> (bucket, key) -> {
            successor.set(raw.claimCleanup(candidate.rawId, Instant.now().plusSeconds(1)).orElseThrow());
            return opened.reclaimer().reclaim(bucket, key);
        });
        assertThat(superseded.recover(candidate.rawId, Instant.now())).isEqualTo(RawObjectRecovery.Outcome.SUPERSEDED);
        assertMissing(candidate);
        var pending = raw.find(candidate.rawId).orElseThrow();
        assertThat(pending.state).isEqualTo(RawObjectRecord.DELETING);
        assertThat(pending.cleanupToken).isEqualTo(successor.get().cleanupToken);
        assertThat(worker().recover(candidate.rawId, Instant.now())).isEqualTo(RawObjectRecovery.Outcome.RECLAIMED);
    }

    @Test void unconfirmedAbsenceKeepsCleanupPendingUntilAnotherPass() {
        var candidate = candidate(GENERATION);
        expire(candidate);
        var uncertain = new RawObjectRecovery(raw, backends, (generation, physical) -> (bucket, key) -> {
            opened.reclaimer().reclaim(bucket, key);
            // Inject an unconfirmed final observation after actual deletion.
            // This is a failure outcome, never a fabricated successful backend.
            return false;
        });
        assertThat(uncertain.recover(candidate.rawId, Instant.now())).isEqualTo(RawObjectRecovery.Outcome.RETRY_REQUIRED);
        var pending = raw.find(candidate.rawId).orElseThrow();
        assertThat(pending.state).isEqualTo(RawObjectRecord.DELETING);
        assertThat(pending.cleanupError).isEqualTo("RECLAMATION_INCOMPLETE");
        assertThat(worker().recover(candidate.rawId, Instant.now())).isEqualTo(RawObjectRecovery.Outcome.RECLAIMED);
    }

    private static RawObjectRecovery worker() {
        return new RawObjectRecovery(raw, backends, (generation, physical) -> {
            if (!GENERATION.equals(generation) || !profile.equals(physical))
                throw new IllegalStateException("Original backend is not configured");
            return opened.reclaimer();
        });
    }
    private static RawObjectRecord candidate(String generation) {
        var row = raw.begin(new RawObjectLedger.Location("account", generation, UUID.randomUUID(), "drive", BUCKET,
                "blobs/.protomolt-managed/v1/" + UUID.randomUUID()), 1, "application/octet-stream", Duration.ofMinutes(5));
        put(row);
        return row;
    }
    private static void put(RawObjectRecord row) {
        opened.store().put(new BlobStore.PutSpec(BUCKET, row.objectKey, row.contentType, null, null), new byte[] {7});
    }
    private static void expire(RawObjectRecord row) {
        tx.inTransaction(em -> { em.find(RawObjectRecord.class, row.rawId).leaseUntil = Instant.now().minusSeconds(10); });
    }
    private static void assertMissing(RawObjectRecord row) {
        assertThatThrownBy(() -> opened.store().get(BUCKET, row.objectKey)).isInstanceOf(BlobStore.BlobNotFoundException.class);
    }
}
