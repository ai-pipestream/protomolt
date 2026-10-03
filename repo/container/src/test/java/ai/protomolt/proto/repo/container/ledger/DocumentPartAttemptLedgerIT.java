package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.BackendIdentity;
import ai.protomolt.proto.repo.v1.DocumentPart;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
class DocumentPartAttemptLedgerIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final String SHA = "ab".repeat(32);
    private static final Duration LEASE = Duration.ofMinutes(5);

    @Test void admissionVerificationAndReplayPreserveExactPlan() {
        try (var db = database()) {
            var tx = new Tx(db.entityManagerFactory());
            var ledger = new DocumentPartAttemptLedger(tx);
            var plan = plan(tx);
            var attempt = ledger.begin(plan, LEASE);
            assertThat(attempt.state()).isEqualTo("STAGING");
            assertThat(ledger.find(attempt.id())).contains(attempt);
            assertThat(ledger.renew(attempt.id(), attempt.token(), Duration.ofSeconds(1)).leaseUntil())
                    .isEqualTo(attempt.leaseUntil());
            var actualKeys = tx.readOnly(em -> em.createNativeQuery(
                    "SELECT object_key FROM document_part_attempt_objects WHERE attempt_id=:id ORDER BY ordinal")
                    .setParameter("id", attempt.id()).getResultList());
            assertThat(actualKeys).containsExactlyElementsOf(plan.objects().stream().map(DocumentPartAttemptLedger.PlannedObject::objectKey).toList());
            for (var object : plan.objects()) {
                assertThatThrownBy(() -> ledger.verify(attempt.id(), UUID.randomUUID(), object.objectKey(), 3, SHA, null, "etag"))
                        .isInstanceOf(DocumentPartAttemptLedger.FenceException.class);
                assertThatThrownBy(() -> ledger.verify(attempt.id(), attempt.token(), object.objectKey(), 4, SHA, null, "etag"))
                        .hasMessageContaining("differs from admitted");
                assertThatThrownBy(() -> ledger.verify(attempt.id(), attempt.token(), object.objectKey(), 3, "cd".repeat(32), null, "etag"))
                        .hasMessageContaining("differs from admitted");
                var verified = ledger.verify(attempt.id(), attempt.token(), object.objectKey(), 3, SHA, null, "etag");
                assertThat(verified.state()).isEqualTo(object == plan.objects().getLast() ? "VERIFIED" : "STAGING");
                assertThat(ledger.verify(attempt.id(), attempt.token(), object.objectKey(), 3, SHA, null, "etag")).isEqualTo(verified);
                assertThatThrownBy(() -> ledger.verify(attempt.id(), attempt.token(), object.objectKey(), 3, SHA, "different", "etag"))
                        .hasMessageContaining("provider identity differs");
            }
            assertThatThrownBy(() -> ledger.verify(attempt.id(), attempt.token(), "documents/missing", 3, SHA, null, null))
                    .hasMessageContaining("not in the admitted plan");
            assertThatThrownBy(() -> ledger.renew(attempt.id(), UUID.randomUUID(), LEASE))
                    .isInstanceOf(DocumentPartAttemptLedger.FenceException.class);
        }
    }

    @Test void attemptIdentityCannotBeReusedAfterProfileRotation() {
        try (var db = database()) {
            var tx = new Tx(db.entityManagerFactory());
            var ledger = new DocumentPartAttemptLedger(tx);
            var plan = plan(tx);
            var first = ledger.begin(plan, LEASE);
            var profiles = new ManagedBackendLedger(tx);
            String rotated = "rotated-" + UUID.randomUUID();
            profiles.bind(rotated, profiles.find(plan.location().backendGeneration()).orElseThrow());
            var location = new DocumentPartAttemptLedger.Location(plan.location().nodeId(), "account", rotated, plan.location().namespace());
            assertThatThrownBy(() -> ledger.begin(new DocumentPartAttemptLedger.Plan(plan.attemptId(), location, 0, Map.of(), plan.objects()), LEASE))
                    .hasStackTraceContaining("duplicate key value violates unique constraint");
            int count = tx.readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM document_part_attempts WHERE storage_realm=:realm")
                    .setParameter("realm", first.storageRealm()).getSingleResult()).intValue());
            assertThat(count).isEqualTo(1);
            var missing = new DocumentPartAttemptLedger.Location(plan.location().nodeId(), "account", "unregistered", "bucket");
            assertThatThrownBy(() -> ledger.begin(new DocumentPartAttemptLedger.Plan(plan.attemptId(), missing, 0, Map.of(), plan.objects()), LEASE))
                    .hasMessageContaining("not registered");
        }
    }

    @Test void longUtf8CoordinatesAndSlotsDoNotOverflowIndexes() {
        try (var db = database()) {
            var tx = new Tx(db.entityManagerFactory());
            var original = plan(tx);
            // Random Unicode prevents PostgreSQL compression from hiding index width problems.
            var random = new java.util.Random(42);
            String wide = random.ints(1024, 0x4e00, 0x9fff)
                    .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();
            var location = new DocumentPartAttemptLedger.Location(original.location().nodeId(), "account",
                    original.location().backendGeneration(), wide);
            String prefix = "documents/account/" + location.nodeId() + "/attempts/" + original.attemptId() + "/";
            var objects = List.of(object(DocumentPart.DOCUMENT_PART_CORE, "", prefix + "core"),
                    object(DocumentPart.DOCUMENT_PART_CHUNKS, wide, prefix + wide + wide.substring(0, 800)));
            var ledger = new DocumentPartAttemptLedger(tx);
            var attempt = ledger.begin(new DocumentPartAttemptLedger.Plan(original.attemptId(), location, 0, Map.of(), objects), LEASE);
            for (var object : objects) ledger.verify(attempt.id(), attempt.token(), object.objectKey(), 3, SHA, null, null);
            assertThat(ledger.find(attempt.id()).orElseThrow().state()).isEqualTo("VERIFIED");
            assertThatThrownBy(() -> execute(tx,
                    "UPDATE document_part_attempt_objects SET key_digest=decode(repeat('00',32),'hex') WHERE attempt_id=:id", attempt.id()))
                    .hasStackTraceContaining("violates check constraint");
        }
    }

    @Test void expiryIsCheckedAfterWaitingForTheAttemptLock() throws Exception {
        try (var db = database(); var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var tx = new Tx(db.entityManagerFactory());
            var ledger = new DocumentPartAttemptLedger(tx);
            var attempt = ledger.begin(plan(tx), Duration.ofSeconds(1));
            var locked = new java.util.concurrent.CountDownLatch(1);
            var release = new java.util.concurrent.CountDownLatch(1);
            var holder = executor.submit(() -> tx.inTransaction(em -> {
                em.createNativeQuery("SELECT attempt_id FROM document_part_attempts WHERE attempt_id=:id FOR UPDATE")
                        .setParameter("id", attempt.id()).getSingleResult();
                locked.countDown();
                try { assertThat(release.await(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue(); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new IllegalStateException(e); }
            }));
            assertThat(locked.await(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            var renewal = executor.submit(() -> ledger.renew(attempt.id(), attempt.token(), LEASE));
            try { Thread.sleep(1100); } finally { release.countDown(); }
            holder.get(10, java.util.concurrent.TimeUnit.SECONDS);
            assertThatThrownBy(() -> renewal.get(10, java.util.concurrent.TimeUnit.SECONDS))
                    .hasCauseInstanceOf(DocumentPartAttemptLedger.FenceException.class);
        }
    }

    @Test void concurrentAdmissionCommitsOnlyOneCompletePlan() throws Exception {
        try (var db = database(); var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var tx = new Tx(db.entityManagerFactory());
            var ledger = new DocumentPartAttemptLedger(tx);
            var plan = plan(tx);
            var start = new java.util.concurrent.CountDownLatch(1);
            var futures = new java.util.ArrayList<java.util.concurrent.Future<DocumentPartAttemptLedger.Attempt>>();
            for (int i = 0; i < 2; i++) futures.add(executor.submit(() -> {
                assertThat(start.await(10, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                return ledger.begin(plan, LEASE);
            }));
            start.countDown();
            int admitted = 0;
            int refused = 0;
            for (var future : futures) {
                try {
                    assertThat(future.get(10, java.util.concurrent.TimeUnit.SECONDS).state()).isEqualTo("STAGING");
                    admitted++;
                } catch (java.util.concurrent.ExecutionException failure) {
                    assertThat(failure.getCause()).hasStackTraceContaining("document_part_attempts_pkey");
                    refused++;
                }
            }
            assertThat(admitted).isEqualTo(1);
            assertThat(refused).isEqualTo(1);
            assertThat(ledger.find(plan.attemptId()).orElseThrow().plannedCount()).isEqualTo(plan.objects().size());
        }
    }

    @Test void databaseRejectsPlanMutationAndPrematureVerification() {
        try (var db = database()) {
            var tx = new Tx(db.entityManagerFactory());
            var ledger = new DocumentPartAttemptLedger(tx);
            var attempt = ledger.begin(plan(tx), LEASE);
            for (String sql : List.of(
                    "UPDATE document_part_attempts SET account_id='other' WHERE attempt_id=:id",
                    "UPDATE document_part_attempts SET state='VERIFIED' WHERE attempt_id=:id",
                    "UPDATE document_part_attempts SET lease_until=lease_until-interval '1 second' WHERE attempt_id=:id",
                    "DELETE FROM document_part_attempts WHERE attempt_id=:id",
                    "UPDATE document_part_attempt_sources SET revision=revision+1 WHERE attempt_id=:id",
                    "DELETE FROM document_part_attempt_sources WHERE attempt_id=:id",
                    "UPDATE document_part_attempt_objects SET expected_size=4 WHERE attempt_id=:id",
                    "UPDATE document_part_attempt_objects SET ordinal=ordinal+10 WHERE attempt_id=:id",
                    "DELETE FROM document_part_attempt_objects WHERE attempt_id=:id",
                    "INSERT INTO document_part_attempt_sources VALUES (:id,gen_random_uuid(),1)")) {
                assertThatThrownBy(() -> execute(tx, sql, attempt.id())).hasStackTraceContaining("ERROR:");
            }
            assertThat(ledger.find(attempt.id())).contains(attempt);
        }
    }

    @Test void expiredWriterCannotVerifyOrRenew() throws Exception {
        try (var db = database()) {
            var tx = new Tx(db.entityManagerFactory());
            var ledger = new DocumentPartAttemptLedger(tx);
            var plan = plan(tx);
            var attempt = ledger.begin(plan, Duration.ofSeconds(1));
            Thread.sleep(1100);
            assertThatThrownBy(() -> ledger.renew(attempt.id(), attempt.token(), LEASE))
                    .isInstanceOf(DocumentPartAttemptLedger.FenceException.class);
            assertThatThrownBy(() -> ledger.verify(attempt.id(), attempt.token(), plan.objects().getFirst().objectKey(), 3, SHA, null, null))
                    .isInstanceOf(DocumentPartAttemptLedger.FenceException.class);
            assertThatThrownBy(() -> execute(tx,
                    "UPDATE document_part_attempt_objects SET verified=true WHERE attempt_id=:id", attempt.id()))
                    .hasStackTraceContaining("requires a live staging lease");
        }
    }

    @Test void incompletePlanCannotCommitEvenThroughDirectSql() {
        try (var db = database()) {
            var tx = new Tx(db.entityManagerFactory());
            var plan = plan(tx);
            UUID id = UUID.randomUUID();
            assertThatThrownBy(() -> tx.inTransaction(em -> {
                em.createNativeQuery("""
                        INSERT INTO document_part_attempts(attempt_id,node_id,account_id,sampled_revision,backend_generation,
                          storage_realm,storage_namespace,planned_count,source_count,lease_token,lease_until,state)
                        SELECT :id,:id,'account',0,generation,storage_realm,'bucket',1,0,gen_random_uuid(),
                          clock_timestamp()+interval '5 minutes','PLANNING'
                        FROM managed_backend_profiles WHERE generation=:generation
                        """).setParameter("id", id).setParameter("generation", plan.location().backendGeneration()).executeUpdate();
            })).hasStackTraceContaining("cannot commit an unsealed plan");
            assertThat(new DocumentPartAttemptLedger(tx).find(id)).isEmpty();
        }
    }

    @Test void planRejectsInvalidSlotsAndSourceRevisions() {
        var location = new DocumentPartAttemptLedger.Location(UUID.randomUUID(), "account", "generation", "bucket");
        UUID id = UUID.randomUUID();
        var core = object(DocumentPart.DOCUMENT_PART_CORE, "", "documents/account/" + location.nodeId() + "/attempts/" + id + "/core");
        assertThatThrownBy(() -> new DocumentPartAttemptLedger.Plan(id, location, 0, Map.of(), List.of()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DocumentPartAttemptLedger.Plan(id, location, 0, Map.of(), List.of(core, core)))
                .hasMessageContaining("Duplicate");
        assertThatThrownBy(() -> new DocumentPartAttemptLedger.Plan(id, location, 0, Map.of(location.nodeId(), 1L), List.of(core)))
                .hasMessageContaining("Same-node source");
        assertThatThrownBy(() -> object(DocumentPart.DOCUMENT_PART_CHUNKS, "", "documents/chunks"))
                .hasMessageContaining("slot");
        assertThatThrownBy(() -> object(DocumentPart.DOCUMENT_PART_CORE, "", "other/core"))
                .hasMessageContaining("namespace");
        for (String key : List.of("archive/documents/core", "documents/.protomolt-managed/core")) {
            assertThatThrownBy(() -> object(DocumentPart.DOCUMENT_PART_CORE, "", key)).hasMessageContaining("namespace");
        }
        assertThatThrownBy(() -> new DocumentPartAttemptLedger.Plan(UUID.randomUUID(), location, 0, Map.of(), List.of(core)))
                .hasMessageContaining("outside its document attempt");
    }

    private static LedgerDatabase database() {
        return new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
    }
    private static DocumentPartAttemptLedger.Plan plan(Tx tx) {
        String generation = "attempt-" + UUID.randomUUID();
        new ManagedBackendLedger(tx).bind(generation, new ManagedBackendLedger.Profile(
                new BackendIdentity("test-location", "test-location/v1", Map.of("endpoint", "https://storage.example")), generation));
        UUID node = UUID.randomUUID();
        var location = new DocumentPartAttemptLedger.Location(node, "account", generation, "original-container");
        UUID id = UUID.randomUUID();
        String prefix = "documents/account/" + node + "/attempts/" + id + "/";
        return new DocumentPartAttemptLedger.Plan(id, location, 7, Map.of(node, 7L, UUID.randomUUID(), 4L), List.of(
                object(DocumentPart.DOCUMENT_PART_CORE, "", prefix + "core"),
                object(DocumentPart.DOCUMENT_PART_CHUNKS, "z", prefix + "chunks-z"),
                object(DocumentPart.DOCUMENT_PART_CHUNKS, "a", prefix + "chunks-a")));
    }
    private static DocumentPartAttemptLedger.PlannedObject object(DocumentPart part, String subKey, String key) {
        return new DocumentPartAttemptLedger.PlannedObject(part, subKey, key, 3, SHA, "application/protobuf");
    }
    private static void execute(Tx tx, String sql, UUID id) {
        tx.inTransaction(em -> { em.createNativeQuery(sql).setParameter("id", id).executeUpdate(); });
    }
}
