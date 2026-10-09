package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentAdmissionPolicy;
import ai.protomolt.proto.repo.v1.*;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** Actual PostgreSQL lock observations; does not claim native publisher activation. */
@Testcontainers
class DocumentSchemaPolicyConcurrencyIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void firstActivationWaitsForAnActualPublicationThatObservedNoPolicy() throws Exception {
        try (var c = context(POSTGRES); var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            c.pool().setMaximumPoolSize(6);
            var prepared = prepare(c, 1);
            var ready = new CountDownLatch(1);
            var release = new CompletableFuture<Void>();
            var writerPid = new AtomicInteger();
            var writer = executor.submit(() -> publish(c, prepared, Fault.NONE, em -> {
                writerPid.set(((Number) em.createNativeQuery("SELECT pg_backend_pid()").getSingleResult()).intValue());
                ready.countDown(); release.join();
            }));
            try {
                assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
                var parallel = executor.submit(() -> c.tx().inTransaction(em -> {
                    em.createNativeQuery("SELECT require_document_schema_policy_absent('account')").getSingleResult();
                    return true;
                }));
                assertThat(parallel.get(10, TimeUnit.SECONDS)).as("ordinary account fences are shared").isTrue();
                var catalog = new DocumentSchemaPolicies(c.tx());
                var activation = executor.submit(() -> catalog.activate(policy("account", 100), 0, () -> {}));
                awaitBlockedBy(c, writerPid.get());
                assertThat(activation.isDone()).isFalse();
                var other = executor.submit(() -> catalog.activate(policy("other", 100), 0, () -> {}));
                assertThat(other.get(10, TimeUnit.SECONDS).revision()).isEqualTo(1);
                release.complete(null);
                assertThat(writer.get(10, TimeUnit.SECONDS).getMembersCount()).isEqualTo(1);
                assertThat(activation.get(10, TimeUnit.SECONDS).revision()).isEqualTo(1);
            } finally { release.complete(null); }
        }
    }

    @Test void publicationWaitingForFirstActivationRejectsAndRollsBackItsDocumentChanges() throws Exception {
        try (var c = context(POSTGRES); var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            c.pool().setMaximumPoolSize(5);
            var prepared = prepare(c, 1);
            var original = c.tx().readOnly(em -> em.createNativeQuery("SELECT row_to_json(d)::text FROM documents d").getSingleResult());
            var ready = new CountDownLatch(1);
            var release = new CompletableFuture<Void>();
            var updaterPid = new AtomicInteger();
            var replacement = policy("account", 100);
            // Direct SQL first activation exercises the INSERT trigger, not the
            // Java adapter's earlier advisory fence.
            var updater = executor.submit(() -> c.tx().inTransaction(em -> {
                em.createNativeQuery("INSERT INTO document_schema_policies VALUES('account',:sha,:codec,1,:bytes)")
                        .setParameter("sha", java.util.HexFormat.of().parseHex(replacement.sha256()))
                        .setParameter("codec", DocumentAdmissionPolicy.CODEC).setParameter("bytes", replacement.bytes().toByteArray()).executeUpdate();
                em.createNativeQuery("INSERT INTO document_schema_policy_current VALUES('account',1,:sha)")
                        .setParameter("sha", java.util.HexFormat.of().parseHex(replacement.sha256())).executeUpdate();
                updaterPid.set(((Number) em.createNativeQuery("SELECT pg_backend_pid()").getSingleResult()).intValue());
                ready.countDown(); release.join(); return true;
            }));
            try {
                assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
                var writer = executor.submit(() -> publish(c, prepared, Fault.NONE, em -> {}));
                awaitBlockedBy(c, updaterPid.get());
                assertThat(writer.isDone()).isFalse();
                release.complete(null);
                assertThat(updater.get(10, TimeUnit.SECONDS)).isTrue();
                assertThatThrownBy(() -> writer.get(10, TimeUnit.SECONDS))
                        .hasStackTraceContaining("Publication requires an explicit schema policy binding");
                var after = c.tx().readOnly(em -> em.createNativeQuery("SELECT row_to_json(d)::text FROM documents d").getSingleResult());
                long commits = c.tx().readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM document_revision_commits").getSingleResult()).longValue());
                assertThat(after).isEqualTo(original);
                assertThat(commits).isZero();
            } finally { release.complete(null); }
        }
    }

    @Test void writersShareThePointerAndAnUpdaterWaitsForBothWithoutBlockingAnotherAccount() throws Exception {
        try (var c = context(POSTGRES); var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            c.pool().setMaximumPoolSize(6);
            var catalog = new DocumentSchemaPolicies(c.tx());
            var initial = catalog.activate(policy("account", 100), 0, () -> {});
            catalog.activate(policy("other", 100), 0, () -> {});
            var ready = new CountDownLatch(2);
            var firstRelease = new CompletableFuture<Void>();
            var secondRelease = new CompletableFuture<Void>();
            var firstPid = new AtomicInteger(); var secondPid = new AtomicInteger();
            var first = executor.submit(() -> c.tx().inTransaction(em -> {
                DocumentSchemaPolicies.lockCurrent(em, initial, () -> {});
                firstPid.set(((Number) em.createNativeQuery("SELECT pg_backend_pid()").getSingleResult()).intValue());
                ready.countDown(); firstRelease.join(); return true;
            }));
            var second = executor.submit(() -> c.tx().inTransaction(em -> {
                DocumentSchemaPolicies.lockCurrent(em, initial, () -> {});
                secondPid.set(((Number) em.createNativeQuery("SELECT pg_backend_pid()").getSingleResult()).intValue());
                ready.countDown(); secondRelease.join(); return true;
            }));
            try {
                assertThat(ready.await(10, TimeUnit.SECONDS)).as("both writers hold their shared locks concurrently").isTrue();
                assertThat(firstPid.get()).isNotEqualTo(secondPid.get());
                var update = executor.submit(() -> catalog.activate(policy("account", 99), 1, () -> {}));
                // PostgreSQL may wait on either transaction in the shared row's
                // multixact first; which one is reported is not acquisition order.
                awaitBlockedBy(c, firstPid.get(), secondPid.get());
                assertThat(update.isDone()).isFalse();
                var other = executor.submit(() -> catalog.activate(policy("other", 99), 1, () -> {}));
                assertThat(other.get(10, TimeUnit.SECONDS).revision()).isEqualTo(2);
                firstRelease.complete(null);
                assertThat(first.get(10, TimeUnit.SECONDS)).isTrue();
                awaitBlockedBy(c, secondPid.get());
                assertThat(update.isDone()).isFalse();
                secondRelease.complete(null);
                assertThat(second.get(10, TimeUnit.SECONDS)).isTrue();
                assertThat(update.get(10, TimeUnit.SECONDS).revision()).isEqualTo(2);
                assertThat(catalog.read("account", () -> {}).policy().limits().maxRoots()).isEqualTo(99);
            } finally { firstRelease.complete(null); secondRelease.complete(null); }
        }
    }

    @Test void aWaitingWriterObservesTheCommittedNewPointerAndRejectsItsOldSelection() throws Exception {
        try (var c = context(POSTGRES); var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            c.pool().setMaximumPoolSize(5);
            var catalog = new DocumentSchemaPolicies(c.tx());
            var old = catalog.activate(policy("account", 100), 0, () -> {});
            var replacement = policy("account", 99);
            var ready = new CountDownLatch(1);
            var release = new CompletableFuture<Void>();
            var updaterPid = new AtomicInteger();
            // A real updater transaction is held before commit so the reader must
            // exercise PostgreSQL's row recheck after its lock wait.
            var updater = executor.submit(() -> c.tx().inTransaction(em -> {
                em.createNativeQuery("INSERT INTO document_schema_policies VALUES(:account,:sha,:codec,1,:bytes)")
                        .setParameter("account", "account").setParameter("sha", java.util.HexFormat.of().parseHex(replacement.sha256()))
                        .setParameter("codec", DocumentAdmissionPolicy.CODEC).setParameter("bytes", replacement.bytes().toByteArray()).executeUpdate();
                em.createNativeQuery("UPDATE document_schema_policy_current SET policy_revision=2,policy_sha256=:sha WHERE account_id='account'")
                        .setParameter("sha", java.util.HexFormat.of().parseHex(replacement.sha256())).executeUpdate();
                updaterPid.set(((Number) em.createNativeQuery("SELECT pg_backend_pid()").getSingleResult()).intValue());
                ready.countDown(); release.join(); return true;
            }));
            try {
                assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
                var writer = executor.submit(() -> c.tx().inTransaction(em -> {
                    return DocumentSchemaPolicies.lockCurrent(em, old, () -> {});
                }));
                awaitBlockedBy(c, updaterPid.get());
                release.complete(null);
                assertThat(updater.get(10, TimeUnit.SECONDS)).isTrue();
                assertThatThrownBy(() -> writer.get(10, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(DocumentSchemaPolicies.StalePolicy.class).hasStackTraceContaining("no longer active");
            } finally { release.complete(null); }
        }
    }

    private static void awaitBlockedBy(Context c, int holder) throws Exception {
        awaitBlockedBy(c, holder, holder);
    }

    private static void awaitBlockedBy(Context c, int holder, int alternative) throws Exception {
        boolean blocked = false;
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        while (!blocked && System.nanoTime() < deadline) {
            blocked = c.tx().readOnly(em -> (Boolean) em.createNativeQuery("""
                    SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database()
                     AND pid<>pg_backend_pid()
                     AND (:holder=ANY(pg_blocking_pids(pid)) OR :alternative=ANY(pg_blocking_pids(pid))))
                    """).setParameter("holder", holder).setParameter("alternative", alternative).getSingleResult());
            if (!blocked) Thread.sleep(10);
        }
        assertThat(blocked).as("a database connection is blocked by backend %s or %s", holder, alternative).isTrue();
    }

    private static DocumentAdmissionPolicy policy(String account, int roots) {
        return DocumentAdmissionPolicy.of(DocumentSchemaPolicy.newBuilder().setEncodingVersion(1).setAccountId(account)
                .setValidationProfile("protomolt-retained-schema-admission/v1")
                .setMode(DocumentSchemaPolicyMode.DOCUMENT_SCHEMA_POLICY_MODE_TYPED_REQUIRED).setRequireStructuredRoot(true)
                .setAnyResolvedSchema(true).setLimits(DocumentSchemaPolicyLimits.newBuilder().setMaxFragments(32)
                        .setMaxFragmentBytes(4_000_000).setMaxRoots(roots).setMaxEvidenceBytes(4_000_000)
                        .setMaxBindings(20).setMaxRetainedBytes(16_000_000).setMaxDecodedBytes(1_000_000)).build(), () -> {});
    }
}
