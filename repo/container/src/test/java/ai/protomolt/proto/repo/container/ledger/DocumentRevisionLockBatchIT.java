package ai.protomolt.proto.repo.container.ledger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import jakarta.persistence.EntityManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

/** Real database locking; synthetic document rows do not represent provider uploads. */
@Testcontainers
class DocumentRevisionLockBatchIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static LedgerDatabase database;
    private static Tx tx;

    @BeforeAll static void open() {
        database = new LedgerDatabase(new LedgerConfig(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword()));
        tx = new Tx(database.entityManagerFactory());
    }
    @AfterAll static void close() { if (database != null) database.close(); }

    @Test void independentDestinationsCanShareAnAdmissionSource() {
        var source=UUID.randomUUID(); var first=UUID.randomUUID(); var second=UUID.randomUUID();
        insert(List.of(source,first,second));
        try (var holder=database.entityManagerFactory().createEntityManager()) {
            holder.getTransaction().begin();
            try {
                DocumentRevisionLocks.lock(holder,Set.of(first),Set.of(source));
                tx.inTransaction(em -> {
                    em.createNativeQuery("SET LOCAL lock_timeout='300ms'").executeUpdate();
                    assertThat(DocumentRevisionLocks.lock(em,Set.of(second),Set.of(source))).hasSize(2);
                });
            } finally { holder.getTransaction().rollback(); }
        }
    }

    @Test void largeInterleavedAdmissionHasBoundedStatements() {
        var sources=new java.util.HashSet<UUID>(); var destinations=new java.util.HashSet<UUID>();
        for (int i=0;i<10000;i++) sources.add(new UUID(0,100000+2L*i));
        for (int i=0;i<64;i++) destinations.add(new UUID(0,100001+312L*i));
        var all=new ArrayList<>(sources); all.addAll(destinations); insert(all);
        var statistics=database.entityManagerFactory().unwrap(org.hibernate.SessionFactory.class).getStatistics();
        statistics.setStatisticsEnabled(true); statistics.clear();
        long started=System.nanoTime();
        try {
            var locked=tx.inTransaction(em -> { return DocumentRevisionLocks.lock(em,destinations,sources); });
            assertThat(locked).hasSize(10064);
            assertThat(statistics.getPrepareStatementCount()).isLessThanOrEqualTo(1+40+128);
            System.out.printf("mixed_revision_lock sources=10000 destinations=64 client_statements=%d elapsed_ms=%.3f%n",
                    statistics.getPrepareStatementCount(),(System.nanoTime()-started)/1_000_000.0);
        } finally { statistics.setStatisticsEnabled(false); }
    }

    @Test void admissionRejectsStaleCachedPolicyAfterWaiting() {
        var id=UUID.randomUUID(); insert(List.of(id));
        tx.inTransaction(em -> {
            em.find(DocumentRecord.class,id);
            tx.inTransaction(other -> { other.createNativeQuery("UPDATE documents SET security='{\"inheritanceEnabled\":true}' WHERE node_id=:id")
                    .setParameter("id",id).executeUpdate(); });
            assertThatThrownBy(() -> DocumentRevisionLocks.lock(em,Set.of(),Set.of(id)))
                    .isInstanceOf(DocumentLedger.RevisionConflictException.class);
        });
    }

    @Test void mixedAdvisoryFunctionRejectsMalformedAndOversizedInputs() {
        for (String arguments : List.of("NULL::bigint[],ARRAY[]::bigint[]", "ARRAY[NULL]::bigint[],ARRAY[]::bigint[]",
                "ARRAY[]::bigint[],array_fill(1::bigint,ARRAY[65])",
                "ARRAY[[1,2],[3,4]]::bigint[],ARRAY[]::bigint[]",
                "ARRAY(SELECT generate_series(1,10064)::bigint),ARRAY[10065]::bigint[]")) {
            assertThatThrownBy(() -> tx.inTransaction(em -> {
                em.createNativeQuery("SELECT lock_document_admission_keys("+arguments+")").getSingleResult();
            })).hasStackTraceContaining("non-null one-dimensional bounds");
        }
    }

    @Test void admissionSourcesBlockDirectPolicyChangesAndDeletion() {
        var source=UUID.randomUUID(); insert(List.of(source));
        try (var holder=database.entityManagerFactory().createEntityManager()) {
            holder.getTransaction().begin();
            try {
                DocumentRevisionLocks.lock(holder,Set.of(),Set.of(source));
                for (String sql : List.of("UPDATE documents SET security='{}' WHERE node_id=:id",
                        "DELETE FROM documents WHERE node_id=:id")) {
                    assertThatThrownBy(() -> tx.inTransaction(em -> {
                        em.createNativeQuery("SET LOCAL lock_timeout='100ms'").executeUpdate();
                        em.createNativeQuery(sql).setParameter("id",source).executeUpdate();
                    })).hasStackTraceContaining("lock timeout");
                }
            } finally { holder.getTransaction().rollback(); }
        }
    }

    @Test void overlappingAndAliasedAdmissionKeysChooseExclusiveModeBeforeAcquiringLocks() {
        var source=new UUID(10,20); var destination=new UUID(20,10);
        insert(List.of(source,destination));
        try (var holder=database.entityManagerFactory().createEntityManager()) {
            holder.getTransaction().begin();
            try {
                DocumentRevisionLocks.lock(holder,Set.of(destination),Set.of(source,destination));
                long sharedAdvisories=((Number)holder.createNativeQuery("""
                        SELECT count(*) FROM pg_locks WHERE pid=pg_backend_pid()
                        AND locktype='advisory' AND mode='ShareLock'
                        """).getSingleResult()).longValue();
                assertThat(sharedAdvisories).isZero();
                assertThatThrownBy(() -> tx.inTransaction(em -> {
                    em.createNativeQuery("SET LOCAL lock_timeout='100ms'").executeUpdate();
                    DocumentRevisionLocks.lock(em,Set.of(),Set.of(source));
                })).hasStackTraceContaining("lock timeout");
                tx.inTransaction(em -> {
                    // Promotion of the aliased advisory key must not turn a
                    // read-only source row into an exclusive row lock.
                    em.createNativeQuery("SELECT node_id FROM documents WHERE node_id=:id FOR SHARE NOWAIT")
                            .setParameter("id",source).getSingleResult();
                });
                assertThatThrownBy(() -> tx.inTransaction(em -> {
                    em.createNativeQuery("SELECT node_id FROM documents WHERE node_id=:id FOR SHARE NOWAIT")
                            .setParameter("id",destination).getSingleResult();
                })).hasStackTraceContaining("could not obtain lock");
            } finally { holder.getTransaction().rollback(); }
        }
    }

    @Test void admissionPreservesGlobalRowOrderAcrossLockModes() throws Exception {
        var source=new UUID(0,401); var destination=new UUID(0,402);
        insert(List.of(source,destination));
        try (var deleter=database.entityManagerFactory().createEntityManager();
             var executor=Executors.newVirtualThreadPerTaskExecutor()) {
            deleter.getTransaction().begin();
            deleter.createNativeQuery("SELECT node_id FROM documents WHERE node_id=:id FOR UPDATE")
                    .setParameter("id",source).getSingleResult();
            var pid=new CompletableFuture<Integer>();
            var future=executor.submit(() -> tx.inTransaction(em -> {
                pid.complete(pid(em));
                return DocumentRevisionLocks.lock(em,Set.of(destination),Set.of(source));
            }));
            try {
                awaitLock(pid.get(10,TimeUnit.SECONDS));
                // A destination-first implementation would already hold this row.
                deleter.createNativeQuery("SELECT node_id FROM documents WHERE node_id=:id FOR UPDATE NOWAIT")
                        .setParameter("id",destination).getSingleResult();
                deleter.getTransaction().commit();
                assertThat(future.get(10,TimeUnit.SECONDS)).hasSize(2);
            } finally { if(deleter.getTransaction().isActive()) deleter.getTransaction().rollback(); }
        }
    }

    @ParameterizedTest @ValueSource(ints = {1, 257, 10000})
    void locksExistingRowsWithBoundedClientStatements(int count) {
        var ids = new ArrayList<UUID>();
        for (int i = 0; i < count; i++) ids.add(UUID.randomUUID());
        insert(ids);
        var sources = revisions(ids);
        var statistics = database.entityManagerFactory().unwrap(org.hibernate.SessionFactory.class).getStatistics();
        statistics.setStatisticsEnabled(true);
        statistics.clear();
        long started = System.nanoTime();
        try {
            var locked = tx.inTransaction(em -> { return DocumentLedger.lockRevisions(em, Set.of(), sources); });
            assertThat(locked).hasSize(count);
            locked.forEach((id, row) -> assertThat(row.mutationRevision).isEqualTo(sources.get(id)));
            assertThat(statistics.getPrepareStatementCount()).isEqualTo(1 + (count + 255) / 256);
            System.out.printf("revision_lock nodes=%d client_statements=%d elapsed_ms=%.3f%n", count,
                    statistics.getPrepareStatementCount(), (System.nanoTime() - started) / 1_000_000.0);
        } finally { statistics.setStatisticsEnabled(false); }
    }

    @Test void rejectsStalePersistenceContextAfterPolicyChange() {
        var id = UUID.randomUUID();
        insert(List.of(id));
        tx.inTransaction(em -> {
            var cached = em.find(DocumentRecord.class, id);
            tx.inTransaction(other -> { other.createNativeQuery("UPDATE documents SET security=CAST(:acl AS jsonb) WHERE node_id=:id")
                    .setParameter("acl", "{\"inheritanceEnabled\":true}").setParameter("id", id).executeUpdate(); });
            assertThat(cached.readSecurity().getInheritanceEnabled()).isFalse();
            assertThatThrownBy(() -> DocumentLedger.lockRevisions(em, Set.of(id), Map.of()))
                    .isInstanceOf(DocumentLedger.RevisionConflictException.class);
        });
    }

    @Test void rejectsStalePersistenceContextAfterSameTransactionSql() {
        var id = UUID.randomUUID();
        insert(List.of(id));
        tx.inTransaction(em -> {
            em.find(DocumentRecord.class, id);
            em.createNativeQuery("UPDATE documents SET filename='changed' WHERE node_id=:id").setParameter("id", id).executeUpdate();
            assertThatThrownBy(() -> DocumentLedger.lockRevisions(em, Set.of(id), Map.of()))
                    .isInstanceOf(DocumentLedger.RevisionConflictException.class);
        });
    }

    @Test void unchangedManagedRowRemainsUsable() {
        var id = UUID.randomUUID();
        insert(List.of(id));
        tx.inTransaction(em -> {
            var cached = em.find(DocumentRecord.class, id);
            var locked = DocumentLedger.lockRevisions(em, Set.of(id), Map.of(id, cached.mutationRevision));
            assertThat(locked.get(id)).isSameAs(cached);
        });
    }

    @Test void distinguishesAbsentDestinationsFromMissingOrStaleSources() {
        var present = UUID.randomUUID();
        var absent = UUID.randomUUID();
        insert(List.of(present));
        var sources = revisions(List.of(present));
        var result = tx.inTransaction(em -> { return DocumentLedger.lockRevisions(em, Set.of(present, absent), sources); });
        assertThat(result).containsKey(absent);
        assertThat(result.get(absent)).isNull();
        assertThat(result.get(present)).isNotNull();
        assertThatThrownBy(() -> tx.inTransaction(em -> { return DocumentLedger.lockRevisions(em, Set.of(), Map.of(absent, 1L)); }))
                .isInstanceOf(DocumentLedger.RevisionConflictException.class);
        assertThatThrownBy(() -> tx.inTransaction(em -> { return DocumentLedger.lockRevisions(em, Set.of(), Map.of(present, sources.get(present) + 1)); }))
                .isInstanceOf(DocumentLedger.RevisionConflictException.class);
    }

    @Test void absentDestinationWaitsForCompetingCreatorAndSeesCommittedRow() throws Exception {
        var id = UUID.randomUUID();
        try (var holder = database.entityManagerFactory().createEntityManager();
             var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            holder.getTransaction().begin();
            DocumentLedger.lockRevisions(holder, Set.of(id), Map.of());
            var pid = new CompletableFuture<Integer>();
            var future = executor.submit(() -> tx.inTransaction(em -> {
                pid.complete(pid(em));
                return DocumentLedger.lockRevisions(em, Set.of(id), Map.of()).get(id);
            }));
            try {
                awaitLock(pid.get(10, TimeUnit.SECONDS));
                insert(holder, List.of(id));
                holder.getTransaction().commit();
                assertThat(future.get(10, TimeUnit.SECONDS).nodeId).isEqualTo(id);
            } finally { if (holder.getTransaction().isActive()) holder.getTransaction().rollback(); }
        }
    }

    @Test void completesAllAdvisoryLocksBeforeAnyRowLocksAndIndependentWorkContinues() throws Exception {
        var first = new UUID(0, 100);
        var last = new UUID(0, 200);
        var independent = new UUID(0, 300);
        insert(List.of(first, last, independent));
        try (var holder = database.entityManagerFactory().createEntityManager();
             var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            holder.getTransaction().begin();
            holder.createNativeQuery("SELECT 1 FROM pg_advisory_xact_lock(200)").getSingleResult();
            var pid = new CompletableFuture<Integer>();
            var future = executor.submit(() -> tx.inTransaction(em -> {
                pid.complete(pid(em));
                return DocumentLedger.lockRevisions(em, new LinkedHashSet<>(List.of(last, first)), Map.of());
            }));
            try {
                awaitLock(pid.get(10, TimeUnit.SECONDS));
                tx.inTransaction(em -> {
                    em.createNativeQuery("SELECT node_id FROM documents WHERE node_id=:id FOR UPDATE NOWAIT")
                            .setParameter("id", first).getSingleResult();
                    assertThat(DocumentLedger.lockRevisions(em, Set.of(independent), Map.of())).containsKey(independent);
                });
                holder.getTransaction().commit();
                assertThat(future.get(10, TimeUnit.SECONDS)).hasSize(2);
            } finally { if (holder.getTransaction().isActive()) holder.getTransaction().rollback(); }
        }
    }

    @ParameterizedTest @ValueSource(ints = {2, 257})
    void preservesJavaUuidOrderForAliasedAdvisoryKeys(int count) throws Exception {
        // Java sorts the negative MSB first; PostgreSQL UUID order does the opposite.
        // Both UUIDs alias the same advisory key, so row order is independently observable.
        var first = new UUID(Long.MIN_VALUE + count, 0);
        var last = new UUID(0, Long.MIN_VALUE + count);
        var ids = new ArrayList<UUID>();
        ids.add(first);
        for (int i = 1; i < count - 1; i++) ids.add(new UUID(first.getMostSignificantBits(), i));
        ids.add(last);
        insert(ids);
        try (var holder = database.entityManagerFactory().createEntityManager();
             var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            holder.getTransaction().begin();
            holder.createNativeQuery("SELECT node_id FROM documents WHERE node_id=:id FOR UPDATE")
                    .setParameter("id", last).getSingleResult();
            var pid = new CompletableFuture<Integer>();
            var future = executor.submit(() -> tx.inTransaction(em -> {
                pid.complete(pid(em));
                return DocumentLedger.lockRevisions(em, new LinkedHashSet<>(ids.reversed()), Map.of());
            }));
            try {
                awaitLock(pid.get(10, TimeUnit.SECONDS));
                assertThatThrownBy(() -> tx.inTransaction(em -> { return em.createNativeQuery(
                        "SELECT node_id FROM documents WHERE node_id=:id FOR UPDATE NOWAIT")
                        .setParameter("id", first).getSingleResult(); })).isInstanceOf(jakarta.persistence.PersistenceException.class);
                holder.getTransaction().commit();
                assertThat(future.get(10, TimeUnit.SECONDS)).hasSize(count);
            } finally { if (holder.getTransaction().isActive()) holder.getTransaction().rollback(); }
        }
    }

    @Test void sourceChangedDuringRowLockWaitIsRejected() throws Exception {
        var id = UUID.randomUUID();
        insert(List.of(id));
        var expected = revisions(List.of(id));
        try (var holder = database.entityManagerFactory().createEntityManager();
             var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            holder.getTransaction().begin();
            holder.createNativeQuery("UPDATE documents SET filename='new-revision' WHERE node_id=:id")
                    .setParameter("id", id).executeUpdate();
            var pid = new CompletableFuture<Integer>();
            var future = executor.submit(() -> tx.inTransaction(em -> {
                pid.complete(pid(em));
                return DocumentLedger.lockRevisions(em, Set.of(), expected);
            }));
            try {
                awaitLock(pid.get(10, TimeUnit.SECONDS));
                holder.getTransaction().commit();
                assertThatThrownBy(() -> future.get(10, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(DocumentLedger.RevisionConflictException.class);
            } finally { if (holder.getTransaction().isActive()) holder.getTransaction().rollback(); }
        }
    }

    @Test void reversedOverlapping257NodeBatchesSerialize() throws Exception {
        var ids = new ArrayList<UUID>();
        for (int i = 0; i < 257; i++) ids.add(UUID.randomUUID());
        insert(ids);
        try (var holder = database.entityManagerFactory().createEntityManager();
             var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            holder.getTransaction().begin();
            assertThat(DocumentLedger.lockRevisions(holder, new LinkedHashSet<>(ids), Map.of())).hasSize(257);
            var pid = new CompletableFuture<Integer>();
            var future = executor.submit(() -> tx.inTransaction(em -> {
                pid.complete(pid(em));
                return DocumentLedger.lockRevisions(em, new LinkedHashSet<>(ids.reversed()), Map.of());
            }));
            try {
                awaitLock(pid.get(10, TimeUnit.SECONDS));
                holder.getTransaction().commit();
                assertThat(future.get(10, TimeUnit.SECONDS)).hasSize(257);
            } finally { if (holder.getTransaction().isActive()) holder.getTransaction().rollback(); }
        }
    }

    @Test void lockTimeoutRollsBackAndReleasesAlreadyAcquiredKeys() throws Exception {
        var first = new UUID(0, 401);
        var last = new UUID(0, 402);
        try (var holder = database.entityManagerFactory().createEntityManager();
             var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            holder.getTransaction().begin();
            holder.createNativeQuery("SELECT 1 FROM pg_advisory_xact_lock(402)").getSingleResult();
            var pid = new CompletableFuture<Integer>();
            var future = executor.submit(() -> tx.inTransaction(em -> {
                int backend = pid(em);
                // Leave a wider timeout than awaitLock's five-second observation
                // window, including on a loaded CI host.
                em.createNativeQuery("SET LOCAL lock_timeout='8s'").executeUpdate();
                pid.complete(backend);
                return DocumentLedger.lockRevisions(em, Set.of(first, last), Map.of());
            }));
            try {
                int backend = pid.get(10, TimeUnit.SECONDS);
                awaitLock(backend);
                var held = tx.readOnly(em -> em.createNativeQuery("""
                        SELECT EXISTS(SELECT 1 FROM pg_locks WHERE pid=:pid AND locktype='advisory'
                            AND classid=0 AND objid=401 AND granted)
                        """, Boolean.class).setParameter("pid", backend).getSingleResult());
                assertThat(held).isEqualTo(true);
                assertThatThrownBy(() -> future.get(10, TimeUnit.SECONDS))
                        .hasCauseInstanceOf(jakarta.persistence.PersistenceException.class);
                tx.inTransaction(em -> {
                    assertThat(em.createNativeQuery("SELECT pg_try_advisory_xact_lock(401)", Boolean.class)
                            .getSingleResult()).isEqualTo(true);
                });
            } finally { holder.getTransaction().rollback(); }
        }
    }

    @Test void boundsAndInvalidArraysFailBeforeLocks() {
        try (var em = database.entityManagerFactory().createEntityManager()) {
            assertThatThrownBy(() -> DocumentLedger.lockRevisions(em, Set.of(UUID.randomUUID()), Map.of()))
                    .isInstanceOf(IllegalStateException.class);
        }
        var excessive = new LinkedHashSet<UUID>();
        for (int i = 0; i < 10065; i++) excessive.add(new UUID(0, i));
        var statistics = database.entityManagerFactory().unwrap(org.hibernate.SessionFactory.class).getStatistics();
        statistics.setStatisticsEnabled(true);
        statistics.clear();
        try {
            var empty = tx.inTransaction(em -> { return DocumentLedger.lockRevisions(em, Set.of(), Map.of()); });
            assertThat(empty).isEmpty();
            assertThatThrownBy(() -> tx.inTransaction(em -> { return DocumentLedger.lockRevisions(em, excessive, Map.of()); }))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThat(statistics.getPrepareStatementCount()).isZero();
        } finally { statistics.setStatisticsEnabled(false); }
        for (String invalid : List.of("NULL::bigint[]", "ARRAY[1,NULL]::bigint[]", "ARRAY[[1,2],[3,4]]::bigint[]",
                "array_fill(1::bigint, ARRAY[10065])")) {
            assertThatThrownBy(() -> tx.inTransaction(em -> {
                return em.createNativeQuery("SELECT lock_document_revision_keys(" + invalid + ")").getSingleResult();
            })).isInstanceOf(jakarta.persistence.PersistenceException.class);
        }
        var emptyKeys = tx.inTransaction(em -> {
            return em.createNativeQuery("SELECT lock_document_revision_keys(ARRAY[]::bigint[])", Boolean.class).getSingleResult();
        });
        assertThat(emptyKeys).isEqualTo(true);
    }

    private static int pid(EntityManager em) {
        em.createNativeQuery("SET LOCAL lock_timeout='8s'").executeUpdate();
        return ((Number) em.createNativeQuery("SELECT pg_backend_pid()").getSingleResult()).intValue();
    }
    private static void awaitLock(int pid) throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
        do {
            boolean waiting = tx.readOnly(em -> !em.createNativeQuery(
                    "SELECT 1 FROM pg_stat_activity WHERE pid=:pid AND wait_event_type='Lock'")
                    .setParameter("pid", pid).getResultList().isEmpty());
            if (waiting) return;
            Thread.sleep(10);
        } while (System.nanoTime() < deadline);
        throw new AssertionError("Backend " + pid + " did not reach an observed database lock wait");
    }
    private static Map<UUID, Long> revisions(List<UUID> ids) {
        long started = System.nanoTime();
        var revisions = tx.readOnly(em -> {
            var result = new HashMap<UUID, Long>();
            for (var row : em.unwrap(org.hibernate.Session.class).createNativeQuery(
                    "SELECT d.node_id, d.mutation_revision FROM unnest(CAST(:ids AS uuid[])) requested(id) "
                            + "JOIN documents d ON d.node_id=requested.id", Object[].class)
                    .setParameter("ids", array(ids)).getResultList()) result.put((UUID) row[0], ((Number) row[1]).longValue());
            return result;
        });
        if (ids.size() >= 10000) System.out.printf("revision_lock fixture_read nodes=%d elapsed_ms=%.3f%n",
                ids.size(), (System.nanoTime() - started) / 1_000_000.0);
        return revisions;
    }
    private static String array(List<UUID> ids) {
        return "{" + String.join(",", ids.stream().map(UUID::toString).toList()) + "}";
    }
    private static void insert(List<UUID> ids) {
        long started = System.nanoTime();
        tx.inTransaction(em -> { insert(em, ids); });
        if (ids.size() >= 10000) System.out.printf("revision_lock fixture_insert nodes=%d elapsed_ms=%.3f%n",
                ids.size(), (System.nanoTime() - started) / 1_000_000.0);
    }
    private static void insert(EntityManager em, List<UUID> ids) {
        em.createNativeQuery("""
                INSERT INTO documents(node_id,doc_id,graph_address_id,graph_id,row_kind,account_id,datasource_id,
                    checksum,drive_name,object_key,etag,size_bytes,security)
                SELECT id,id::text,'source','intake:lock-test','INTAKE','lock-test','source',
                    'synthetic','test',id::text,'synthetic',0,'{}'::jsonb
                FROM unnest(CAST(:ids AS uuid[])) AS x(id)
                """).setParameter("ids", array(ids)).executeUpdate();
    }
}
