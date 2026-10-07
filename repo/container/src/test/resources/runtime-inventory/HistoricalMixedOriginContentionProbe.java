package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.*;
import java.util.Map;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;

/** A fresh origin must be locked before any candidate retention row, including historical rows. */
final class HistoricalMixedOriginContentionProbe {
    static DocumentAssessmentCreation.Created create(Tx tx, DataSource database, DocumentHistoricalExecution execution,
            RepositoryCaller caller, DocumentPublicationCommand command, DocumentPublicationAssessment.Historical assessment,
            Map<String, DocumentSelectedAttemptLedger.Selected> selected, DocumentAssessmentRuntimeObserver.Observation observation,
            DocumentAssessmentStartJournal.Started started) throws Exception {
        if (selected.size() != 1) throw new AssertionError("Contention fixture requires one fresh attempt");
        var attempt = selected.values().iterator().next().attempt();
        var objects = new HashSet<UUID>();
        for (var member : command.intent().getMembersList()) for (var part : member.getPartsList())
            if (part.hasHistoricalReuse()) objects.add(UUID.fromString(part.getHistoricalReuse().getObject().getObjectId()));
        if (objects.isEmpty()) throw new AssertionError("Contention fixture requires historical objects");
        objects.addAll(tx.readOnly(em -> em.createNativeQuery(
                "SELECT physical_object_id FROM document_part_attempt_objects WHERE attempt_id=:attempt", UUID.class)
                .setParameter("attempt", attempt).getResultList()));
        try (var workers = Executors.newVirtualThreadPerTaskExecutor(); var blocker = database.getConnection()) {
            blocker.setAutoCommit(false);
            blocker.setTransactionIsolation(java.sql.Connection.TRANSACTION_READ_COMMITTED);
            int blockerPid;
            try (var statement = blocker.prepareStatement("SELECT pg_backend_pid() FROM document_part_attempts WHERE attempt_id=? FOR UPDATE")) {
                statement.setObject(1, attempt);
                try (var rows = statement.executeQuery()) {
                    if (!rows.next()) throw new AssertionError("Fresh attempt is missing");
                    blockerPid = rows.getInt(1);
                }
            }
            var pending = workers.submit(() -> execution.createAssessment(caller, assessment, selected, observation,
                    new RepositorySchemaArtifacts(tx), started, RepositoryReadControl.NONE));
            try {
                awaitOriginWait(tx, blockerPid);
                if (pending.isDone()) throw new AssertionError("CREATE returned while its fresh origin was locked");
                try (var retention = database.getConnection()) {
                    retention.setAutoCommit(false);
                    try (var query = retention.prepareStatement("""
                            SELECT object_id FROM repository_object_retention
                            WHERE object_id=ANY(?) ORDER BY object_id FOR UPDATE NOWAIT
                            """)) {
                        var ids = retention.createArrayOf("uuid", objects.toArray());
                        try {
                            query.setArray(1, ids);
                            int count = 0;
                            try (var rows = query.executeQuery()) { while (rows.next()) count++; }
                            if (count != objects.size()) throw new AssertionError("Every historical and fresh retention row must be present and unlocked");
                            awaitOriginWait(tx, blockerPid);
                            if (pending.isDone()) throw new AssertionError("CREATE stopped waiting while retention was independently locked");
                        } finally { ids.free(); }
                    } finally { retention.rollback(); }
                }
            } finally { blocker.rollback(); }
            return pending.get(15, TimeUnit.SECONDS);
        }
    }
    private static void awaitOriginWait(Tx tx, int blocker) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            var waiting = tx.readOnly(em -> em.createNativeQuery("""
                    SELECT pid FROM pg_stat_activity WHERE :pid=ANY(pg_blocking_pids(pid))
                      AND wait_event_type='Lock'
                      AND query LIKE '%document_part_attempts%ORDER BY attempt_id FOR UPDATE%'
                    """).setParameter("pid", blocker).getResultList());
            if (waiting.size() == 1) return;
            if (!waiting.isEmpty()) throw new AssertionError("Only one CREATE should wait on the fresh origin");
            Thread.sleep(10);
        }
        throw new AssertionError("CREATE did not wait for the held fresh origin");
    }
}
