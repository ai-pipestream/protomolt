package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/** Competing ACL administration waits behind the actual publication transaction. */
final class HistoricalPublicCommitWinnerProbe {
    static void run(Tx observer, HistoricalAuthorizationCommitGate gate, DocumentPublicationRepository repository,
            RepositoryCaller caller, PublishDocumentRequest request, UUID node, boolean removeRead) throws Exception {
        var command = new DocumentPublicationCommand(request.getIntent());
        var key = new RepositoryOperationLedger.Key(command.intent().getAccountId(), caller.principalName(), command.operationId());
        var saved = observer.readOnly(em -> (String) em.createNativeQuery("SELECT security::text FROM documents WHERE node_id=:id")
                .setParameter("id", node).getSingleResult());
        var security = DocumentSecurity.newBuilder().addPermissions(AccessRule.newBuilder().setIdentityType("public")
                .setIdentity("public").setAccess(removeRead ? Access.ACCESS_WRITE : Access.ACCESS_READ)).build();
        var json = com.google.protobuf.util.JsonFormat.printer().print(security);
        gate.armPublication(key);
        try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var publication = workers.submit(() -> repository.publishDocument(caller, request, RepositoryReadControl.NONE));
            try {
                int publisher = gate.awaitWriter();
                var writerPid = new AtomicInteger();
                var writer = workers.submit(() -> observer.withTimeouts(new SqlTimeouts(Duration.ofSeconds(12), Duration.ofSeconds(15)))
                        .inTransaction(em -> {
                            writerPid.set(((Number) em.createNativeQuery("SELECT pg_backend_pid()").getSingleResult()).intValue());
                            int changed = em.createNativeQuery("UPDATE documents SET security=CAST(:security AS jsonb) WHERE node_id=:id")
                                    .setParameter("security", json).setParameter("id", node).executeUpdate();
                            require(changed == 1, "ACL writer selects one document");
                        }));
                awaitBlocked(observer, publisher, writerPid, writer);
                require(!publication.isDone() && success(observer, key) == null, "publication remains uncommitted while ACL writer waits");
                gate.commit();
                gate.awaitCommitted();
                writer.get(10, TimeUnit.SECONDS);
                var committed = success(observer, key);
                require(committed != null, "publication committed before ACL administration completed");
                gate.release();
                if (removeRead) {
                    try { publication.get(10, TimeUnit.SECONDS); throw new AssertionError("Revoked READ received committed publication"); }
                    catch (ExecutionException failure) { denied(failure.getCause()); }
                    try { repository.publishDocument(caller, request, RepositoryReadControl.NONE); throw new AssertionError("Revoked READ replayed receipt"); }
                    catch (RepositoryException failure) { denied(failure); }
                } else {
                    var result = publication.get(10, TimeUnit.SECONDS);
                    DocumentPublicationResponseValidator.requireValid(command, caller.principalName(), result);
                    require(result.hasCommitted() && result.getCommitted().equals(committed), "WRITE revocation preserves authorized receipt delivery");
                    require(repository.publishDocument(caller, request, RepositoryReadControl.NONE).equals(result),
                            "WRITE revocation preserves exact committed replay with READ access");
                }
                require(success(observer, key).equals(committed), "ACL change cannot replace committed result");
                for (String table : java.util.List.of("repository_operation_success", "document_revision_commits", "document_assessment_owners")) {
                    long count = observer.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table
                                    + " WHERE account_id=:a AND principal=:p AND operation_id=:o")
                            .setParameter("a", key.account()).setParameter("p", key.principal()).setParameter("o", key.operationId())
                            .getSingleResult()).longValue());
                    require(count == 1, "one publication record in " + table);
                }
            } finally { gate.release(); }
        } finally {
            observer.inTransaction(em -> {
                require(em.createNativeQuery("UPDATE documents SET security=CAST(:security AS jsonb) WHERE node_id=:id")
                        .setParameter("security", saved).setParameter("id", node).executeUpdate() == 1, "restore original ACL");
            });
        }
    }

    private static void awaitBlocked(Tx tx, int publisher, AtomicInteger writer, Future<?> pending) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < until) {
            if (pending.isDone()) { pending.get(); throw new AssertionError("ACL writer did not wait for publication"); }
            if (writer.get() != 0 && tx.readOnly(em -> (Boolean) em.createNativeQuery(
                    "SELECT :publisher=ANY(pg_blocking_pids(:writer))")
                    .setParameter("publisher", publisher).setParameter("writer", writer.get()).getSingleResult())) return;
            Thread.sleep(10);
        }
        throw new AssertionError("ACL writer did not block on exact publisher PID");
    }

    private static DocumentPublicationResult success(Tx tx, RepositoryOperationLedger.Key key) throws Exception {
        var rows = tx.readOnly(em -> em.createNativeQuery("SELECT result_bytes FROM repository_operation_success"
                        + " WHERE account_id=:a AND principal=:p AND operation_id=:o", byte[].class)
                .setParameter("a", key.account()).setParameter("p", key.principal()).setParameter("o", key.operationId()).getResultList());
        return rows.isEmpty() ? null : DocumentPublicationResult.parseFrom((byte[]) rows.getFirst());
    }
    private static void denied(Throwable failure) {
        require(failure instanceof RepositoryException denied && denied.code() == RepositoryException.Code.NOT_FOUND,
                "current READ access guards committed receipt: " + failure);
    }
    private static void require(boolean condition, String message) { if (!condition) throw new AssertionError(message); }
}
