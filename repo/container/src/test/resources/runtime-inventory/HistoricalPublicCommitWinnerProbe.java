package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/** Competing authority administration waits behind the actual publication transaction. */
final class HistoricalPublicCommitWinnerProbe {
    static void run(Tx observer, HistoricalAuthorizationCommitGate gate, DocumentPublicationRepository repository,
            RepositoryCaller caller, PublishDocumentRequest request, UUID node, String change, HistoricalObservedWriter updater) throws Exception {
        boolean credential = change.equals("credential"), policy = change.equals("policy"), removeRead = change.equals("read");
        if (!credential && !policy && !removeRead && !change.equals("write")) throw new IllegalArgumentException(change);
        var command = new DocumentPublicationCommand(request.getIntent());
        var key = new RepositoryOperationLedger.Key(command.intent().getAccountId(), caller.principalName(), command.operationId());
        var saved = observer.readOnly(em -> (String) em.createNativeQuery("SELECT security::text FROM documents WHERE node_id=:id")
                .setParameter("id", node).getSingleResult());
        var security = DocumentSecurity.newBuilder().addPermissions(AccessRule.newBuilder().setIdentityType("public")
                .setIdentity("public").setAccess(removeRead ? Access.ACCESS_WRITE : Access.ACCESS_READ)).build();
        var json = com.google.protobuf.util.JsonFormat.printer().print(security);
        var policies = new DocumentSchemaPolicies(observer);
        var originalPolicy = policy ? policies.read(key.account(), () -> {}) : null;
        var replacement = new AtomicReference<DocumentSchemaPolicies.Selection>();
        gate.armPublication(key);
        try (var workers = Executors.newVirtualThreadPerTaskExecutor()) {
            var publication = workers.submit(() -> repository.publishDocument(caller, request, RepositoryReadControl.NONE));
            try {
                int publisher = gate.awaitWriter();
                var writer = workers.submit(() -> updater.run(() -> {
                    if (credential) new RepositoryCredentialAuthorities(updater.tx).revoke(new RepositoryCaller("operator", true),
                            caller.credentialBinding().orElseThrow(), caller.principalName());
                    else if (policy) {
                        var limits = originalPolicy.policy().definition().getLimits();
                        require(limits.getMaxFragments() > 1, "valid policy limit can be reduced");
                        var updated = ai.protomolt.proto.repo.admission.DocumentAdmissionPolicy.of(originalPolicy.policy().definition().toBuilder()
                                .setLimits(limits.toBuilder().setMaxFragments(limits.getMaxFragments() - 1)).build(), () -> {});
                        replacement.set(new DocumentSchemaPolicies(updater.tx).activate(updated, originalPolicy.revision(), () -> {}));
                    } else updater.tx.inTransaction(em -> {
                            int changed = em.createNativeQuery("UPDATE documents SET security=CAST(:security AS jsonb) WHERE node_id=:id")
                                    .setParameter("security", json).setParameter("id", node).executeUpdate();
                            require(changed == 1, "ACL writer selects one document");
                        });
                }));
                awaitBlocked(observer, publisher, updater.pid, writer);
                require(!publication.isDone() && success(observer, key) == null, "publication remains uncommitted while authority writer waits");
                gate.commit();
                gate.awaitCommitted();
                writer.get(10, TimeUnit.SECONDS);
                var committed = success(observer, key);
                require(committed != null, "publication committed before authority administration completed");
                gate.release();
                if (removeRead || credential) {
                    var expected = credential ? RepositoryException.Code.UNAUTHENTICATED : RepositoryException.Code.NOT_FOUND;
                    try { publication.get(10, TimeUnit.SECONDS); throw new AssertionError("Revoked authority received committed publication"); }
                    catch (ExecutionException failure) { denied(failure.getCause(), expected); }
                    try { repository.publishDocument(caller, request, RepositoryReadControl.NONE); throw new AssertionError("Revoked authority replayed receipt"); }
                    catch (RepositoryException failure) { denied(failure, expected); }
                } else {
                    var result = publication.get(10, TimeUnit.SECONDS);
                    DocumentPublicationResponseValidator.requireValid(command, caller.principalName(), result);
                    require(result.hasCommitted() && result.getCommitted().equals(committed), "authorized receipt delivery survives administrative update");
                    require(repository.publishDocument(caller, request, RepositoryReadControl.NONE).equals(result),
                            "authorized replay preserves the original committed result");
                }
                require(success(observer, key).equals(committed), "authority change cannot replace committed result");
                for (String table : java.util.List.of("repository_operation_success", "document_revision_commits", "document_assessment_owners")) {
                    long count = observer.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table
                                    + " WHERE account_id=:a AND principal=:p AND operation_id=:o")
                            .setParameter("a", key.account()).setParameter("p", key.principal()).setParameter("o", key.operationId())
                            .getSingleResult()).longValue());
                    require(count == 1, "one publication record in " + table);
                }
            } finally { gate.release(); }
        } finally {
            if (replacement.get() != null) policies.activate(originalPolicy.policy(), replacement.get().revision(), () -> {});
            if (!credential && !policy) observer.inTransaction(em -> {
                require(em.createNativeQuery("UPDATE documents SET security=CAST(:security AS jsonb) WHERE node_id=:id")
                        .setParameter("security", saved).setParameter("id", node).executeUpdate() == 1, "restore original ACL");
            });
        }
    }

    private static void awaitBlocked(Tx tx, int publisher, AtomicInteger writer, Future<?> pending) throws Exception {
        long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < until) {
            if (pending.isDone()) { pending.get(); throw new AssertionError("authority writer did not wait for publication"); }
            if (writer.get() != 0 && tx.readOnly(em -> (Boolean) em.createNativeQuery(
                    "SELECT :publisher=ANY(pg_blocking_pids(:writer))")
                    .setParameter("publisher", publisher).setParameter("writer", writer.get()).getSingleResult())) return;
            Thread.sleep(10);
        }
        throw new AssertionError("authority writer did not block on exact publisher PID");
    }

    private static DocumentPublicationResult success(Tx tx, RepositoryOperationLedger.Key key) throws Exception {
        var rows = tx.readOnly(em -> em.createNativeQuery("SELECT result_bytes FROM repository_operation_success"
                        + " WHERE account_id=:a AND principal=:p AND operation_id=:o", byte[].class)
                .setParameter("a", key.account()).setParameter("p", key.principal()).setParameter("o", key.operationId()).getResultList());
        return rows.isEmpty() ? null : DocumentPublicationResult.parseFrom((byte[]) rows.getFirst());
    }
    private static void denied(Throwable failure, RepositoryException.Code expected) {
        require(failure instanceof RepositoryException denied && denied.code() == expected,
                "current authority guards committed receipt: " + failure);
    }
    private static void require(boolean condition, String message) { if (!condition) throw new AssertionError(message); }
}
