package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/** A real credential revocation commits while a public historical upload reply is held. */
final class HistoricalPublicCredentialProbe {
    static void run(Tx tx, HistoricalPublicTakeoverProbe provider, DocumentPublicationRuntime runtime,
            DocumentReadLedger reads, DocumentPublicationRepository repository, RepositoryCaller caller,
            PublishDocumentRequest request, AtomicInteger selections, AtomicInteger resolutions) throws Exception {
        var command = new DocumentPublicationCommand(request.getIntent());
        var key = new RepositoryOperationLedger.Key(command.intent().getAccountId(), caller.principalName(), command.operationId());
        var registry = runtime.withHistoricalAttempts(value -> value);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var pending = executor.submit(() -> repository.publishDocument(caller, request, RepositoryReadControl.NONE));
            try {
                provider.awaitPut();
                require(!pending.isDone() && registry.drain().active() == 1 && reads.outstandingReads() > 0,
                        "accepted call retains its generation and historical captures before revocation");
                new RepositoryCredentialAuthorities(tx).revoke(new RepositoryCaller("operator", true),
                        caller.credentialBinding().orElseThrow(), caller.principalName());
                provider.releasePut();
                try {
                    pending.get(15, TimeUnit.SECONDS);
                    throw new AssertionError("Revoked credential completed public historical publication");
                } catch (ExecutionException failure) {
                    require(failure.getCause() instanceof RepositoryException denied
                                    && denied.code() == RepositoryException.Code.UNAUTHENTICATED,
                            "public call reports revoked credential: " + failure.getCause());
                }
                for (String table : java.util.List.of("document_revision_commits", "document_assessment_owners",
                        "repository_operation_success")) {
                    long count = tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table
                                    + " WHERE account_id=:a AND principal=:p AND operation_id=:o")
                            .setParameter("a", key.account()).setParameter("p", key.principal())
                            .setParameter("o", key.operationId()).getSingleResult()).longValue());
                    require(count == 0, "revocation before upload completion leaves no rows in " + table);
                }
                int puts = provider.completedPuts(), selected = selections.get(), resolved = resolutions.get();
                require(puts == 1, "one real upload precedes revocation");
                try {
                    repository.publishDocument(caller, request, RepositoryReadControl.NONE);
                    throw new AssertionError("Revoked credential retried public historical publication");
                } catch (RepositoryException denied) {
                    require(denied.code() == RepositoryException.Code.UNAUTHENTICATED, "retry rechecks revoked generation");
                }
                require(puts == provider.completedPuts() && selected == selections.get() && resolved == resolutions.get(),
                        "revoked retry adds no PUT, host selection or schema resolution");
                runtime.close();
                boolean stopped = false;
                for (int pass = 0; pass < 16 && !stopped; pass++) stopped = runtime.shutdownStep(Duration.ofSeconds(1));
                require(stopped && registry.drain().unresolved() == 0 && reads.outstandingReads() == 0,
                        "explicit process cleanup authority drains revoked caller resources");
            } finally { provider.releasePut(); }
        }
    }

    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
