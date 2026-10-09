package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/** Authorization changes commit while a public historical upload reply is held. */
final class HistoricalPublicAuthorizationProbe {
    static void run(Tx tx, HistoricalPublicTakeoverProbe provider, DocumentPublicationRuntime runtime,
            DocumentReadLedger reads, DocumentPublicationRepository repository, RepositoryCaller caller,
            PublishDocumentRequest request, AtomicInteger selections, AtomicInteger resolutions,
            String change, java.util.UUID node) throws Exception {
        var command = new DocumentPublicationCommand(request.getIntent());
        var key = new RepositoryOperationLedger.Key(command.intent().getAccountId(), caller.principalName(), command.operationId());
        var registry = runtime.withHistoricalAttempts(value -> value);
        boolean credential = change.equals("credential");
        boolean policyChange = change.equals("policy");
        if (!credential && !policyChange && !change.equals("read") && !change.equals("write")) throw new IllegalArgumentException(change);
        var catalog = new DocumentSchemaPolicies(tx);
        var oldPolicy = policyChange ? catalog.read(key.account(), () -> {}) : null;
        DocumentSchemaPolicies.Selection replacement = null;
        String saved = credential || policyChange ? null : tx.readOnly(em -> (String) em.createNativeQuery(
                "SELECT security::text FROM documents WHERE node_id=:id").setParameter("id", node).getSingleResult());
        var expected = credential ? RepositoryException.Code.UNAUTHENTICATED
                : policyChange ? RepositoryException.Code.FAILED_PRECONDITION : RepositoryException.Code.NOT_FOUND;
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var pending = executor.submit(() -> repository.publishDocument(caller, request, RepositoryReadControl.NONE));
            try {
                provider.awaitPut();
                require(!pending.isDone() && registry.drain().active() == 1 && reads.outstandingReads() > 0,
                        "accepted call retains its generation and historical captures before revocation");
                if (credential) new RepositoryCredentialAuthorities(tx).revoke(new RepositoryCaller("operator", true),
                        caller.credentialBinding().orElseThrow(), caller.principalName());
                else if (policyChange) {
                    var limits = oldPolicy.policy().definition().getLimits();
                    require(limits.getMaxFragments() > 1, "fixture policy has room to reduce fragment bound");
                    var changed = ai.protomolt.proto.repo.admission.DocumentAdmissionPolicy.of(oldPolicy.policy().definition().toBuilder()
                            .setLimits(limits.toBuilder().setMaxFragments(limits.getMaxFragments() - 1)).build(), () -> {});
                    replacement = catalog.activate(changed, oldPolicy.revision(), () -> {});
                    require(replacement.revision() == oldPolicy.revision() + 1
                                    && !replacement.policy().sha256().equals(oldPolicy.policy().sha256()),
                            "policy replacement changes content and advances its revision");
                }
                else {
                    var retained = change.equals("read") ? Access.ACCESS_WRITE : Access.ACCESS_READ;
                    var removed = change.equals("read") ? Access.ACCESS_READ : Access.ACCESS_WRITE;
                    var security = DocumentSecurity.newBuilder().addPermissions(AccessRule.newBuilder()
                            .setIdentityType("public").setIdentity("public").setAccess(retained)).build();
                    require(DocumentAccessPolicy.allows(caller, key.account(), security, java.util.List.of(), retained)
                                    && !DocumentAccessPolicy.allows(caller, key.account(), security, java.util.List.of(), removed),
                            "fixture removes only the selected permission");
                    security(tx, node, com.google.protobuf.util.JsonFormat.printer().print(security));
                }
                provider.releasePut();
                try {
                    pending.get(15, TimeUnit.SECONDS);
                    throw new AssertionError("Revoked authority completed public historical publication");
                } catch (ExecutionException failure) {
                    require(failure.getCause() instanceof RepositoryException denied
                                    && denied.code() == expected,
                            "public call reports revoked " + change + ": " + failure.getCause());
                    if (policyChange) require(stalePolicy(failure), "first refusal identifies the exact stale policy");
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
                    throw new AssertionError("Revoked authority retried public historical publication");
                } catch (RepositoryException denied) {
                    require(denied.code() == expected, "retry rechecks current " + change + " authority");
                    if (policyChange) require(stalePolicy(denied), "retry retains the original stale policy assessment");
                }
                require(puts == provider.completedPuts() && selected == selections.get() && resolved == resolutions.get(),
                        "revoked retry adds no PUT, host selection or schema resolution");
                runtime.close();
                boolean stopped = false;
                for (int pass = 0; pass < 16 && !stopped; pass++) stopped = runtime.shutdownStep(Duration.ofSeconds(1));
                require(stopped && registry.drain().unresolved() == 0 && reads.outstandingReads() == 0,
                        "explicit process cleanup authority releases revoked request resources");
            } finally { provider.releasePut(); }
        } finally {
            if (replacement != null) catalog.activate(oldPolicy.policy(), replacement.revision(), () -> {});
            else if (!credential && !policyChange) security(tx, node, saved);
        }
    }

    private static boolean stalePolicy(Throwable failure) {
        for (Throwable next = failure; next != null; next = next.getCause()) {
            if (next instanceof DocumentSchemaPolicies.StalePolicy
                    && "Prepared schema policy is no longer active".equals(next.getMessage())) return true;
            if (next instanceof io.grpc.StatusRuntimeException remote
                    && remote.getStatus().getCode() == io.grpc.Status.Code.FAILED_PRECONDITION
                    && "Prepared schema policy is no longer active".equals(remote.getStatus().getDescription())) return true;
        }
        return false;
    }

    private static void security(Tx tx, java.util.UUID node, String value) {
        tx.inTransaction(em -> {
            int changed = em.createNativeQuery("UPDATE documents SET security=CAST(:security AS jsonb) WHERE node_id=:id")
                    .setParameter("security", value).setParameter("id", node).executeUpdate();
            require(changed == 1, "ACL change selects exactly one document");
        });
    }

    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
