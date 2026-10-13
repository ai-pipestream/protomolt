package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.asset.bridge.BridgeEngine;
import ai.protomolt.proto.repo.admission.*;
import ai.protomolt.proto.repo.container.ledger.Tx;
import ai.protomolt.proto.repo.schema.registry.RegistrySchemaResolver;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import ai.protomolt.proto.schema.registry.git.GitSchemaRegistryStore;
import io.grpc.*;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Real Redis host: the physical endpoint remains available but its retained generation is not mounted. */
final class ManagedHistoricalUnavailableProbe {
    static void run(RepoServiceConfig sourceConfig, Path bundle, GitSchemaRegistryStore git,
            DocumentSchemaAdmission.Definition definition, Tx tx, RepositoryCaller caller,
            PublishDocumentRequest source) throws Exception {
        var request = source.toBuilder().setIntent(source.getIntent().toBuilder()
                .setOperationId(UUID.randomUUID().toString())).build();
        var operation = UUID.fromString(request.getIntent().getOperationId());
        var policy = sourceConfig.managedStorage();
        var config = sourceConfig.withManagedStorage(new ManagedStoragePolicy("unavailable-test-" + UUID.randomUUID(),
                policy.storageRealm(), true));
        var identity = new ReaderHostOptions(UUID.randomUUID(), "unavailable-historical-host", UUID.randomUUID().toString());
        var resolver = new RegistrySchemaResolver(git, new DocumentSchemaArtifactCache.Limits(8_000_000, 16, 4_000_000), 4, 16);
        var resolutions = new AtomicInteger();
        ManagedSchemaAccess schemas = new ManagedSchemaAccess() {
            public DocumentSchemaAdmission.Resolution open(RepositoryCaller actual, DocumentPublicationMember member, RepositoryReadControl control) {
                return resolver.open(occurrence -> {
                    resolutions.incrementAndGet();
                    return new RegistrySchemaResolver.Selected(definition.metadata(), definition.source());
                }, control::check);
            }
            public void close() { resolver.close(); }
            public boolean awaitIdle(Duration timeout) throws InterruptedException { return resolver.awaitLoads(timeout); }
        };
        ManagedPublicationOptions.OperationAuthority authority = (account, principal, id) -> {
            require(account.equals(request.getIntent().getAccountId()) && principal.equals(caller.principalName())
                    && id.equals(operation), "unavailable host authority stays operation-scoped");
            return caller;
        };
        var options = new ManagedPublicationOptions(bundle, Duration.ofMinutes(5), Duration.ofSeconds(5), authority)
                .withRecovery(authority).withHistoricalPublication(2);
        try {
            try (var host = RepoServices.buildBoundedDocumentsHosted(config, BridgeEngine.standard(), null, schemas,
                    options, new BoundedDocumentOptions(1024 * 1024, 64L * 1024 * 1024), identity)) {
                    Runnable library = () -> {
                        try { host.publicationRepository().publishDocument(caller, request, RepositoryReadControl.NONE);
                            throw new AssertionError("Unavailable historical generation was substituted");
                        } catch (RepositoryException failure) {
                            require(failure.code() == RepositoryException.Code.FAILED_PRECONDITION
                                    && failure.getMessage().equals("Original document backend is unavailable"), "explicit library backend refusal");
                        }
                    };
                    library.run(); library.run();
                    require(resolutions.get() == 0, "unavailable backend performs no schema resolution");
                    for (String table : List.of("document_revision_commits", "document_assessment_owners", "repository_operation_success")) {
                        long count = tx.readOnly(em -> ((Number) em.createNativeQuery(
                                "SELECT count(*) FROM " + table + " WHERE operation_id=:id")
                                .setParameter("id", operation).getSingleResult()).longValue());
                        require(count == 0, "unavailable backend creates no assessment, revision or success receipt");
                    }
            }
            var states = tx.readOnly(em -> em.createNativeQuery(
                    "SELECT state FROM repository_reader_incarnations WHERE host_execution=:id", String.class)
                    .setParameter("id", identity.execution()).getResultList());
            require(states.equals(List.of("QUIESCED")), "unavailable host reader drains after both refusals");
            var hostState = tx.readOnly(em -> em.createNativeQuery(
                    "SELECT state FROM repository_reader_host_executions WHERE execution=:id", String.class)
                    .setParameter("id", identity.execution()).getSingleResult());
            require(hostState.equals("FENCED"), "unavailable host is fenced after reader drain");
        } finally { schemas.close(); require(schemas.awaitIdle(Duration.ofSeconds(5)), "unavailable registry drained"); }
        System.out.println("MANAGED_HISTORICAL_UNAVAILABLE_" + "LIBRARY" + "_OK");
    }
    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
