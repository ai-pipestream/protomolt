package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.service.RepoServices;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.*;
import io.grpc.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/** Cold recovery can read source generation A but must not replace the retained upload generation B. */
final class ManagedHistoricalUnavailableUploadProbe {
    static final String GENERATION = "assessment-unmounted-upload";
    static boolean enabled() { return "true".equals(System.getenv("PROTOMOLT_TEST_COLD_MISSING_UPLOAD")); }

    static void run(Tx tx, AssessmentProviderProbe provider, RepositoryCaller caller, DocumentPublicationCommand command,
            PublishDocumentRequest request, RepoServices host, DocumentPublicationServiceGrpc.DocumentPublicationServiceBlockingStub stub,
            AtomicInteger resolutions) {
        var key = new RepositoryOperationLedger.Key(command.intent().getAccountId(), caller.principalName(), command.operationId());
        assertPlacements(tx, key, command, provider);
        var selector = command.intent().getMembers(0).getPartsList().stream().filter(DocumentPublicationPart::hasHistoricalReuse)
                .findFirst().orElseThrow().getHistoricalReuse();
        require(selector.getObject().getBackendGeneration().equals("assessment-s3"), "source and unavailable upload generations differ");
        var before = source(host, caller, selector);
        var versions = provider.versions();
        require(!versions.isEmpty(), "real versioned source objects exist before recovery");
        Runnable library = () -> {
            try { host.publicationRepository().publishDocument(caller, request, RepositoryReadControl.NONE);
                throw new AssertionError("Cold recovery substituted a new upload backend");
            } catch (RepositoryException failure) {
                require(failure.code() == RepositoryException.Code.FAILED_PRECONDITION
                        && failure.getMessage().equals("Original document backend is unavailable"), "retained upload has explicit library refusal");
            }
        };
        Runnable rpc = () -> {
            try { stub.publishDocument(request); throw new AssertionError("Cold RPC substituted a new upload backend"); }
            catch (StatusRuntimeException failure) {
                require(failure.getStatus().getCode() == Status.Code.FAILED_PRECONDITION
                        && "Original document backend is unavailable".equals(failure.getStatus().getDescription()), "retained upload has explicit RPC refusal");
            }
        };
        boolean remoteFirst = "initial".equals(System.getenv("PROTOMOLT_TEST_COLD_PHASE"));
        (remoteFirst ? rpc : library).run();
        var identity = identity(tx, command.operationId());
        (remoteFirst ? library : rpc).run();
        require(identity(tx, command.operationId()).equals(identity), "exact refused retry preserves recovery identity");
        require(resolutions.get() == 0, "missing upload backend does not resolve a fresh schema");
        require(source(host, caller, selector).equals(before), "mounted source remains readable and unchanged after refusal");
        require(provider.versions().equals(versions), "refused recovery adds no provider version or delete marker");
        assertPlacements(tx, key, command, provider);
        for (String table : List.of("document_assessment_owners", "document_revision_commits", "repository_operation_success"))
            require(count(tx, table, command.operationId()) == 0, "refusal creates no assessment or terminal publication");
        require(count(tx, "repository_historical_activations", command.operationId()) == 1,
                "refusal is after actual successor activation, not an earlier source-routing failure");
        require(count(tx, "repository_publication_assessment_starts", command.operationId()) == 2,
                "original and recovered attempts each have one START");
    }
    private static Document source(RepoServices host, RepositoryCaller caller, PublicationHistoricalReuse selector) {
        try (var read = host.historicalRepository().readValidated(caller, selector.getSource(), UUID.fromString(selector.getRevisionId()), RepositoryReadControl.NONE)) {
            read.authorizeDelivery(RepositoryReadControl.NONE);
            return read.document();
        }
    }
    private static void assertPlacements(Tx tx, RepositoryOperationLedger.Key key, DocumentPublicationCommand command, AssessmentProviderProbe provider) {
        var rows = tx.readOnly(em -> em.createNativeQuery("""
                SELECT preparation_bytes,preparation_sha256,owner_nonce,command_sha256,predecessor_generation
                FROM repository_publication_preparations WHERE operation_id=:id ORDER BY predecessor_generation
                """).setParameter("id", command.operationId()).setMaxResults(4).getResultList());
        require(!rows.isEmpty() && rows.size() < 4, "bounded immutable preparation lineage");
        for (var value : rows) {
            var row = (Object[]) value;
            var record = DocumentPublicationPreparationJournal.decode(row, ((byte[]) row[0]).length, key, command.sha256(), ((Number) row[4]).longValue());
            require(!record.placements().isEmpty() && record.placements().values().stream().allMatch(
                    placement -> placement.generation().equals(GENERATION) && placement.profile().equals(provider.profile())),
                    "every retained preparation keeps original upload generation and physical profile");
        }
    }
    private static List<String> identity(Tx tx, UUID operation) {
        var rows = tx.readOnly(em -> em.createNativeQuery("""
                SELECT c.claim_epoch::text || ':' || c.claim_token::text || ':' || o.owner_generation::text || ':' || o.owner_token::text
                FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                WHERE c.operation_id=:id
                """, String.class).setParameter("id", operation).getResultList());
        require(rows.size() == 1, "one exact recovery claim and owner");
        return List.copyOf(rows);
    }
    private static long count(Tx tx, String table, UUID operation) {
        return tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table + " WHERE operation_id=:id")
                .setParameter("id", operation).getSingleResult()).longValue());
    }
    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
