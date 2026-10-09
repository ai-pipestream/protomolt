package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.spi.*;
import com.google.protobuf.ByteString;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;

/** Competing real SQL starts recover one identity but confer only one CREATE permit. */
final class HistoricalConcurrentStartProbe {
    static void run(Tx tx, DocumentPublicationRegistration registration, DocumentHistoricalExecution first,
            RepositoryCaller caller, RepositoryOperationLedger.Owner owner,
            Map<String, DocumentPublicationCandidate.Mode> modes, DocumentPublicationCommand command,
            DocumentSchemaPolicies.Selection policy, Map<Integer, ByteString> fragments,
            DocumentAssessmentRuntimeObserver.Observation observation) throws Exception {
        var authority = HistoricalAssessmentCreationProbe.authority(tx, owner.key());
        try (var second = registration.historicalExecution(caller, owner, modes, RepositoryReadControl.NONE)) {
            DocumentAssessmentStartJournal.Started started;
            try (var threads = Executors.newFixedThreadPool(2)) {
                var ready = new CountDownLatch(2);
                var go = new CountDownLatch(1);
                var left = threads.submit(() -> { ready.countDown(); go.await();
                    return first.start(caller, Duration.ofMinutes(2), RepositoryReadControl.NONE); });
                var right = threads.submit(() -> { ready.countDown(); go.await();
                    return second.start(caller, Duration.ofMinutes(2), RepositoryReadControl.NONE); });
                try {
                    require(ready.await(10, TimeUnit.SECONDS), "both start workers ready");
                } finally { go.countDown(); }
                started = left.get(20, TimeUnit.SECONDS);
                require(right.get(20, TimeUnit.SECONDS).equals(started), "competing starts share exact coordinates");
            }
            int successes = 0, refused = 0;
            for (var execution : List.of(first, second)) {
                require(execution.start(caller, Duration.ofMinutes(2), RepositoryReadControl.NONE).equals(started),
                        "repeat start preserves the shared identity");
                long claimsBefore = claims(tx, command);
                try (var assessment = execution.prepareAssessment(caller, policy, Map.of("a", fragments), Optional.empty(),
                        (member, occurrence) -> { throw new AssertionError("Historical schemas must be retained"); },
                        new DocumentRevisionAssembly.Limits(4_000_000, 32, 64, 10000, 1_000_000), Instant.now(),
                        RepositoryReadControl.NONE)) {
                    try {
                        var result = execution.createAssessment(caller, assessment, Map.of(), observation,
                                new RepositorySchemaArtifacts(tx), started, RepositoryReadControl.NONE);
                        require(result.assessment().equals(started.assessment()) && result.retainUntil().equals(started.retainUntil()),
                                "winner creates the exact shared start");
                        successes++;
                    } catch (RepositoryException failure) {
                        require(failure.code() == RepositoryException.Code.FAILED_PRECONDITION
                                        && failure.getMessage().contains("requires reconciliation"),
                                "loser is refused by CREATE ownership, not a SQL uniqueness error");
                        require(claims(tx, command) == claimsBefore, "loser adds no schema claims");
                        refused++;
                    }
                }
            }
            require(successes == 1 && refused == 1, "exactly one acknowledged INSERT handle may CREATE");
            var discovered = new DocumentAssessmentDiscovery(tx).discover(caller, owner, command, () -> {}).orElseThrow();
            require(discovered.stage().assessment().equals(started.assessment()), "the sole CREATE is discoverable");
            long owners = tx.readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM document_assessment_owners WHERE assessment_id=:id")
                    .setParameter("id", started.assessment()).getSingleResult()).longValue());
            require(owners == 1, "one durable assessment owner");
            long published = tx.readOnly(em -> ((Number) em.createNativeQuery(
                    "SELECT count(*) FROM document_revision_commits WHERE operation_id=:op")
                    .setParameter("op", command.operationId()).getSingleResult()).longValue());
            require(published == 0, "competing starts and CREATE do not publish");
            require(HistoricalAssessmentCreationProbe.authority(tx, owner.key()).equals(authority),
                    "competing starts preserve exact owner and claim leases");
        }
    }
    private static long claims(Tx tx, DocumentPublicationCommand command) {
        return tx.readOnly(em -> ((Number) em.createNativeQuery(
                "SELECT count(*) FROM repository_schema_artifact_claims WHERE operation_id=:op")
                .setParameter("op", command.operationId()).getSingleResult()).longValue());
    }
    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
