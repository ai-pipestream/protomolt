package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.codec.DocumentRevisionAssembly;
import ai.protomolt.proto.repo.engine.DocumentPartReader;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.DocumentPublicationIntent;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Set;
import java.util.UUID;

/** Fresh process receives only the command; no former writer token or schema registry. */
public final class RejectedAssessmentRestartProbe {
    public static void main(String[] args) throws Exception {
        var file = Path.of(args[0]);
        require(Files.size(file) <= 1_048_576, "bounded command fixture");
        var command = new DocumentPublicationCommand(DocumentPublicationIntent.parseFrom(Files.readAllBytes(file)));
        var observation = DocumentAssessmentRuntimeObserver.observe(Path.of(args[1]), () -> {});
        var caller = new RepositoryCaller("principal", false, Set.of("account"), Set.of());
        try (var database = new LedgerDatabase(new LedgerConfig(System.getenv("PROTOMOLT_TEST_JDBC"),
                System.getenv("PROTOMOLT_TEST_USER"), System.getenv("PROTOMOLT_TEST_PASSWORD")));
                var provider = new AssessmentProviderProbe()) {
            var tx = new Tx(database.entityManagerFactory());
            boolean expired = tx.readOnly(em -> (Boolean) em.createNativeQuery(
                    "SELECT lease_until<=clock_timestamp() FROM repository_operation_owners WHERE operation_id=:op")
                    .setParameter("op", command.operationId()).getSingleResult());
            require(expired, "original writer lease expired before rejected-evidence read");
            var receipt = new DocumentPublicationReplay(tx).observe(caller, command).rejection().orElseThrow();
            var reads = new DocumentReadLedger(tx, UUID.randomUUID(), 1);
            var budget = new PayloadBudget(128_000_000); var payload = new PayloadBudget(16_000_000);
            var reader = new DocumentPartReader((generation, profile) -> {
                require(generation.equals("assessment-s3") && profile.equals(provider.profile()), "original provider identity");
                return provider.store();
            }, 2, 16_000_000, payload);
            try (reader) {
                var capture = reads.captureRejectedAssessment(caller, command, budget, RepositoryReadControl.NONE);
                try (capture) {
                    var result = DocumentAssessmentReplay.replay(capture, reader, budget,
                            new DocumentRevisionAssembly.Limits(4_000_000, 32, 64, 10000, 1_000_000), observation, RepositoryReadControl.NONE);
                    require(result.firstFailure().isPresent()
                            && result.assessment().toString().equals(receipt.getAssessment().getAssessmentId())
                            && result.manifestSha256().equals(receipt.getAssessment().getManifestSha256())
                            && result.commandSha256().equals(command.sha256()), "restarted read reproduces exact rejection evidence");
                } finally {
                    reader.close();
                    require(reader.awaitIdle(Duration.ofSeconds(5)), "provider workers drain");
                    require(budget.reservedBytes() == 0 && payload.reservedBytes() == 0, "memory reservations drain");
                    boolean drained = capture.awaitDrained(Duration.ZERO);
                    int released = reads.releaseDrained(1);
                    int outstanding = reads.outstandingReads();
                    System.out.printf("REJECTED_READ_RELEASE_STATE drained=%s released=%d outstanding=%d%n",
                            drained, released, outstanding);
                    require(drained, "rejected SQL session local uses drain");
                    require(released == 1, "exact SQL session release count=" + released);
                    require(outstanding == 0, "outstanding SQL sessions=" + outstanding);
                }
            }
            require(new DocumentPublicationReplay(tx).observe(caller, command).rejection().orElseThrow().equals(receipt),
                    "evidence read leaves receipt unchanged");
        }
        System.out.println("RESTARTED_REJECTION_EVIDENCE_OK");
    }
    private static void require(boolean condition, String description) {
        if (!condition) throw new AssertionError(description);
    }
}
