package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.v1.DocumentPublicationIntent;
import ai.protomolt.proto.repo.v1.DocumentPublicationMember;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/** Required replay qualification in a separate production-JAR process and database. */
public final class AssessmentOperationReplayHost {
    public static void main(String[] args) throws Exception {
        try {
            Class.forName("org.junit.jupiter.api.Test");
            throw new AssertionError("Ambient test framework leaked into replay host");
        } catch (ClassNotFoundException expected) { /* Production classpath only. */ }
        var observation = DocumentAssessmentRuntimeObserver.observe(Path.of(args[0]), () -> {});
        try (var provider = new AssessmentProviderProbe();
             var database = new LedgerDatabase(new LedgerConfig(System.getenv("PROTOMOLT_TEST_JDBC"),
                     System.getenv("PROTOMOLT_TEST_USER"), System.getenv("PROTOMOLT_TEST_PASSWORD")))) {
            var tx = new Tx(database.entityManagerFactory());
            var members = new ArrayList<DocumentPublicationMember>();
            for (String id : List.of("a", "b")) {
                var member = ObservedAssessmentProbe.member(id).member();
                members.add(member.toBuilder().setDestination(member.getDestination().toBuilder()
                        .setAddress(member.getDestination().getAddress().toBuilder().setGraphId("rejection-authorization"))).build());
            }
            var targets = AssessmentRestartProbe.seedDestinations(tx, members);
            var policy = new DocumentSchemaPolicies(tx).activate(AssessmentCreationProbe.initialPolicy(), 0, () -> {});
            AssessmentOperationReplayProbe.run(tx, provider, policy, observation, database.dataSource(), targets);
            // The fresh-process rejection reader requires actual lease expiry. Do not
            // rely on incidental time spent in the unrelated aggregate host.
            var request = Path.of(System.getenv("PROTOMOLT_TEST_RESTART_REQUEST") + ".rejected");
            if (Files.size(request) > 1_048_576) throw new AssertionError("Unbounded rejected command fixture");
            var command = DocumentPublicationIntent.parseFrom(Files.readAllBytes(request));
            long deadline = System.nanoTime() + Duration.ofSeconds(65).toNanos();
            while (!tx.readOnly(em -> (Boolean) em.createNativeQuery(
                    "SELECT lease_until<=clock_timestamp() FROM repository_operation_owners WHERE operation_id=:op")
                    .setParameter("op", java.util.UUID.fromString(command.getOperationId())).getSingleResult())) {
                if (System.nanoTime() >= deadline) throw new AssertionError("Rejected writer lease did not expire");
                Thread.sleep(50);
            }
            observation.identity(() -> {});
        }
        System.out.println("ASSESSMENT_OPERATION_REPLAY_HOST_OK");
    }
}
