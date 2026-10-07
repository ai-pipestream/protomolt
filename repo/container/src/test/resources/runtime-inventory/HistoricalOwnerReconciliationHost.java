package ai.protomolt.proto.repo.container.ledger;

import java.nio.file.Path;

/** Separate production-JAR host for owner recovery cases, with its own test database. */
public final class HistoricalOwnerReconciliationHost {
    public static void main(String[] args) throws Exception {
        try {
            Class.forName("org.junit.jupiter.api.Test");
            throw new AssertionError("Ambient test framework leaked into reconciliation host");
        } catch (ClassNotFoundException expected) { /* Production classpath only. */ }
        var check = args.length == 1 ? HistoricalInstalledOwnerProbe.Check.ORDINARY : switch (args[1]) {
            case "self-supersession" -> HistoricalInstalledOwnerProbe.Check.SELF_SUPERSESSION;
            case "overlap" -> HistoricalInstalledOwnerProbe.Check.OVERLAP;
            case "takeover-first" -> HistoricalInstalledOwnerProbe.Check.TAKEOVER_FIRST;
            case "claim-expires" -> HistoricalInstalledOwnerProbe.Check.CLAIM_EXPIRES;
            case "commit-wins" -> HistoricalInstalledOwnerProbe.Check.COMMIT_WINS;
            case "commit-wins-old-first" -> HistoricalInstalledOwnerProbe.Check.COMMIT_WINS_OLD_FIRST;
            default -> throw new IllegalArgumentException("Unknown historical qualification mode");
        };

        var observation = DocumentAssessmentRuntimeObserver.observe(Path.of(args[0]), () -> {});
        try (var provider = new AssessmentProviderProbe();
             var database = new LedgerDatabase(new LedgerConfig(System.getenv("PROTOMOLT_TEST_JDBC"),
                     System.getenv("PROTOMOLT_TEST_USER"), System.getenv("PROTOMOLT_TEST_PASSWORD")))) {
            var tx = new Tx(database.entityManagerFactory());
            var source = AssessmentMixedReuseProbe.publishSource(tx, provider, "reconciliation", true);
            new DocumentSchemaPolicies(tx).activate(AssessmentCreationProbe.initialPolicy(), 0, () -> {});
            NativeSchemaRevisionProbe.run(tx, provider, source, database.dataSource(), true, check);
            observation.identity(() -> {});
        }
        System.out.println(switch (check) {
            case TAKEOVER_FIRST -> "HISTORICAL_TAKEOVER_FIRST_HOST_OK";
            case CLAIM_EXPIRES -> "HISTORICAL_CLAIM_EXPIRY_HOST_OK";
            case OVERLAP -> "HISTORICAL_GENERATION_OVERLAP_HOST_OK";
            case SELF_SUPERSESSION -> "HISTORICAL_SELF_SUPERSESSION_HOST_OK";
            case COMMIT_WINS, COMMIT_WINS_OLD_FIRST -> "HISTORICAL_PUBLICATION_COMMIT_WINNER_HOST_OK";
            default -> "HISTORICAL_RECONCILIATION_HOST_OK";
        });

    }
}
