package ai.protomolt.proto.repo.container.ledger;

import java.nio.file.Path;

/** Separate production-JAR host for owner recovery cases, with its own test database. */
public final class HistoricalOwnerReconciliationHost {
    public static void main(String[] args) throws Exception {
        try {
            Class.forName("org.junit.jupiter.api.Test");
            throw new AssertionError("Ambient test framework leaked into reconciliation host");
        } catch (ClassNotFoundException expected) { /* Production classpath only. */ }
        var observation = DocumentAssessmentRuntimeObserver.observe(Path.of(args[0]), () -> {});
        try (var provider = new AssessmentProviderProbe();
             var database = new LedgerDatabase(new LedgerConfig(System.getenv("PROTOMOLT_TEST_JDBC"),
                     System.getenv("PROTOMOLT_TEST_USER"), System.getenv("PROTOMOLT_TEST_PASSWORD")))) {
            var tx = new Tx(database.entityManagerFactory());
            var source = AssessmentMixedReuseProbe.publishSource(tx, provider, "reconciliation", true);
            new DocumentSchemaPolicies(tx).activate(AssessmentCreationProbe.initialPolicy(), 0, () -> {});
            NativeSchemaRevisionProbe.run(tx, provider, source, database.dataSource(), true);
            observation.identity(() -> {});
        }
        System.out.println("HISTORICAL_RECONCILIATION_HOST_OK");
    }
}
