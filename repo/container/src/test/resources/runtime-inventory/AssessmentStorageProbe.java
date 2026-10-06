package ai.protomolt.proto.repo.container.ledger;

import java.nio.file.Path;

/** No test framework, fake runtime observation, or provider success stubs. */
public final class AssessmentStorageProbe {
    public static void main(String[] args) throws Exception {
        try {
            Class.forName("org.junit.jupiter.api.Test");
            throw new AssertionError("Ambient test framework leaked into production host");
        } catch (ClassNotFoundException expected) { /* Deliberately absent. */ }
        var observation = DocumentAssessmentRuntimeObserver.observe(Path.of(args[0]), () -> {});
        ai.protomolt.proto.repo.service.ManagedJournaledDrainProbe.run(Path.of(args[0]));
        try (var provider = new AssessmentProviderProbe();
                var database = new LedgerDatabase(new LedgerConfig(System.getenv("PROTOMOLT_TEST_JDBC"),
                System.getenv("PROTOMOLT_TEST_USER"), System.getenv("PROTOMOLT_TEST_PASSWORD")))) {
            var tx = new Tx(database.entityManagerFactory());
            tx.inTransaction(em -> {
                em.createNativeQuery("SELECT require_repository_read_committed()").getSingleResult();
                Object[] tables = (Object[]) em.createNativeQuery("""
                        SELECT to_regclass('document_assessment_owners') IS NOT NULL,
                            to_regclass('document_assessment_artifacts') IS NOT NULL,
                            to_regclass('document_assessment_roots') IS NOT NULL
                        """).getSingleResult();
                for (Object present : tables) if (!Boolean.TRUE.equals(present)) throw new AssertionError("Assessment migration missing");
                observation.identity(() -> {});
            });
            // Exercise genuine typed/opaque assessment, real validation and scoped
            // evidence while the production Hibernate/JDBC host is initialized.
            ObservedAssessmentProbe.run(observation);
            AssessmentCreationProbe.run(tx, observation, database.dataSource(), provider);
            observation.identity(() -> {});
        }
        System.out.println("OBSERVED_SQL_HOST_OK");
    }
}
