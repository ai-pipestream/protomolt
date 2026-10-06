package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentPublicationRegistrationInspectionIT.*;
import static org.assertj.core.api.Assertions.*;

/** Atomic initial admission against PostgreSQL; no provider work occurs during admission. */
@Testcontainers
class DocumentInitialAdmissionIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @ParameterizedTest @ValueSource(strings={"repository_execution_claims","repository_coordinator_bindings",
            "repository_publication_preparations","repository_publication_modes","repository_operations","repository_operation_owners"})
    void insertionFailureRollsBackEntireRegistration(String failedTable) {
        try(var c=context(POSTGRES)) {
            var input=input(c); var budget=new PayloadBudget(64_000_000);
            var session=DocumentPublicationSession.journaled(c.tx(),CALLER,input.command(),input.placements(),LEASE,budget);
            c.tx().inTransaction(em -> {
                em.createNativeQuery("CREATE FUNCTION fail_initial_owner() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'controlled initial owner failure'; END; $$").executeUpdate();
                em.createNativeQuery("CREATE TRIGGER fail_initial_owner AFTER INSERT ON "+failedTable+" FOR EACH ROW EXECUTE FUNCTION fail_initial_owner()").executeUpdate();
                return null;
            });
            try(var execution=session.begin(CALLER,NONE)) {
                execution.bindModes(MODES);
                assertThatThrownBy(() -> session.admit(CALLER,NONE)).hasStackTraceContaining("controlled initial owner failure");
                for(String table:List.of("repository_execution_claims","repository_coordinator_bindings",
                        "repository_publication_preparations","repository_publication_modes","repository_operations","repository_operation_owners")) {
                    assertThat(c.tx().<Number>readOnly(em -> (Number) em.createNativeQuery("SELECT count(*) FROM "+table+" WHERE operation_id=:id")
                            .setParameter("id",input.command().operationId()).getSingleResult()).longValue()).as(table).isZero();
                }
                c.tx().inTransaction(em -> { em.createNativeQuery("DROP TRIGGER fail_initial_owner ON "+failedTable).executeUpdate(); return null; });
                var owner=session.admit(CALLER,NONE).orElseThrow();
                assertThat(owner.token()).isEqualTo(session.seeds().ownerNonce());
                assertThat(session.admit(CALLER,NONE)).contains(owner);
            }
            assertThat(budget.reservedBytes()).isZero();
        }
    }
}
