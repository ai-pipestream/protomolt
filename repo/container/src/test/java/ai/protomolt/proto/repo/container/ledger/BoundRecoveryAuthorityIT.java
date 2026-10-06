package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import ai.protomolt.proto.repo.v1.PublicationUpload;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentPublicationRegistrationInspectionIT.*;
import static org.assertj.core.api.Assertions.*;

/** Real PostgreSQL authority changes; synthetic payloads do not establish provider execution. */
@Testcontainers
class BoundRecoveryAuthorityIT {
    @Container static final PostgreSQLContainer POSTGRES=new PostgreSQLContainer("postgres:18-alpine");
    private static final SqlTimeouts TIMEOUTS=new SqlTimeouts(Duration.ofSeconds(1),Duration.ofSeconds(5));

    @ParameterizedTest
    @CsvSource({"key,false", "key,true", "grant,false", "grant,true", "rotation,false", "rotation,true"})
    void authorityChangeStopsBoundRecovery(String change, boolean pending) throws Exception {
        try (var c=context(POSTGRES)) {
            var input=input(c);
            var original=input.command().intent().getMembers(0);
            var member=original.toBuilder().clearSources().clearParts()
                    .setDestination(original.getDestination().toBuilder().setIfAbsent(true)
                            .setAddress(original.getDestination().getAddress().toBuilder().setDocId("bound-authority")));
            for (var part:original.getPartsList()) member.addParts(part.toBuilder().clearReuse().setUpload(
                    PublicationUpload.newBuilder().setSizeBytes(1).setSha256("a".repeat(64)).setContentType("application/protobuf")));
            var command=new DocumentPublicationCommand(input.command().intent().toBuilder().setMembers(0,member).build());
            var record=new DocumentPublicationPreparationRecord(input.key(),command,
                    DocumentPublicationSeeds.mint(input.key(),command),input.placements(),LEASE,0);
            var binding=new RepositoryCredentialBinding("bound-retry-test",UUID.randomUUID(),1);
            var caller=new RepositoryCaller("principal",false,Set.of("account"),Set.of(),Optional.of(binding));
            var credentials=new RepositoryCredentialAuthorities(c.tx());
            credentials.register(CALLER,binding,caller.principalName());
            var grants=new RepositoryCreationGrants(c.tx(),new DriveLedger(c.tx()));
            grants.install(CALLER,RepositoryCreationGrants.prepare(caller,command,input.placements(),
                    (System.currentTimeMillis()+300_000)*1000));
            var source=RepositoryRecoveryAttemptsIT.source(c,record);
            var cancelled=new AtomicBoolean(); var reservationReply=new AtomicBoolean();
            var datasource=DocumentJdbcFaults.afterCommit(c.pool(),() -> {
                if ((!reservationReply.get() && count(c,"repository_successor_executions")==1)
                        || (reservationReply.get() && count(c,"repository_coordinator_expirations")==2)) cancelled.set(true);
            });
            var control=new RepositoryReadControl() {
                public boolean isCancelled() { return cancelled.get(); }
                public long remainingNanos() { return Long.MAX_VALUE; }
            };
            var budget=new PayloadBudget(128_000_000); var lease=Duration.ofSeconds(3);
            try (var emf=jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",Map.of(
                    "hibernate.connection.datasource",datasource,"hibernate.hbm2ddl.auto","validate"));
                 var resources=DocumentJournaledSessionsIT.resources(new Tx(emf).withTimeouts(TIMEOUTS),1,1_000_000,
                         lease,new PayloadBudget(128_000_000));
                 var attempts=new RepositoryRecoveryAttempts(new Tx(emf),budget,resources.sessions(),lease,TIMEOUTS,1)) {
                try (var attempt=attempts.begin(caller,command,source.observation())) {
                    var originalProposal=attempt.proposal();
                    attempt.advance(CALLER,caller,MODES,NONE); attempt.advance(CALLER,caller,MODES,NONE);
                    assertThatThrownBy(() -> attempt.advance(CALLER,caller,MODES,control))
                            .isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.CANCELLED));
                    // Wait for natural lease expiry without modifying durable timestamps.
                    RepositoryRecoveryAttemptsIT.expire(c,command);
                    if (pending) {
                        reservationReply.set(true); cancelled.set(false);
                        assertThatThrownBy(() -> attempt.reconcileExpiredBound(CALLER,caller,MODES,control))
                                .isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.CANCELLED));
                        assertThat(count(c,"repository_coordinator_expirations")).isEqualTo(2);
                    }
                    switch (change) {
                        case "key" -> credentials.revoke(CALLER,binding,caller.principalName());
                        case "rotation" -> credentials.rotate(CALLER,binding,caller.principalName());
                        case "grant" -> grants.revoke(CALLER,input.key());
                        default -> throw new IllegalArgumentException(change);
                    }
                    var before=identity(c,command); long held=budget.reservedBytes();
                    assertThatThrownBy(() -> attempt.reconcileExpiredBound(CALLER,caller,MODES,NONE))
                            .isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code()).isEqualTo(
                                    change.equals("grant") ? RepositoryException.Code.NOT_FOUND : RepositoryException.Code.UNAUTHENTICATED));
                    assertThat(identity(c,command)).containsExactly(before);
                    assertThat(attempt.proposal()).isSameAs(originalProposal);
                    assertThat(budget.reservedBytes()).isEqualTo(held).isPositive();
                    assertThat(count(c,"repository_coordinator_expirations")).isEqualTo(pending ? 2 : 1);
                    assertThat(count(c,"repository_successor_executions")).isEqualTo(1);
                    assertThat(count(c,"repository_coordinator_supersessions")).isZero();
                }
                var beforeClose=identity(c,command);
                attempts.close();
                assertThat(attempts.detachClosed(Duration.ofSeconds(5),ignored -> CALLER,NONE)).isTrue();
                assertThat(budget.reservedBytes()).isZero();
                assertThat(identity(c,command)).containsExactly(beforeClose);
                assertThat(resources.sessions().retainedSessions()).isEqualTo(1);
                assertThat(attempts.drain()).isEqualTo(new RepositoryRecoveryAttempts.Drain(0,0));
            }
        }
    }

    private static Object[] identity(Context c, DocumentPublicationCommand command) {
        return c.tx().readOnly(em -> (Object[])em.createNativeQuery("""
                SELECT c.claim_epoch,c.claim_token,c.lease_until,o.owner_generation,o.owner_token,o.lease_until
                FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                WHERE c.operation_id=:id
                """).setParameter("id",command.operationId()).getSingleResult());
    }
}
