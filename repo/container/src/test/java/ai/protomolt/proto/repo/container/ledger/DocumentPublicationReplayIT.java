package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.v1.*;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static org.assertj.core.api.Assertions.*;

import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;

/** Real SQL replay and current-policy checks using synthetic publication evidence. */
@Testcontainers
class DocumentPublicationReplayIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    @Test void replayDistinguishesUnobservedPendingAndCommittedWithoutSideEffects() {
        try (var c=context()) {
            var prepared=prepare(c,2);
            var replay=new DocumentPublicationReplay(c.tx());
            var caller=new RepositoryCaller("principal",true);
            assertThat(replay.observe(new RepositoryCaller("other",true),prepared.command()).state())
                    .isEqualTo(DocumentPublicationReplay.State.NOT_OBSERVED);
            assertThat(replay.observe(caller,prepared.command()).state()).isEqualTo(DocumentPublicationReplay.State.PENDING);
            var result=publish(c,prepared,Fault.NONE,em -> {});
            for (int i=0;i<2;i++) {
                var observed=replay.observe(caller,prepared.command());
                assertThat(observed.state()).isEqualTo(DocumentPublicationReplay.State.COMMITTED);
                assertThat(observed.result()).contains(result);
            }
            assertThat(count(c,"repository_operation_success")).isEqualTo(1);
            assertThat(count(c,"document_events_outbox")).isEqualTo(2);
            assertThat(count(c,"document_revision_publications")).isEqualTo(4);
            var different=new DocumentPublicationCommand(prepared.command().intent().toBuilder()
                    .setMembers(0,prepared.command().intent().getMembers(0).toBuilder().setMemberId("different")).build());
            assertThatThrownBy(() -> replay.observe(caller,different))
                    .isInstanceOf(RepositoryOperationLedger.CommandConflictException.class);
        }
    }

    @Test void replayRequiresCurrentReadAccessToEveryDestination() {
        try (var c=context()) {
            var prepared=prepare(c,2);
            var result=publish(c,prepared,Fault.NONE,em -> {});
            var replay=new DocumentPublicationReplay(c.tx());
            var caller=new RepositoryCaller("principal",false,Set.of("account"),Set.of());
            assertUnavailable(() -> replay.observe(caller,prepared.command()));
            c.tx().inTransaction(em -> { em.createNativeQuery("""
                    UPDATE documents SET security=CAST(:policy AS jsonb)
                    """).setParameter("policy","{\"permissions\":[{\"identityType\":\"public\",\"identity\":\"public\",\"access\":\"ACCESS_READ\"}]}")
                    .executeUpdate(); });
            assertThat(replay.observe(caller,prepared.command()).result()).contains(result);
            assertUnavailable(() -> replay.observe(new RepositoryCaller("principal",false,Set.of("other"),Set.of()),prepared.command()));
            c.tx().inTransaction(em -> { em.createNativeQuery("UPDATE documents SET security=CAST(:policy AS jsonb) WHERE node_id=:node")
                    .setParameter("policy","{\"permissions\":[{\"identityType\":\"public\",\"identity\":\"public\",\"access\":\"ACCESS_DENY\"}]}")
                    .setParameter("node",prepared.sources().get(1).row().nodeId).executeUpdate(); });
            assertUnavailable(() -> replay.observe(caller,prepared.command()));
        }
    }

    @Test void replayRejectsInvalidStoredResults() {
        for (var fault:List.of(Fault.WRONG_RESULT_REVISION,Fault.INVALID_RESULT_WIRE)) try (var c=context()) {
            var prepared=prepare(c,1);
            publish(c,prepared,fault,em -> {});
            assertThatThrownBy(() -> new DocumentPublicationReplay(c.tx())
                    .observe(new RepositoryCaller("principal",true),prepared.command()))
                    .isInstanceOfSatisfying(RepositoryException.class,
                            failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.DATA_LOSS));
            assertThat(count(c,"repository_operation_success")).isEqualTo(1);
        }
    }

    @Test void replayWaitsForPublicationAndSeesItsCommittedResult() throws Exception {
        try (var c=context(); var executor=java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var prepared=prepare(c,1);
            var ready=new java.util.concurrent.CountDownLatch(1);
            var release=new java.util.concurrent.CompletableFuture<Void>();
            var pid=new java.util.concurrent.atomic.AtomicInteger();
            var publication=executor.submit(() -> publish(c,prepared,Fault.NONE,em -> {
                pid.set(((Number)em.createNativeQuery("SELECT pg_backend_pid()").getSingleResult()).intValue());
                ready.countDown();
                release.join();
            }));
            try {
                assertThat(ready.await(5,java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                var observation=executor.submit(() -> new DocumentPublicationReplay(c.tx())
                        .observe(new RepositoryCaller("principal",true),prepared.command()));
                awaitBlocked(c,pid.get());
                release.complete(null);
                assertThat(observation.get(5,java.util.concurrent.TimeUnit.SECONDS).result())
                        .contains(publication.get(5,java.util.concurrent.TimeUnit.SECONDS));
                assertThat(count(c,"document_events_outbox")).isEqualTo(1);
            } finally { release.complete(null); }
        }
    }

    @Test void replayDoesNotExposeUnavailableTargetsEvenToProcessAuthority() {
        for (String statement:List.of("UPDATE documents SET status='PENDING_PURGE'","DELETE FROM documents")) try (var c=context()) {
            var prepared=prepare(c,1);
            publish(c,prepared,Fault.NONE,em -> {});
            c.tx().inTransaction(em -> { em.createNativeQuery(statement).executeUpdate(); });
            assertUnavailable(() -> new DocumentPublicationReplay(c.tx()).observe(new RepositoryCaller("principal",true),prepared.command()));
        }
    }

    @Test void replaySurvivesOwnerExpiryWithoutRenewingLease() {
        try (var c=context()) {
            var prepared=prepare(c,1,false,Duration.ofSeconds(1));
            var result=publish(c,prepared,Fault.NONE,em -> {});
            c.tx().readOnly(em -> em.createNativeQuery("""
                    SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM lease_until-clock_timestamp()))+0.02)
                    FROM repository_operation_owners
                    """).getSingleResult());
            assertThat(new DocumentPublicationReplay(c.tx()).observe(new RepositoryCaller("principal",true),prepared.command()).result())
                    .contains(result);
            Object live=c.tx().readOnly(em -> em.createNativeQuery("SELECT lease_until>clock_timestamp() FROM repository_operation_owners").getSingleResult());
            assertThat(live).isEqualTo(false);
        }
    }

    @Test void replayKeepsOriginalResultAfterAnotherRevision() {
        try (var c=context()) {
            var first=prepare(c,1);
            var original=publish(c,first,Fault.NONE,em -> {});
            var current=new DocumentLedger(c.tx()).findByNodeId(first.sources().getFirst().row().nodeId).orElseThrow();
            var priorMember=first.command().intent().getMembers(0);
            var condition=priorMember.getDestination().toBuilder().setExpectedMutationRevision(current.mutationRevision).build();
            var member=priorMember.toBuilder().setDestination(condition).clearParts();
            for (var part:priorMember.getPartsList()) member.addParts(part.toBuilder().setReuse(part.getReuse().toBuilder().setSource(condition)));
            var command=new DocumentPublicationCommand(first.command().intent().toBuilder()
                    .setOperationId(UUID.randomUUID().toString()).setMembers(0,member).build());
            var owner=new RepositoryOperationLedger(c.tx()).admit(new RepositoryOperationLedger.Key("account","principal",command.operationId()),
                    command,UUID.randomUUID(),Duration.ofMinutes(5)).owner().orElseThrow();
            var drives=new DriveLedger(c.tx());
            var drive=drives.findById(UUID.fromString(member.getDriveId())).orElseThrow();
            var placement=DocumentUploadPlan.Placement.sample(drive,"native-test",new ManagedBackendLedger(c.tx()).find("native-test").orElseThrow());
            new DocumentOperationUploadAdmission(c.tx(),drives).admit(new RepositoryCaller("principal",true),owner,
                    DocumentOperationUploadAdmission.prepare(command,Map.of(drive.driveId,placement),Map.of(),Duration.ofMinutes(5)));
            var old=first.sources().getFirst();
            var source=new ManagedDocumentFixture(current,UUID.fromString(original.getMembers(0).getRevisionId()),old.slots(),old.identities());
            var newer=publish(c,new Prepared(command,owner,List.of(source),Map.of()),Fault.NONE,em -> {});
            assertThat(newer.getMembers(0).getRevisionId()).isNotEqualTo(original.getMembers(0).getRevisionId());
            assertThat(new DocumentPublicationReplay(c.tx()).observe(new RepositoryCaller("principal",true),first.command()).result())
                    .contains(original);
            assertThat(count(c,"document_events_outbox")).isEqualTo(2);
        }
    }

    @Test void replaySeesPolicyRevocationCommittedWhileItWaits() throws Exception {
        try (var c=context()) {
            var prepared=prepare(c,1);
            publish(c,prepared,Fault.NONE,em -> {});
            c.tx().inTransaction(em -> { em.createNativeQuery("UPDATE documents SET security=CAST(:policy AS jsonb)")
                    .setParameter("policy","{\"permissions\":[{\"identityType\":\"public\",\"identity\":\"public\",\"access\":\"ACCESS_READ\"}]}")
                    .executeUpdate(); });
            try (var connection=c.pool().getConnection(); var executor=java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
                connection.setAutoCommit(false);
                int pid;
                try (var statement=connection.createStatement()) {
                    var rs=statement.executeQuery("SELECT pg_backend_pid()"); rs.next(); pid=rs.getInt(1);
                    statement.executeUpdate("UPDATE documents SET security='{}'::jsonb");
                }
                var waiting=executor.submit(() -> new DocumentPublicationReplay(c.tx())
                        .observe(new RepositoryCaller("principal",false,Set.of("account"),Set.of()),prepared.command()));
                try {
                    awaitBlocked(c,pid);
                    connection.commit();
                    assertThatThrownBy(() -> waiting.get(5,java.util.concurrent.TimeUnit.SECONDS))
                            .hasCauseInstanceOf(RepositoryException.class)
                            .satisfies(failure -> assertThat(((RepositoryException)failure.getCause()).code())
                                    .isEqualTo(RepositoryException.Code.NOT_FOUND));
                } finally { connection.rollback(); }
            }
        }
    }

    private static void assertUnavailable(org.assertj.core.api.ThrowableAssert.ThrowingCallable call) {
        assertThatThrownBy(call).isInstanceOfSatisfying(RepositoryException.class,
                failure -> assertThat(failure.code()).isEqualTo(RepositoryException.Code.NOT_FOUND));
    }

    private static void awaitBlocked(Context c,int pid) throws InterruptedException {
        boolean blocked=false;
        long end=System.nanoTime()+Duration.ofSeconds(5).toNanos();
        while (!blocked && System.nanoTime()<end) {
            blocked=c.tx().readOnly(em -> (Boolean)em.createNativeQuery(
                    "SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE :pid=ANY(pg_blocking_pids(pid)))")
                    .setParameter("pid",pid).getSingleResult());
            if (!blocked) Thread.sleep(10);
        }
        assertThat(blocked).as("replay waits on the held PostgreSQL transaction").isTrue();
    }

    private static Context context() { return DocumentNativePublicationFixture.context(POSTGRES); }
}
