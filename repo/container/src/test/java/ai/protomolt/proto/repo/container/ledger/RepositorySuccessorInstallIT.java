package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentPublicationRegistrationInspectionIT.*;
import static org.assertj.core.api.Assertions.*;

/** Real SQL registration checks; these grant no storage I/O or restored publication. */
@Testcontainers
class RepositorySuccessorInstallIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");

    static RepositorySuccessorInstall.Plan plan(Context c) {
        return plan(c, LEASE);
    }

    static RepositorySuccessorInstall.Plan plan(Context c, Duration successorLease) {
        var input = input(c); var incarnation = UUID.randomUUID();
        var previous = new DocumentPublicationPreparationRecord(input.key(), input.command(), input.seeds(),
                input.placements(), Duration.ofSeconds(1), 0);
        var budget = new PayloadBudget(64_000_000);
        var claim = new DocumentPublicationPreparationJournal(c.tx(), budget)
                .acquireInitial(CALLER, previous, UUID.randomUUID(), incarnation, NONE);
        new DocumentPublicationModesJournal(c.tx(), budget).bind(CALLER, claim, 0, MODES, NONE);
        new RepositoryOperationLedger(c.tx()).admit(previous.key(), previous.command(), previous.seeds().ownerNonce(), Duration.ofSeconds(1), claim);
        RepositoryCoordinatorDrain.begin(c.tx(), CALLER, claim, incarnation, NONE);
        var identity = new RepositoryCoordinatorDrain.Identity(claim.key(),claim.commandSha256(),1,claim.token(),incarnation);
        RepositoryCoordinatorLocalDrain.record(c.tx(),CALLER,identity,NONE);
        c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(1.1)").getSingleResult());
        var handoff = new RepositoryCoordinatorHandoff.Proposal(identity,UUID.randomUUID(),UUID.randomUUID(),successorLease);
        RepositoryCoordinatorHandoff.reserve(c.tx(),CALLER,handoff,NONE);
        return RepositorySuccessorInstall.prepare(handoff,previous,LEASE,MODES);
    }

    @Test void installsAllThreeRecordsAndExactRetryWithoutOpeningExecution() {
        try(var c=context(POSTGRES)) {
            var plan=plan(c); var budget=new PayloadBudget(64_000_000);
            RepositorySuccessorInstall.install(c.tx(),budget,CALLER,plan,NONE);
            var before=state(c,plan);
            assertThat(((Number)before[0]).longValue()).isEqualTo(2);
            assertThat(before[1]).isEqualTo(plan.next().seeds().ownerNonce());
            RepositorySuccessorInstall.install(c.tx(),budget,CALLER,plan,NONE);
            assertThat(state(c,plan)).containsExactly(before);
            for(String table:List.of("repository_successor_installs","repository_publication_preparations","repository_publication_modes")) {
                long count=c.tx().readOnly(em -> ((Number)em.createNativeQuery("SELECT count(*) FROM "+table+" WHERE predecessor_generation=1").getSingleResult()).longValue());
                assertThat(count).isEqualTo(1);
            }
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                em.createNativeQuery("SELECT require_repository_execution_claim(:a,:p,:o)")
                        .setParameter("a",plan.next().key().account()).setParameter("p",plan.next().key().principal())
                        .setParameter("o",plan.next().key().operationId()).getSingleResult();
            })).hasStackTraceContaining("locally drained");
            var conflict=RepositorySuccessorInstall.prepare(plan.handoff(),plan.previous(),LEASE,MODES);
            assertThatThrownBy(() -> RepositorySuccessorInstall.install(c.tx(),budget,CALLER,conflict,NONE)).hasMessageContaining("differs");
            assertThat(budget.reservedBytes()).isZero();
        }
    }

    @Test void cancellationAfterOwnerChangeRollsBackEveryRecord() {
        try(var c=context(POSTGRES)) {
            var plan=plan(c); var budget=new PayloadBudget(64_000_000);
            var checks=new java.util.concurrent.atomic.AtomicInteger();
            var failure=new RepositoryException(RepositoryException.Code.CANCELLED,"cancel successor install");
            var control=new RepositoryReadControl() {
                public boolean isCancelled(){return false;}
                public long remainingNanos(){return Long.MAX_VALUE;}
                public void check(){if(checks.incrementAndGet()>=9)throw failure;}
            };
            assertThat(catchThrowable(() -> RepositorySuccessorInstall.install(c.tx(),budget,CALLER,plan,control))).isSameAs(failure);
            assertThat(((Number)state(c,plan)[0]).longValue()).isEqualTo(1);
            for(String table:List.of("repository_successor_installs","repository_publication_preparations","repository_publication_modes")) {
                long count=c.tx().readOnly(em -> ((Number)em.createNativeQuery("SELECT count(*) FROM "+table+" WHERE predecessor_generation=1").getSingleResult()).longValue());
                assertThat(count).isZero();
            }
            assertThat(budget.reservedBytes()).isZero();
            RepositorySuccessorInstall.install(c.tx(),budget,CALLER,plan,NONE);
        }
    }

    @Test void lostCommitAcknowledgmentConfirmsCompleteInstall() {
        try(var c=context(POSTGRES)) {
            var plan=plan(c);var budget=new PayloadBudget(64_000_000);var armed=new java.util.concurrent.atomic.AtomicBoolean(true);
            var source=DocumentJdbcFaults.afterCommit(c.pool(),()->{
                long count=c.tx().readOnly(em -> ((Number)em.createNativeQuery("SELECT count(*) FROM repository_successor_installs").getSingleResult()).longValue());
                if(count==1&&armed.compareAndSet(true,false))throw new java.sql.SQLException("install reply lost","08006");
            });
            try(var emf=jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",Map.of(
                    "hibernate.connection.datasource",source,"hibernate.hbm2ddl.auto","validate"))) {
                RepositorySuccessorInstall.install(new Tx(emf),budget,CALLER,plan,NONE);
                assertThat(armed).isFalse();
                assertThat(((Number)state(c,plan)[0]).longValue()).isEqualTo(2);
                assertThat(budget.reservedBytes()).isZero();
            }
        }
    }

    private static Object[] state(Context c,RepositorySuccessorInstall.Plan plan){
        return c.tx().readOnly(em -> (Object[])em.createNativeQuery("SELECT owner_generation,owner_token,lease_until FROM repository_operation_owners WHERE operation_id=:o")
                .setParameter("o",plan.next().key().operationId()).getSingleResult());
    }

    @Test void incompleteManifestCannotCommitAndDoesNotAuthorizeLaterTransactions() {
        try(var c=context(POSTGRES)) {
            var plan=plan(c);
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { manifest(em,plan); }))
                    .hasStackTraceContaining("must atomically complete");
            assertThat(((Number)state(c,plan)[0]).longValue()).isEqualTo(1);
            long count=c.tx().readOnly(em -> ((Number)em.createNativeQuery("SELECT count(*) FROM repository_successor_installs").getSingleResult()).longValue());
            assertThat(count).isZero();
            RepositorySuccessorInstall.install(c.tx(),new PayloadBudget(64_000_000),CALLER,plan,NONE);
            boolean usable=c.tx().inTransaction(em -> {
                return (Boolean)em.createNativeQuery("SELECT repository_successor_registration(:a,:p,:o,1,:n)")
                        .setParameter("a",plan.next().key().account()).setParameter("p",plan.next().key().principal())
                        .setParameter("o",plan.next().key().operationId()).setParameter("n",plan.next().seeds().ownerNonce()).getSingleResult();
            });
            assertThat(usable).isFalse();
        }
    }

    @Test void leaseExpiryInsideTransactionFailsDeferredCheck() {
        try(var c=context(POSTGRES)) {
            var plan=plan(c,Duration.ofSeconds(1));
            assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                manifest(em,plan);
                em.createNativeQuery("SELECT pg_sleep(1.1)").getSingleResult();
            })).hasStackTraceContaining("expired before install commit");
            assertThat(((Number)state(c,plan)[0]).longValue()).isEqualTo(1);
        }
    }

    @Test void ownerLeaseMustRemainLiveUntilCommit() {
        try(var c=context(POSTGRES)) {
            var original=plan(c);
            var plan=RepositorySuccessorInstall.prepare(original.handoff(),original.previous(),Duration.ofSeconds(1),MODES);
            var checks=new java.util.concurrent.atomic.AtomicInteger();
            var control=new RepositoryReadControl() {
                public boolean isCancelled(){return false;}
                public long remainingNanos(){return Long.MAX_VALUE;}
                public void check(){
                    if(checks.incrementAndGet()==9)
                        c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(1.1)").getSingleResult());
                }
            };
            assertThatThrownBy(() -> RepositorySuccessorInstall.install(c.tx(),new PayloadBudget(64_000_000),CALLER,plan,control))
                    .hasStackTraceContaining("must atomically complete");
            assertThat(((Number)state(c,plan)[0]).longValue()).isEqualTo(1);
        }
    }

    @Test void competingInstallsHaveOneCompleteWinner() throws Exception {
        try(var c=context(POSTGRES);var workers=java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var first=plan(c);var second=RepositorySuccessorInstall.prepare(first.handoff(),first.previous(),LEASE,MODES);
            var gate=new java.util.concurrent.CountDownLatch(1);
            var tasks=List.of(first,second).stream().map(plan -> workers.submit(() -> {
                gate.await();
                return catchThrowable(() -> RepositorySuccessorInstall.install(c.tx(),new PayloadBudget(64_000_000),CALLER,plan,NONE));
            })).toList();
            gate.countDown();int winners=0;
            for(var task:tasks){
                var failure=task.get(10,java.util.concurrent.TimeUnit.SECONDS);
                if(failure==null)winners++;
                else assertThat(failure.toString()).satisfiesAnyOf(
                        text -> assertThat(text).contains("Install requires exact expired owner"),
                        text -> assertThat(text).contains("differs from committed proposal"));
            }
            assertThat(winners).isEqualTo(1);
            var owner=state(c,first);
            assertThat(((Number)owner[0]).longValue()).isEqualTo(2);
            var winner=owner[1].equals(first.next().seeds().ownerNonce())?first:second;
            RepositorySuccessorInstall.install(c.tx(),new PayloadBudget(64_000_000),CALLER,winner,NONE);
            long count=c.tx().readOnly(em -> ((Number)em.createNativeQuery("SELECT count(*) FROM repository_successor_installs").getSingleResult()).longValue());
            assertThat(count).isEqualTo(1);
        }
    }

    @Test void transactionProofDoesNotCoverDifferentModesOrOwner() {
        try(var c=context(POSTGRES)) {
            var plan=plan(c);var p=plan.next();
            for(boolean wrongModes:List.of(false,true)) {
                assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                    manifest(em,plan);
                    if(wrongModes) em.createNativeQuery("""
                            INSERT INTO repository_publication_modes(account_id,principal,operation_id,predecessor_generation,owner_nonce,modes)
                            VALUES(:a,:p,:o,1,:n,'{"member-0":"OPAQUE"}'::jsonb)
                            """).setParameter("a",p.key().account()).setParameter("p",p.key().principal())
                            .setParameter("o",p.key().operationId()).setParameter("n",p.seeds().ownerNonce()).executeUpdate();
                    else em.createNativeQuery("""
                            UPDATE repository_operation_owners SET owner_generation=2,owner_token=:n,
                            lease_until=clock_timestamp()+interval '1 minute' WHERE operation_id=:o
                            """).setParameter("n",UUID.randomUUID()).setParameter("o",p.key().operationId()).executeUpdate();
                })).hasStackTraceContaining("locally drained");
            }
            assertThat(((Number)state(c,plan)[0]).longValue()).isEqualTo(1);
        }
    }

    private static void manifest(jakarta.persistence.EntityManager em,RepositorySuccessorInstall.Plan plan) {
        var p=plan.next();var h=plan.handoff();
        var json=new com.google.gson.JsonObject();new TreeMap<>(plan.modes()).forEach((key,mode)->json.addProperty(key,mode.name()));
        em.createNativeQuery("""
                INSERT INTO repository_successor_installs
                (account_id,principal,operation_id,predecessor_epoch,successor_epoch,successor_token,successor_incarnation,
                 predecessor_generation,predecessor_nonce,predecessor_preparation_sha256,owner_nonce,command_sha256,
                 preparation_sha256,modes_sha256,install_xid)
                VALUES(:a,:p,:o,:e,:se,:t,:i,:g,:old,:oldSha,:n,:d,:sha,
                 sha256(convert_to(CAST(:m AS jsonb)::text,'UTF8')),pg_current_xact_id())
                """).setParameter("a",p.key().account()).setParameter("p",p.key().principal()).setParameter("o",p.key().operationId())
                .setParameter("e",h.predecessor().epoch()).setParameter("se",h.predecessor().epoch()+1)
                .setParameter("t",h.successorToken()).setParameter("i",h.successorIncarnation())
                .setParameter("g",p.predecessorGeneration()).setParameter("old",plan.previous().seeds().ownerNonce())
                .setParameter("oldSha",DocumentPublicationPreparationJournal.digest(DocumentPublicationPreparationCodec.encode(plan.previous())))
                .setParameter("n",p.seeds().ownerNonce()).setParameter("d",HexFormat.of().parseHex(p.command().sha256()))
                .setParameter("sha",DocumentPublicationPreparationJournal.digest(DocumentPublicationPreparationCodec.encode(p)))
                .setParameter("m",json.toString()).executeUpdate();
    }
}
