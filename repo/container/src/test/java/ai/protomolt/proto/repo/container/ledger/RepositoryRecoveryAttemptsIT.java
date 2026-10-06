package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.*;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentPublicationRegistrationInspectionIT.*;
import static org.assertj.core.api.Assertions.*;

@Testcontainers
class RepositoryRecoveryAttemptsIT {
    @Container static final PostgreSQLContainer POSTGRES=new PostgreSQLContainer("postgres:18-alpine");
    private static final SqlTimeouts TIMEOUTS=new SqlTimeouts(Duration.ofSeconds(1),Duration.ofSeconds(5));
    private record Source(DocumentPublicationCommand command, RepositoryCoordinatorRecoveryDiscovery.Observation observation) {}
    private static Source source(Context c) {
        return source(c,input(c));
    }
    private static Source source(Context c, DocumentPublicationPreparationRecord input) {
        var budget=new PayloadBudget(64_000_000);
        var previous=new DocumentPublicationPreparationRecord(input.key(),input.command(),input.seeds(),input.placements(),Duration.ofSeconds(1),0);
        var claim=new DocumentPublicationPreparationJournal(c.tx(),budget).acquireInitial(CALLER,previous,UUID.randomUUID(),UUID.randomUUID(),NONE);
        new DocumentPublicationModesJournal(c.tx(),budget).bind(CALLER,claim,0,MODES,NONE);
        new RepositoryOperationLedger(c.tx()).admit(input.key(),input.command(),input.seeds().ownerNonce(),Duration.ofSeconds(1),claim);
        c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(1.1)").getSingleResult());
        return new Source(input.command(),new RepositoryCoordinatorRecoveryDiscovery(c.tx(),TIMEOUTS).inspect(CALLER,input.key(),input.command().sha256(),NONE));
    }

    @ParameterizedTest @ValueSource(ints={0,1,2})
    void retainedRetryReconcilesOnlyItsExpiredUnactivatedSuccessor(int phases) throws Exception {
        try (var c=context(POSTGRES)) {
            var source=source(c); var budget=new PayloadBudget(128_000_000);
            try (var resources=DocumentJournaledSessionsIT.resources(c.tx(),2,1_000_000,Duration.ofSeconds(2),budget);
                 var attempts=new RepositoryRecoveryAttempts(c.tx(),budget,resources.sessions(),Duration.ofSeconds(2),TIMEOUTS,1)) {
                try (var attempt=attempts.begin(CALLER,source.command(),source.observation())) {
                    if (phases==0) {
                        // Real commit while the local handle still has no acknowledgment.
                        RepositoryCoordinatorExpiration.reserve(c.tx(),CALLER,
                                (RepositoryCoordinatorReservation.ExpiredUnquiesced)attempt.proposal(),NONE);
                    } else for (int i=0;i<phases;i++) attempt.advance(CALLER,CALLER,MODES,NONE);
                    assertThat(attempt.reconcileUnactivated(CALLER,CALLER,MODES,NONE)).isFalse();
                }
                expire(c,source.command());
                try (var retry=attempts.resume(CALLER,source.command()).orElseThrow()) {
                    var previous=retry.proposal();
                    var claimBefore=claimAndOwner(c,source.command());
                    assertThatThrownBy(() -> retry.reconcileUnactivated(CALLER,CALLER,
                            Map.of("member-0",DocumentPublicationCandidate.Mode.OPAQUE),NONE))
                            .isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code())
                                    .isEqualTo(RepositoryException.Code.FAILED_PRECONDITION));
                    assertThat(count(c,"repository_coordinator_supersessions")).isZero();
                    assertThat(claimAndOwner(c,source.command())).containsExactly(claimBefore);
                    assertThat(retry.reconcileUnactivated(CALLER,CALLER,MODES,NONE)).isTrue();
                    assertThat(retry.proposal().predecessor().epoch()).isEqualTo(previous.predecessor().epoch()+1);
                    assertThat(count(c,"repository_coordinator_supersessions")).isEqualTo(1);
                    assertThat(retry.advance(CALLER,CALLER,MODES,NONE)).isEqualTo(RepositoryRecoveryAttempts.Phase.INSTALLED);
                    assertThat(retry.advance(CALLER,CALLER,MODES,NONE)).isEqualTo(RepositoryRecoveryAttempts.Phase.ACTIVATED);
                    assertThat(count(c,"repository_successor_executions")).isEqualTo(1);
                    assertThat(budget.reservedBytes()).isZero();
                }
            }
        }
    }

    @ParameterizedTest @ValueSource(booleans={false,true})
    void freshManagedOwnerRecoversExpiredUnactivatedSuccessor(boolean installed) throws Exception {
        try (var c=context(POSTGRES)) {
            var input=input(c);
            var intent=input.command().intent().toBuilder();
            var bodies=new HashMap<DocumentUploadPayloads.Key,ai.protomolt.proto.repo.codec.PartObject>();
            for (int m=0;m<intent.getMembersCount();m++) {
                var member=intent.getMembersBuilder(m);
                for (int ordinal=0;ordinal<member.getPartsCount();ordinal++) {
                    var part=member.getPartsBuilder(ordinal);
                    if (!part.hasUpload()) continue;
                    byte[] bytes=new byte[Math.toIntExact(part.getUpload().getSizeBytes())];
                    var sha=ai.protomolt.proto.repo.codec.DocumentPartCodec.sha256Hex(bytes);
                    part.getUploadBuilder().setSha256(sha);
                    bodies.put(new DocumentUploadPayloads.Key(member.getMemberId(),ordinal),
                            new ai.protomolt.proto.repo.codec.PartObject(part.getSlot().getPart(),part.getSlot().getSubKey(),bytes,sha));
                }
            }
            var command=new DocumentPublicationCommand(intent.build());
            var source=source(c,new DocumentPublicationPreparationRecord(input.key(),command,
                    DocumentPublicationSeeds.mint(input.key(),command),input.placements(),LEASE,0));
            var firstBudget=new PayloadBudget(128_000_000);
            var secondBudget=new PayloadBudget(128_000_000);
            try (var first=DocumentJournaledSessionsIT.resources(c.tx(),2,1_000_000,Duration.ofSeconds(1),firstBudget);
                 var second=DocumentJournaledSessionsIT.resources(c.tx(),2,1_000_000,LEASE,secondBudget);
                 var attempts=new RepositoryRecoveryAttempts(c.tx(),firstBudget,first.sessions(),Duration.ofSeconds(1),TIMEOUTS,1)) {
                try (var attempt=attempts.begin(CALLER,command,source.observation())) {
                    attempt.advance(CALLER,CALLER,MODES,NONE);
                    if (installed) attempt.advance(CALLER,CALLER,MODES,NONE);
                }
                var recovery=new RepositoryManagedRecovery(c.tx(),secondBudget,second.sessions(),LEASE,TIMEOUTS,2,
                        (account,principal,operation) -> CALLER);
                try (var call=recovery.calls.enter(CALLER,command)) {
                    assertThatThrownBy(() -> recovery.prepare(CALLER,command,bodies,MODES,NONE))
                            .isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code())
                                    .isEqualTo(RepositoryException.Code.FAILED_PRECONDITION));
                    assertThat(count(c,"repository_coordinator_supersessions")).isZero();
                    expire(c,command);
                    var beforeMismatch=claimAndOwner(c,command);
                    assertThatThrownBy(() -> recovery.prepare(CALLER,command,bodies,
                            Map.of("member-0",DocumentPublicationCandidate.Mode.OPAQUE),NONE))
                            .isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code())
                                    .isEqualTo(RepositoryException.Code.FAILED_PRECONDITION));
                    assertThat(count(c,"repository_coordinator_supersessions")).isZero();
                    assertThat(claimAndOwner(c,command)).containsExactly(beforeMismatch);
                    try (var payloads=recovery.prepare(CALLER,command,bodies,MODES,NONE)) {
                        assertThat(payloads).isNotNull();
                        assertThat(payloads.bodies()).hasSize(bodies.size());
                        assertThat(count(c,"repository_coordinator_supersessions")).isEqualTo(1);
                        assertThat(count(c,"repository_successor_installs")).isEqualTo(installed ? 2 : 1);
                        assertThat(count(c,"repository_successor_executions")).isEqualTo(1);
                        assertThat(second.sessions().retainedSessions()).isEqualTo(1);
                    }
                }
                attempts.close();
                assertThat(attempts.detachClosed(Duration.ZERO,key -> CALLER,NONE)).isTrue();
                assertThat(recovery.detach(Duration.ZERO,NONE)).isTrue();
                assertThat(firstBudget.reservedBytes()).isZero();
                assertThat(secondBudget.reservedBytes()).isZero();
            }
        }
    }

    @Test void recoveryPayloadCapacityFailurePrecedesDurableSuccession() throws Exception {
        try (var c=context(POSTGRES)) {
            var source=source(c);
            var budget=new PayloadBudget(64_000_000);
            try (var resources=DocumentJournaledSessionsIT.resources(c.tx(),2,1_000_000,LEASE,budget)) {
                var recovery=new RepositoryManagedRecovery(c.tx(),budget,resources.sessions(),LEASE,TIMEOUTS,2,
                        (account,principal,operation) -> CALLER);
                var bodies=new HashMap<DocumentUploadPayloads.Key,ai.protomolt.proto.repo.codec.PartObject>();
                for (var member : source.command().intent().getMembersList()) {
                    for (int ordinal=0;ordinal<member.getPartsCount();ordinal++) {
                        var part=member.getParts(ordinal);
                        if (part.hasUpload()) bodies.put(new DocumentUploadPayloads.Key(member.getMemberId(),ordinal),
                                new ai.protomolt.proto.repo.codec.PartObject(part.getSlot().getPart(),part.getSlot().getSubKey(),
                                        new byte[Math.toIntExact(part.getUpload().getSizeBytes())],part.getUpload().getSha256()));
                    }
                }
                try (var pressure=budget.reserve(budget.capacity()); var call=recovery.calls.enter(CALLER,source.command())) {
                    assertThatThrownBy(() -> recovery.prepare(CALLER,source.command(),bodies,MODES,NONE))
                            .isInstanceOf(PayloadBudget.CapacityExceededException.class);
                    assertThat(budget.reservedBytes()).isEqualTo(pressure.bytes());
                    assertThat(count(c,"repository_coordinator_reservations")).isZero();
                    assertThat(count(c,"repository_successor_installs")).isZero();
                    assertThat(count(c,"repository_successor_executions")).isZero();
                }
                assertThat(recovery.detach(Duration.ZERO,NONE)).isTrue();
                assertThat(budget.reservedBytes()).isZero();
            }
        }
    }

    @ParameterizedTest @ValueSource(booleans={false,true})
    void freshAndTerminalRoutingDoNotRequireRecoveryAuthority(boolean terminal) throws Exception {
        try (var c=context(POSTGRES)) {
            var input=input(c);
            var budget=new PayloadBudget(64_000_000);
            if (terminal) {
                var claim=new DocumentPublicationPreparationJournal(c.tx(),budget)
                        .acquireInitial(CALLER,input,UUID.randomUUID(),NONE);
                new DocumentPublicationModesJournal(c.tx(),budget).bind(CALLER,claim,0,MODES,NONE);
                var owner=new RepositoryOperationLedger(c.tx()).admit(input.key(),input.command(),
                        input.seeds().ownerNonce(),LEASE,claim).owner().orElseThrow();
                new DocumentPublicationRejections(c.tx()).cancel(CALLER,owner,input.command(),NONE);
            }
            try (var resources=DocumentJournaledSessionsIT.resources(c.tx(),2,1_000_000,LEASE,budget)) {
                var recovery=new RepositoryManagedRecovery(c.tx(),budget,resources.sessions(),LEASE,TIMEOUTS,2,
                        (account,principal,operation) -> { throw new AssertionError("Normal routing requested recovery authority"); });
                try (var call=recovery.calls.enter(CALLER,input.command())) {
                    recovery.prepare(CALLER,input.command(),Map.of(),Map.of(),NONE);
                }
                assertThat(count(c,"repository_coordinator_reservations")).isZero();
                assertThat(recovery.detach(Duration.ZERO,NONE)).isTrue();
            }
        }
    }

    @Test void managedRetryDoesNotExposeOwnerlessDiscoveryState() throws Exception {
        try (var c=context(POSTGRES)) {
            var input=input(c);
            new RepositoryExecutionClaimLedger(c.tx()).acquire(input.key(),input.command(),UUID.randomUUID(),Duration.ofSeconds(1));
            c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(1.1)").getSingleResult());
            var budget=new PayloadBudget(64_000_000);
            try (var resources=DocumentJournaledSessionsIT.resources(c.tx(),2,1_000_000,LEASE,budget)) {
                var recovery=new RepositoryManagedRecovery(c.tx(),budget,resources.sessions(),LEASE,TIMEOUTS,2,
                        (account,principal,operation) -> CALLER);
                var scoped=new RepositoryCaller(CALLER.principalName(),false,Set.of("account"),Set.of());
                try (var call=recovery.calls.enter(scoped,input.command())) {
                    assertThatThrownBy(() -> recovery.prepare(scoped,input.command(),Map.of(),MODES,NONE))
                            .isInstanceOfSatisfying(RepositoryException.class,e -> {
                                assertThat(e.code()).isEqualTo(RepositoryException.Code.FAILED_PRECONDITION);
                                assertThat(e.getMessage()).isEqualTo("Operation is not eligible for managed retry");
                            });
                }
                assertThat(count(c,"repository_coordinator_reservations")).isZero();
                assertThat(recovery.detach(Duration.ZERO,NONE)).isTrue();
            }
        }
    }

    @Test void publicationRoutingGuardIsBoundedPerKeyAndDoesNotQueueDuplicates() throws Exception {
        try (var c=context(POSTGRES); var executor=java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var command=source(c).command();
            var calls=new RepositoryPublicationCalls(2);
            try (var first=calls.enter(CALLER,command)) {
                var duplicate=executor.submit(() -> {
                    assertThatThrownBy(() -> calls.enter(CALLER,command)).isInstanceOfSatisfying(RepositoryException.class,
                            e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.CONFLICT));
                });
                duplicate.get(2,java.util.concurrent.TimeUnit.SECONDS);
                try (var independent=calls.enter(new RepositoryCaller("other",true),command)) {
                    assertThatThrownBy(() -> calls.enter(new RepositoryCaller("third",true),command))
                            .isInstanceOfSatisfying(RepositoryException.class,
                                    e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.RESOURCE_EXHAUSTED));
                    first.close();
                    try (var replacement=calls.enter(CALLER,command)) {
                        first.close();
                        assertThatThrownBy(() -> calls.enter(CALLER,command)).isInstanceOfSatisfying(RepositoryException.class,
                                e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.CONFLICT));
                    }
                }
            }
            assertThatThrownBy(() -> calls.enter(null,command)).isInstanceOfSatisfying(RepositoryException.class,
                    e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.UNAUTHENTICATED));
        }
    }

    @ParameterizedTest @ValueSource(booleans={false,true})
    void partialDisposalCanRetryAfterLaterAuthorityFailureOrCancellation(boolean cancellation) throws Exception {
        try (var c=context(POSTGRES)) {
            var input=input(c);
            var secondCommand=new DocumentPublicationCommand(input.command().intent().toBuilder().setOperationId(UUID.randomUUID().toString()).build());
            var secondKey=new RepositoryOperationLedger.Key(input.key().account(),input.key().principal(),secondCommand.operationId());
            var first=source(c,input);
            var second=source(c,new DocumentPublicationPreparationRecord(secondKey,secondCommand,
                    DocumentPublicationSeeds.mint(secondKey,secondCommand),input.placements(),LEASE,0));
            var budget=new PayloadBudget(128_000_000);
            try (var resources=DocumentJournaledSessionsIT.resources(c.tx(),2,1_000_000,LEASE,budget);
                 var attempts=new RepositoryRecoveryAttempts(c.tx(),budget,resources.sessions(),LEASE,TIMEOUTS,2)) {
                for (var source : List.of(first,second)) {
                    try (var attempt=attempts.begin(CALLER,source.command(),source.observation())) {
                        attempt.advance(CALLER,CALLER,MODES,NONE);
                    }
                }
                attempts.close();
                var visits=new java.util.concurrent.atomic.AtomicInteger(); var cancelled=new AtomicBoolean();
                var control=new RepositoryReadControl() {
                    public boolean isCancelled() { return cancelled.get(); }
                    public long remainingNanos() { return Long.MAX_VALUE; }
                };
                assertThatThrownBy(() -> attempts.detachClosed(Duration.ZERO,key -> {
                    if (visits.incrementAndGet()!=2) return CALLER;
                    if (cancellation) { cancelled.set(true); return CALLER; }
                    return new RepositoryCaller(CALLER.principalName(),false,Set.of("account"),Set.of());
                },control)).isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code()).isEqualTo(
                        cancellation ? RepositoryException.Code.CANCELLED : RepositoryException.Code.PERMISSION_DENIED));
                assertThat(attempts.drain()).isEqualTo(new RepositoryRecoveryAttempts.Drain(0,1));
                assertThat(budget.reservedBytes()).isPositive();
                assertThat(count(c,"repository_coordinator_reservations")).isEqualTo(2);
                assertThat(attempts.detachClosed(Duration.ZERO,key -> CALLER,NONE)).isTrue();
                assertThat(attempts.drain()).isEqualTo(new RepositoryRecoveryAttempts.Drain(0,0));
                assertThat(budget.reservedBytes()).isZero();
                assertThat(count(c,"repository_coordinator_reservations")).isEqualTo(2);
            }
        }
    }

    @ParameterizedTest @ValueSource(ints={0,1,2})
    void closedMetadataRecoveryReleasesBytesWithoutChangingDurableState(int completedPhases) throws Exception {
        try (var c=context(POSTGRES)) {
            var source=source(c); var budget=new PayloadBudget(128_000_000);
            try (var resources=DocumentJournaledSessionsIT.resources(c.tx(),2,1_000_000,LEASE,budget);
                 var attempts=new RepositoryRecoveryAttempts(c.tx(),budget,resources.sessions(),LEASE,TIMEOUTS,1)) {
                assertThatThrownBy(() -> attempts.awaitIdle(Duration.ZERO)).hasMessageContaining("Close recovery");
                try (var attempt=attempts.begin(CALLER,source.command(),source.observation())) {
                    for (int i=0;i<completedPhases;i++) attempt.advance(CALLER,CALLER,MODES,NONE);
                    attempts.close();
                    assertThat(attempts.awaitIdle(Duration.ZERO)).isFalse();
                    assertThat(attempts.detachClosed(Duration.ZERO,key -> CALLER,NONE)).isFalse();
                    assertThat(budget.reservedBytes()).isPositive();
                }
                var reservations=count(c,"repository_coordinator_reservations");
                var installs=count(c,"repository_successor_installs");
                assertThat(attempts.awaitIdle(Duration.ZERO)).isTrue();
                assertThat(attempts.detachClosed(Duration.ZERO,key -> CALLER,NONE)).isTrue();
                assertThat(attempts.drain()).isEqualTo(new RepositoryRecoveryAttempts.Drain(0,0));
                assertThat(budget.reservedBytes()).isZero();
                assertThat(count(c,"repository_coordinator_reservations")).isEqualTo(reservations);
                assertThat(count(c,"repository_successor_installs")).isEqualTo(installs);
                assertThat(count(c,"repository_successor_executions")).isZero();
                assertThat(attempts.detachClosed(Duration.ZERO,key -> CALLER,NONE)).isTrue();
                assertThatThrownBy(() -> attempts.resume(CALLER,source.command())).hasMessageContaining("admission is closed");
            }
        }
    }

    @Test void closingLastAcceptedHandleWakesRecoveryWaiter() throws Exception {
        try (var c=context(POSTGRES)) {
            var source=source(c); var budget=new PayloadBudget(128_000_000);
            try (var resources=DocumentJournaledSessionsIT.resources(c.tx(),2,1_000_000,LEASE,budget);
                 var attempts=new RepositoryRecoveryAttempts(c.tx(),budget,resources.sessions(),LEASE,TIMEOUTS,1);
                 var attempt=attempts.begin(CALLER,source.command(),source.observation());
                 var executor=java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
                attempts.close();
                var entered=new java.util.concurrent.CountDownLatch(1);
                var waiter=executor.submit(() -> { entered.countDown(); return attempts.awaitIdle(Duration.ofSeconds(10)); });
                try {
                    assertThat(entered.await(2,java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                    assertThatThrownBy(() -> waiter.get(50,java.util.concurrent.TimeUnit.MILLISECONDS))
                            .isInstanceOf(java.util.concurrent.TimeoutException.class);
                    attempt.close();
                    assertThat(waiter.get(2,java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                    assertThat(attempts.drain()).isEqualTo(new RepositoryRecoveryAttempts.Drain(0,1));
                    assertThat(attempts.detachClosed(Duration.ZERO,key -> CALLER,NONE)).isTrue();
                } finally { waiter.cancel(true); }
            }
        }
    }

    @Test void rolledBackActivationTransfersShutdownOwnershipBeforeReleasingRecoveryBytes() throws Exception {
        try (var c=context(POSTGRES)) {
            var source=source(c); var budget=new PayloadBudget(128_000_000); var armed=new AtomicBoolean(true);
            var datasource=DocumentJdbcFaults.beforeCommit(c.pool(),connection -> {
                try (var statement=connection.createStatement(); var rows=statement.executeQuery("SELECT count(*) FROM repository_successor_executions")) {
                    rows.next();
                    if (rows.getInt(1)!=0 && armed.compareAndSet(true,false))
                        throw new java.sql.SQLException("Recovery activation rolled back for disposal test","08006");
                }
            });
            try (var emf=jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                    Map.of("hibernate.connection.datasource",datasource,"hibernate.hbm2ddl.auto","validate"));
                 var resources=DocumentJournaledSessionsIT.resources(new Tx(emf).withTimeouts(TIMEOUTS),2,1_000_000,LEASE,budget);
                 var attempts=new RepositoryRecoveryAttempts(new Tx(emf),budget,resources.sessions(),LEASE,TIMEOUTS,1)) {
                try (var attempt=attempts.begin(CALLER,source.command(),source.observation())) {
                    attempt.advance(CALLER,CALLER,MODES,NONE);
                    attempt.advance(CALLER,CALLER,MODES,NONE);
                    assertThatThrownBy(() -> attempt.advance(CALLER,CALLER,MODES,NONE))
                            .hasStackTraceContaining("Recovery activation rolled back for disposal test");
                }
                assertThat(resources.sessions().retainedSessions()).isEqualTo(1);
                assertThat(budget.reservedBytes()).isPositive();
                attempts.close();
                assertThat(attempts.detachClosed(Duration.ZERO,key -> CALLER,NONE)).isTrue();
                assertThat(budget.reservedBytes()).isZero();
                assertThat(resources.sessions().retainedSessions()).isEqualTo(1);
                assertThat(resources.sessions().drainRegistrations(Duration.ZERO,key -> CALLER,NONE).detached()).isEqualTo(1);
                assertThat(resources.sessions().awaitIdle(Duration.ZERO)).isTrue();
                assertThat(resources.sessions().attestLocalDrain(key -> CALLER,NONE)).isTrue();
                assertThat(count(c,"repository_coordinator_reservations")).isEqualTo(1);
                assertThat(count(c,"repository_successor_installs")).isEqualTo(1);
                assertThat(count(c,"repository_successor_executions")).isZero();
            }
        }
    }

    @ParameterizedTest @org.junit.jupiter.params.provider.CsvSource({"1,false","2,false","3,false","1,true","2,true","3,true"})
    void expiredUnactivatedAttemptKeepsPendingSupersessionAcrossLostReply(int completedPhases, boolean shutdown) throws Exception {
        try (var c=context(POSTGRES)) {
            var source=source(c); var cancelled=new AtomicBoolean(); var cancelOnce=new AtomicBoolean(true);
            var failActivation=new AtomicBoolean(completedPhases==3);
            var failingActivation=DocumentJdbcFaults.beforeCommit(c.pool(), connection -> {
                if (!failActivation.get()) return;
                try (var query=connection.createStatement(); var rows=query.executeQuery("SELECT count(*) FROM repository_successor_executions")) {
                    rows.next();
                    if (rows.getInt(1)==1 && failActivation.compareAndSet(true,false))
                        throw new java.sql.SQLException("Activation commit refused by test", "08006");
                }
            });
            var datasource=DocumentJdbcFaults.afterCommit(failingActivation,() -> {
                if (count(c,"repository_coordinator_supersessions")==1 && cancelOnce.compareAndSet(true,false)) cancelled.set(true);
            });
            var control=new RepositoryReadControl() {
                public boolean isCancelled() { return cancelled.get(); }
                public long remainingNanos() { return Long.MAX_VALUE; }
            };
            var budget=new PayloadBudget(128_000_000); var lease=Duration.ofSeconds(3);
            try (var emf=jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",Map.of(
                    "hibernate.connection.datasource",datasource,"hibernate.hbm2ddl.auto","validate"));
                 var resources=DocumentJournaledSessionsIT.resources(new Tx(emf).withTimeouts(TIMEOUTS),1,1_000_000,lease,budget);
                 var attempts=new RepositoryRecoveryAttempts(new Tx(emf),budget,resources.sessions(),lease,TIMEOUTS,1)) {
                RepositoryCoordinatorReservation.Proposal original;
                try (var attempt=attempts.begin(CALLER,source.command(),source.observation())) {
                    original=attempt.proposal();
                    assertThat(attempt.advance(CALLER,CALLER,MODES,NONE)).isEqualTo(RepositoryRecoveryAttempts.Phase.RESERVED);
                    if (completedPhases>=2) assertThat(attempt.advance(CALLER,CALLER,MODES,NONE)).isEqualTo(RepositoryRecoveryAttempts.Phase.INSTALLED);
                    if (completedPhases==3) {
                        assertThatThrownBy(() -> attempt.advance(CALLER,CALLER,MODES,NONE)).hasStackTraceContaining("Activation commit refused by test");
                        assertThat(resources.sessions().retainedSessions()).isEqualTo(1);
                        assertThat(count(c,"repository_successor_executions")).isZero();
                    }
                    assertThatThrownBy(() -> attempt.supersedeExpired(CALLER,CALLER,NONE)).hasMessageContaining("not expired and unactivated");
                    expire(c,source.command());
                    long held=budget.reservedBytes();
                    assertThatThrownBy(() -> attempt.supersedeExpired(CALLER,CALLER,control))
                            .isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.CANCELLED));
                    assertThat(cancelOnce).isFalse();
                    assertThat(count(c,"repository_coordinator_supersessions")).isEqualTo(1);
                    assertThat(attempt.proposal()).isSameAs(original);
                    assertThat(budget.reservedBytes()).isEqualTo(held);
                    assertThatThrownBy(() -> attempt.advance(CALLER,CALLER,MODES,NONE)).hasMessageContaining("Pending supersession");
                }
                Object committed=c.tx().readOnly(em -> em.createNativeQuery("SELECT successor_token FROM repository_coordinator_supersessions").getSingleResult());
                if (shutdown) {
                    attempts.close();
                    assertThat(attempts.detachClosed(Duration.ZERO,key -> CALLER,NONE)).isTrue();
                    assertThat(attempts.drain()).isEqualTo(new RepositoryRecoveryAttempts.Drain(0,0));
                    assertThat(budget.reservedBytes()).isZero();
                    assertThat(count(c,"repository_coordinator_supersessions")).isEqualTo(1);
                    assertThat(count(c,"repository_successor_executions")).isZero();
                    assertThat(c.tx().<Object>readOnly(em -> em.createNativeQuery("SELECT successor_token FROM repository_coordinator_supersessions").getSingleResult())).isEqualTo(committed);
                    var progress=resources.sessions().drainRegistrations(Duration.ZERO,key -> CALLER,NONE);
                    assertThat(progress.unresolved()).isZero();
                    assertThat(progress.fenced()).isEqualTo(completedPhases==3 ? 1 : 0);
                    assertThat(resources.sessions().awaitIdle(Duration.ZERO)).isTrue();
                    assertThat(resources.sessions().attestLocalDrain(key -> CALLER,NONE)).isTrue();
                    return;
                }
                try (var retry=attempts.resume(CALLER,source.command()).orElseThrow()) {
                    var beforeRetry=claimAndOwner(c,source.command());
                    assertThatThrownBy(() -> retry.reconcileUnactivated(CALLER,CALLER,
                            Map.of("member-0",DocumentPublicationCandidate.Mode.OPAQUE),NONE))
                            .isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code())
                                    .isEqualTo(RepositoryException.Code.FAILED_PRECONDITION));
                    assertThat(claimAndOwner(c,source.command())).containsExactly(beforeRetry);
                    assertThat(retry.proposal()).isSameAs(original);
                    assertThat(retry.reconcileUnactivated(CALLER,CALLER,MODES,NONE)).isTrue();
                    assertThat(retry.proposal().successorToken()).isEqualTo(committed);
                    assertThat(retry.proposal().successorIncarnation()).isNotEqualTo(original.successorIncarnation());
                    assertThat(retry.proposal().predecessor().epoch()).isEqualTo(original.predecessor().epoch()+1);
                    assertThat(budget.reservedBytes()).isEqualTo((long)source.command().canonical().size()+source.command().intent().getSerializedSize());
                    assertThat(retry.advance(CALLER,CALLER,MODES,NONE)).isEqualTo(RepositoryRecoveryAttempts.Phase.INSTALLED);
                    assertThat(retry.advance(CALLER,CALLER,MODES,NONE)).isEqualTo(RepositoryRecoveryAttempts.Phase.ACTIVATED);
                    assertThat(resources.sessions().retainedSessions()).isEqualTo(1);
                    assertThat(count(c,"repository_coordinator_supersessions")).isEqualTo(1);
                    assertThat(count(c,"repository_successor_executions")).isEqualTo(1);
                    assertThat(count(c,"repository_successor_installs")).isEqualTo(completedPhases==1 ? 1 : 2);
                    assertThat(budget.reservedBytes()).isZero();
                }
            }
        }
    }

    @Test void liveCommittedActivationLostReplyReattachesExactSuccessor() throws Exception {
        try (var c=context(POSTGRES)) {
            var source=source(c); var cancelled=new AtomicBoolean();
            var datasource=DocumentJdbcFaults.afterCommit(c.pool(),() -> {
                if (count(c,"repository_successor_executions")==1) cancelled.set(true);
            });
            var control=new RepositoryReadControl() {
                public boolean isCancelled() { return cancelled.get(); }
                public long remainingNanos() { return Long.MAX_VALUE; }
            };
            var attemptBudget=new PayloadBudget(128_000_000); var lease=Duration.ofSeconds(30);
            try (var emf=jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",Map.of(
                    "hibernate.connection.datasource",datasource,"hibernate.hbm2ddl.auto","validate"));
                 var resources=DocumentJournaledSessionsIT.resources(new Tx(emf).withTimeouts(TIMEOUTS),1,1_000_000,lease,new PayloadBudget(128_000_000));
                 var attempts=new RepositoryRecoveryAttempts(new Tx(emf),attemptBudget,resources.sessions(),lease,TIMEOUTS,1)) {
                try (var attempt=attempts.begin(CALLER,source.command(),source.observation())) {
                    var original=attempt.proposal();
                    attempt.advance(CALLER,CALLER,MODES,NONE); attempt.advance(CALLER,CALLER,MODES,NONE);
                    assertThatThrownBy(() -> attempt.advance(CALLER,CALLER,MODES,control))
                            .isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.CANCELLED));
                    assertThat(count(c,"repository_successor_executions")).isEqualTo(1);
                    assertThat(attempt.reconcileUnactivated(CALLER,CALLER,MODES,NONE)).isFalse();
                    assertThat(attempt.advance(CALLER,CALLER,MODES,NONE))
                            .isEqualTo(RepositoryRecoveryAttempts.Phase.ACTIVATED);
                    assertThat(attempt.proposal()).isSameAs(original);
                    assertThat(count(c,"repository_successor_executions")).isEqualTo(1);
                    assertThat(count(c,"repository_successor_installs")).isEqualTo(1);
                    assertThat(count(c,"repository_coordinator_supersessions")).isZero();
                    assertThat(resources.sessions().retainedSessions()).isEqualTo(1);
                    assertThat(attemptBudget.reservedBytes()).isZero();
                }
                attempts.close();
                assertThat(attempts.drain()).isEqualTo(new RepositoryRecoveryAttempts.Drain(0,0));
            }
        }
    }

    @ParameterizedTest @ValueSource(booleans={false,true})
    void expiredCommittedActivationRetainsNewReservationAndReactivates(boolean lostReservationReply) throws Exception {
        try (var c=context(POSTGRES)) {
            var source=source(c); var cancelled=new AtomicBoolean(); var reservationReply=new AtomicBoolean();
            var datasource=DocumentJdbcFaults.afterCommit(c.pool(),() -> {
                if ((!reservationReply.get() && count(c,"repository_successor_executions")==1)
                        || (reservationReply.get() && count(c,"repository_coordinator_expirations")==2)) cancelled.set(true);
            });
            var control=new RepositoryReadControl() {
                public boolean isCancelled() { return cancelled.get(); }
                public long remainingNanos() { return Long.MAX_VALUE; }
            };
            var attemptBudget=new PayloadBudget(128_000_000); var lease=Duration.ofSeconds(3);
            try (var emf=jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",Map.of(
                    "hibernate.connection.datasource",datasource,"hibernate.hbm2ddl.auto","validate"));
                 var resources=DocumentJournaledSessionsIT.resources(new Tx(emf).withTimeouts(TIMEOUTS),1,1_000_000,lease,new PayloadBudget(128_000_000));
                 var attempts=new RepositoryRecoveryAttempts(new Tx(emf),attemptBudget,resources.sessions(),lease,TIMEOUTS,1)) {
                try (var attempt=attempts.begin(CALLER,source.command(),source.observation())) {
                    var original=attempt.proposal();
                    attempt.advance(CALLER,CALLER,MODES,NONE); attempt.advance(CALLER,CALLER,MODES,NONE);
                    assertThatThrownBy(() -> attempt.advance(CALLER,CALLER,MODES,control))
                            .isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.CANCELLED));
                    assertThat(count(c,"repository_successor_executions")).isEqualTo(1);
                    expire(c,source.command());
                    assertThat(new RepositoryCoordinatorRecoveryDiscovery(c.tx(),TIMEOUTS)
                            .inspect(CALLER,original.predecessor().key(),source.command().sha256(),NONE).status())
                            .isEqualTo(RepositoryCoordinatorRecoveryDiscovery.Status.EXPIRED_BOUND);
                    var wrong=Map.of("target",DocumentPublicationCandidate.Mode.OPAQUE);
                    assertThatThrownBy(() -> attempt.reconcileExpiredBound(CALLER,CALLER,wrong,NONE))
                            .isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code())
                                    .isEqualTo(RepositoryException.Code.FAILED_PRECONDITION));
                    assertThat(attempt.proposal()).isSameAs(original);
                    assertThat(count(c,"repository_coordinator_expirations")).isEqualTo(1);
                    Object[] pendingIdentity=null;
                    if (lostReservationReply) {
                        reservationReply.set(true); cancelled.set(false);
                        assertThatThrownBy(() -> attempt.reconcileExpiredBound(CALLER,CALLER,MODES,control))
                                .isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code())
                                        .isEqualTo(RepositoryException.Code.CANCELLED));
                        assertThat(count(c,"repository_coordinator_expirations")).isEqualTo(2);
                        assertThat(attempt.proposal()).isSameAs(original);
                        pendingIdentity=claimAndOwner(c,source.command());
                        assertThat(attempt.retireFenced(CALLER,CALLER,NONE)).isFalse();
                        assertThatThrownBy(() -> attempt.advance(CALLER,CALLER,MODES,NONE))
                                .hasMessageContaining("Pending supersession");
                        assertThatThrownBy(() -> attempt.reconcileUnactivated(CALLER,CALLER,MODES,NONE))
                                .hasMessageContaining("Pending bound reservation");
                        assertThatThrownBy(() -> attempt.supersedeExpired(CALLER,CALLER,NONE))
                                .hasMessageContaining("Pending bound reservation");
                        assertThatThrownBy(() -> attempt.reconcileExpiredBound(CALLER,CALLER,wrong,NONE))
                                .isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code())
                                        .isEqualTo(RepositoryException.Code.FAILED_PRECONDITION));
                    }
                    assertThat(attempt.reconcileExpiredBound(CALLER,CALLER,MODES,NONE)).isTrue();
                    if (pendingIdentity!=null)
                        assertThat(claimAndOwner(c,source.command())).containsExactly(pendingIdentity);

                    assertThat(attempt.proposal().predecessor().epoch()).isEqualTo(original.predecessor().epoch()+1);
                    assertThat(attempt.advance(CALLER,CALLER,MODES,NONE)).isEqualTo(RepositoryRecoveryAttempts.Phase.INSTALLED);
                    assertThat(attempt.advance(CALLER,CALLER,MODES,NONE)).isEqualTo(RepositoryRecoveryAttempts.Phase.ACTIVATED);
                    assertThat(count(c,"repository_coordinator_expirations")).isEqualTo(2);
                    assertThat(count(c,"repository_successor_executions")).isEqualTo(2);
                    assertThat(count(c,"repository_coordinator_supersessions")).isZero();
                    assertThat(resources.sessions().retainedSessions()).isEqualTo(1);
                    assertThat(attemptBudget.reservedBytes()).isZero();
                }
                attempts.close();
                assertThat(attempts.drain()).isEqualTo(new RepositoryRecoveryAttempts.Drain(0,0));
            }
        }
    }

    @Test void expiredCommittedActivationCannotBeSupersededAsUnactivated() throws Exception {
        try (var c=context(POSTGRES)) {
            var source=source(c); var cancelled=new AtomicBoolean();
            var datasource=DocumentJdbcFaults.afterCommit(c.pool(),() -> {
                if (count(c,"repository_successor_executions")==1) cancelled.set(true);
            });
            var control=new RepositoryReadControl() {
                public boolean isCancelled() { return cancelled.get(); }
                public long remainingNanos() { return Long.MAX_VALUE; }
            };
            var attemptBudget=new PayloadBudget(128_000_000); var lease=Duration.ofSeconds(3);
            try (var emf=jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",Map.of(
                    "hibernate.connection.datasource",datasource,"hibernate.hbm2ddl.auto","validate"));
                 var resources=DocumentJournaledSessionsIT.resources(new Tx(emf).withTimeouts(TIMEOUTS),1,1_000_000,lease,new PayloadBudget(128_000_000));
                 var attempts=new RepositoryRecoveryAttempts(new Tx(emf),attemptBudget,resources.sessions(),lease,TIMEOUTS,1)) {
                try (var attempt=attempts.begin(CALLER,source.command(),source.observation())) {
                    var original=attempt.proposal();
                    attempt.advance(CALLER,CALLER,MODES,NONE); attempt.advance(CALLER,CALLER,MODES,NONE);
                    assertThatThrownBy(() -> attempt.advance(CALLER,CALLER,MODES,control))
                            .isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.CANCELLED));
                    assertThat(count(c,"repository_successor_executions")).isEqualTo(1);
                    expire(c,source.command());
                    assertThat(new RepositoryCoordinatorRecoveryDiscovery(c.tx(),TIMEOUTS)
                            .inspect(CALLER,original.predecessor().key(),source.command().sha256(),NONE).status())
                            .isEqualTo(RepositoryCoordinatorRecoveryDiscovery.Status.EXPIRED_BOUND);
                    long held=attemptBudget.reservedBytes();
                    assertThat(attempt.reconcileUnactivated(CALLER,CALLER,MODES,NONE)).isFalse();
                    assertThatThrownBy(() -> attempt.supersedeExpired(CALLER,CALLER,NONE)).hasMessageContaining("not expired and unactivated");
                    assertThat(count(c,"repository_coordinator_supersessions")).isZero();
                    assertThat(attempt.proposal()).isSameAs(original);
                    assertThat(attemptBudget.reservedBytes()).isEqualTo(held).isPositive();
                    attempts.close();
                }
                // Retaining unresolved identity is intentional; close is not reconciliation.
                assertThat(attempts.drain()).isEqualTo(new RepositoryRecoveryAttempts.Drain(0,1));
            }
        }
    }

    @ParameterizedTest @ValueSource(booleans={false,true})
    void differentCoordinatorWinnerCannotReplaceRetainedIdentity(boolean pending) throws Exception {
        try (var c=context(POSTGRES)) {
            var source=source(c); var budget=new PayloadBudget(128_000_000); var lease=Duration.ofSeconds(3);
            var failOnce=new AtomicBoolean(pending);
            var cancelReadback=new AtomicBoolean(); var cancelled=new AtomicBoolean();
            var failingCommit=DocumentJdbcFaults.beforeCommit(c.pool(), connection -> {
                if (!failOnce.get()) return;
                try (var query=connection.createStatement(); var rows=query.executeQuery("SELECT count(*) FROM repository_coordinator_supersessions")) {
                    rows.next();
                    if (rows.getInt(1)==1 && failOnce.compareAndSet(true,false))
                        throw new java.sql.SQLException("Supersession commit refused by test", "08006");
                }
            });
            var datasource=DocumentJdbcFaults.afterCommit(failingCommit,() -> {
                if (cancelReadback.compareAndSet(true,false)) cancelled.set(true);
            });
            try (var emf=jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",Map.of(
                    "hibernate.connection.datasource",datasource,"hibernate.hbm2ddl.auto","validate"));
                 var resources=DocumentJournaledSessionsIT.resources(new Tx(emf).withTimeouts(TIMEOUTS),1,1_000_000,LEASE,new PayloadBudget(128_000_000));
                 var attempts=new RepositoryRecoveryAttempts(new Tx(emf),budget,resources.sessions(),lease,TIMEOUTS,1)) {
                try (var attempt=attempts.begin(CALLER,source.command(),source.observation())) {
                    var original=attempt.proposal();
                    assertThat(attempt.retireFenced(CALLER,CALLER,NONE)).isFalse();
                    attempt.advance(CALLER,CALLER,MODES,NONE); attempt.advance(CALLER,CALLER,MODES,NONE);
                    expire(c,source.command());
                    assertThat(attempt.retireFenced(CALLER,CALLER,NONE)).isFalse();
                    if (pending) {
                        assertThatThrownBy(() -> attempt.supersedeExpired(CALLER,CALLER,NONE))
                                .hasStackTraceContaining("Supersession commit refused by test");
                        assertThat(failOnce).isFalse();
                        assertThat(count(c,"repository_coordinator_supersessions")).isZero();
                    }
                    var observed=new RepositoryCoordinatorRecoveryDiscovery(c.tx(),TIMEOUTS)
                            .inspect(CALLER,original.predecessor().key(),source.command().sha256(),NONE).unactivated().orElseThrow();
                    var winner=new RepositoryCoordinatorReservation.SupersededUnactivated(observed.predecessor(),UUID.randomUUID(),UUID.randomUUID(),
                            lease,observed.owner(),observed.preparationSha256(),observed.installation());
                    RepositoryCoordinatorSupersession.reserve(c.tx(),CALLER,winner,NONE);
                    expire(c,source.command());
                    long held=budget.reservedBytes();
                    assertThatThrownBy(() -> attempt.reconcileUnactivated(CALLER,CALLER,MODES,NONE))
                            .hasMessageContaining(pending ? "differs from original binding" : "differs from retained attempt");
                    assertThatThrownBy(() -> attempt.supersedeExpired(CALLER,CALLER,NONE))
                            .hasMessageContaining(pending ? "differs from original binding" : "differs from retained attempt");
                    if (pending) assertThatThrownBy(() -> attempt.advance(CALLER,CALLER,MODES,NONE)).hasMessageContaining("Pending supersession");
                    else assertThatThrownBy(() -> attempt.advance(CALLER,CALLER,MODES,NONE))
                            .hasStackTraceContaining("Execution requires exact live successor claim");
                    assertThat(count(c,"repository_successor_executions")).isZero();
                    assertThat(attempt.proposal()).isSameAs(original);
                    assertThat(budget.reservedBytes()).isEqualTo(held).isPositive();
                    assertThat(count(c,"repository_coordinator_supersessions")).isEqualTo(1);
                    assertThat(c.tx().<Object>readOnly(em -> em.createNativeQuery("SELECT successor_token FROM repository_coordinator_supersessions").getSingleResult()))
                            .isEqualTo(winner.successorToken());
                    var before=claimAndOwner(c,source.command());
                    var control=new RepositoryReadControl() {
                        public boolean isCancelled() { return cancelled.get(); }
                        public long remainingNanos() { return Long.MAX_VALUE; }
                    };
                    cancelReadback.set(true);
                    assertThatThrownBy(() -> attempt.retireFenced(CALLER,CALLER,control))
                            .isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.CANCELLED));
                    assertThat(cancelReadback).isFalse();
                    assertThat(attempt.proposal()).isSameAs(original);
                    assertThat(budget.reservedBytes()).isEqualTo(held);
                    attempts.close();
                    assertThat(attempts.drain()).isEqualTo(new RepositoryRecoveryAttempts.Drain(1,1));
                    assertThat(attempt.retireFenced(CALLER,CALLER,NONE)).isTrue();
                    assertThat(attempt.retireFenced(CALLER,CALLER,NONE)).isTrue();
                    assertThat(budget.reservedBytes()).isZero();
                    assertThat(claimAndOwner(c,source.command())).containsExactly(before);
                    assertThatThrownBy(() -> attempt.advance(CALLER,CALLER,MODES,NONE)).hasMessageContaining("retired");
                    assertThat(resources.sessions().retainedSessions()).isEqualTo(pending ? 0 : 1);
                    assertThat(resources.sessions().retireSuperseded(CALLER,source.command(),NONE)).isFalse();
                    var scoped=new RepositoryCaller(CALLER.principalName(),false,Set.of("account"),Set.of());
                    assertThatThrownBy(() -> resources.sessions().retireClaimFenced(scoped,source.command(),NONE))
                            .isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.PERMISSION_DENIED));
                    if (!pending) {
                        cancelled.set(false); cancelReadback.set(true);
                        assertThatThrownBy(() -> resources.sessions().retireClaimFenced(CALLER,source.command(),control))
                                .isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.CANCELLED));
                        assertThat(cancelReadback).isFalse();
                        assertThat(resources.sessions().retainedSessions()).isEqualTo(1);
                        assertThat(resources.sessions().retainedCommandBytes()).isPositive();
                    }
                    assertThat(resources.sessions().retireClaimFenced(CALLER,source.command(),NONE)).isEqualTo(!pending);
                    assertThat(resources.sessions().retainedSessions()).isZero();
                    assertThat(resources.sessions().retainedCommandBytes()).isZero();
                    assertThat(claimAndOwner(c,source.command())).containsExactly(before);
                    attempts.close();
                    assertThat(attempts.drain()).isEqualTo(new RepositoryRecoveryAttempts.Drain(1,0));
                }
                assertThat(attempts.drain()).isEqualTo(new RepositoryRecoveryAttempts.Drain(0,0));
            }
        }
    }

    private static Object[] claimAndOwner(Context c, DocumentPublicationCommand command) {
        return c.tx().readOnly(em -> (Object[])em.createNativeQuery("""
                SELECT c.claim_epoch,c.claim_token,c.lease_until,o.owner_generation,o.owner_token,o.lease_until
                FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                WHERE c.operation_id=:id
                """).setParameter("id",command.operationId()).getSingleResult());
    }

    @Test void absentClaimIsNotRetirementProof() throws Exception {
        try (var c=context(POSTGRES);
             var resources=DocumentJournaledSessionsIT.resources(c.tx().withTimeouts(TIMEOUTS),1,1_000_000,LEASE,new PayloadBudget(128_000_000))) {
            var command=input(c).command();
            var key=new RepositoryOperationLedger.Key(command.intent().getAccountId(),CALLER.principalName(),command.operationId());
            // A supplied observation is not authoritative; no claim has been created in SQL.
            var observed=new RepositoryCoordinatorRecoveryDiscovery.Observation(RepositoryCoordinatorRecoveryDiscovery.Status.EXPIRED_BOUND,
                    Optional.of(new RepositoryCoordinatorRecoveryDiscovery.Candidate(
                            new RepositoryCoordinatorDrain.Identity(key,command.sha256(),1,UUID.randomUUID(),UUID.randomUUID()),
                            new RepositoryCoordinatorReservation.OwnerIdentity(1,UUID.randomUUID()))),Optional.empty());
            var budget=new PayloadBudget(128_000_000);
            try (var attempts=new RepositoryRecoveryAttempts(c.tx(),budget,resources.sessions(),LEASE,TIMEOUTS,1)) {
                try (var attempt=attempts.begin(CALLER,command,observed)) {
                    long held=budget.reservedBytes();
                    assertThat(count(c,"repository_execution_claims")).isZero();
                    assertThat(attempt.retireFenced(CALLER,CALLER,NONE)).isFalse();
                    assertThat(budget.reservedBytes()).isEqualTo(held).isPositive();
                    attempts.close();
                }
                assertThat(attempts.drain()).isEqualTo(new RepositoryRecoveryAttempts.Drain(0,1));
            }
        }
    }

    @ParameterizedTest @ValueSource(booleans={false,true})
    void changedRetryModesCannotInstallOrActivate(boolean installed) throws Exception {
        try (var c=context(POSTGRES)) {
            var source=source(c); var budget=new PayloadBudget(128_000_000);
            try (var resources=DocumentJournaledSessionsIT.resources(c.tx(),2,1_000_000,LEASE,new PayloadBudget(128_000_000));
                 var attempts=new RepositoryRecoveryAttempts(c.tx(),budget,resources.sessions(),LEASE,TIMEOUTS,1)) {
                var changed=new HashMap<String,DocumentPublicationCandidate.Mode>();
                MODES.forEach((member,mode) -> changed.put(member,mode==DocumentPublicationCandidate.Mode.TYPED
                        ? DocumentPublicationCandidate.Mode.OPAQUE : DocumentPublicationCandidate.Mode.TYPED));
                RepositoryCoordinatorReservation.Proposal proposal;
                try (var attempt=attempts.begin(CALLER,source.command(),source.observation())) {
                    proposal=attempt.proposal();
                    assertThat(attempt.advance(CALLER,CALLER,MODES,NONE)).isEqualTo(RepositoryRecoveryAttempts.Phase.RESERVED);
                    if (installed) assertThat(attempt.advance(CALLER,CALLER,MODES,NONE)).isEqualTo(RepositoryRecoveryAttempts.Phase.INSTALLED);
                    assertThatThrownBy(() -> attempt.advance(CALLER,CALLER,changed,NONE))
                            .isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.FAILED_PRECONDITION));
                    assertThat(count(c,"repository_successor_installs")).isEqualTo(installed ? 1 : 0);
                    assertThat(count(c,"repository_successor_executions")).isZero();
                    assertThat(attempt.proposal()).isSameAs(proposal);
                    assertThat(budget.reservedBytes()).isPositive();
                }
                try (var retry=attempts.resume(CALLER,source.command()).orElseThrow()) {
                    assertThat(retry.proposal()).isSameAs(proposal);
                    RepositoryRecoveryAttempts.Phase phase;
                    do { phase=retry.advance(CALLER,CALLER,MODES,NONE); } while (phase!=RepositoryRecoveryAttempts.Phase.ACTIVATED);
                    assertThatThrownBy(() -> retry.advance(CALLER,CALLER,changed,NONE))
                            .isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.FAILED_PRECONDITION));
                    assertThat(retry.advance(CALLER,CALLER,MODES,NONE)).isEqualTo(RepositoryRecoveryAttempts.Phase.ACTIVATED);
                    retry.close();
                    assertThatThrownBy(() -> retry.advance(CALLER,CALLER,MODES,NONE)).hasMessageContaining("closed");
                }
                attempts.close();
                assertThat(attempts.drain()).isEqualTo(new RepositoryRecoveryAttempts.Drain(0,0));
                assertThat(budget.reservedBytes()).isZero();
            }
        }
    }

    private static void expire(Context c, DocumentPublicationCommand command) {
        c.tx().readOnly(em -> em.createNativeQuery("""
                SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM
                 (GREATEST(c.lease_until,o.lease_until)-clock_timestamp())))+0.05)
                FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                WHERE c.operation_id=:id
                """).setParameter("id",command.operationId()).getSingleResult());
    }

    @ParameterizedTest @ValueSource(ints={0,1,2})
    void acceptedCallRetainsItsIdentityAcrossAdmissionClosure(int completedPhases) throws Exception {
        try (var c=context(POSTGRES)) {
            var source=source(c); var budget=new PayloadBudget(128_000_000);
            try (var resources=DocumentJournaledSessionsIT.resources(c.tx(),2,1_000_000,LEASE,budget);
                 var attempts=new RepositoryRecoveryAttempts(c.tx(),budget,resources.sessions(),LEASE,TIMEOUTS,1)) {
                assertThat(attempts.resume(CALLER,source.command())).isEmpty();
                try (var pressure=budget.reserve(budget.capacity()-1)) {
                    assertThatThrownBy(() -> attempts.begin(CALLER,source.command(),source.observation()))
                            .isInstanceOf(PayloadBudget.CapacityExceededException.class);
                    assertThat(count(c,"repository_coordinator_reservations")).isZero();
                }
                try (var attempt=attempts.begin(CALLER,source.command(),source.observation())) {
                    var changed=new DocumentPublicationCommand(source.command().intent().toBuilder().setMembers(0,
                            source.command().intent().getMembers(0).toBuilder().setMemberId("changed")).build());
                    assertThatThrownBy(() -> attempts.begin(CALLER,changed,source.observation())).hasMessageContaining("command changed");
                    assertThatThrownBy(() -> attempts.resume(CALLER,changed)).hasMessageContaining("command changed");
                    assertThatThrownBy(() -> attempts.resume(CALLER,source.command())).hasMessageContaining("in use");
                    var scoped=new RepositoryCaller(CALLER.principalName(),false,Set.of("account"),Set.of());
                    assertThatThrownBy(() -> attempts.resume(scoped,source.command())).hasMessageContaining("caller identity changed");
                    assertThatThrownBy(() -> attempt.advance(CALLER,CALLER,Map.of(),NONE)).isInstanceOf(IllegalArgumentException.class);
                    assertThat(count(c,"repository_coordinator_reservations")).isZero();
                    for (int i=0;i<completedPhases;i++) attempt.advance(CALLER,CALLER,MODES,NONE);
                    attempts.close();
                    assertThat(attempts.drain()).isEqualTo(new RepositoryRecoveryAttempts.Drain(1,1));
                    assertThatThrownBy(() -> attempts.begin(CALLER,source.command(),source.observation())).hasMessageContaining("admission is closed");
                    assertThatThrownBy(() -> attempts.resume(CALLER,source.command())).hasMessageContaining("admission is closed");
                    RepositoryRecoveryAttempts.Phase current;
                    do { current=attempt.advance(CALLER,CALLER,MODES,NONE); } while (current!=RepositoryRecoveryAttempts.Phase.ACTIVATED);
                    assertThat(attempts.drain()).isEqualTo(new RepositoryRecoveryAttempts.Drain(1,0));
                }
                assertThat(attempts.drain()).isEqualTo(new RepositoryRecoveryAttempts.Drain(0,0));
                assertThat(budget.reservedBytes()).isZero();
            }
        }
    }

    @ParameterizedTest @org.junit.jupiter.params.provider.CsvSource({
            "reservation,false", "installation,false", "activation,false",
            "reservation,true", "installation,true", "activation,true"})
    void committedPhaseWithCancelledReplyKeepsExactIdentity(String phase, boolean shutdown) throws Exception {
        try (var c=context(POSTGRES)) {
            var source=source(c); var armed=new AtomicBoolean(true); var cancelled=new AtomicBoolean();
            String table=switch (phase) {
                case "reservation" -> "repository_coordinator_reservations";
                case "installation" -> "repository_successor_installs";
                case "activation" -> "repository_successor_executions";
                default -> throw new IllegalArgumentException(phase);
            };
            var datasource=DocumentJdbcFaults.afterCommit(c.pool(),() -> {
                if (count(c,table)==1 && armed.compareAndSet(true,false)) cancelled.set(true);
            });
            var control=new RepositoryReadControl() {
                public boolean isCancelled() { return cancelled.get(); }
                public long remainingNanos() { return Long.MAX_VALUE; }
            };
            var budget=new PayloadBudget(128_000_000);
            try (var emf=jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",Map.of(
                    "hibernate.connection.datasource",datasource,"hibernate.hbm2ddl.auto","validate"));
                 var resources=DocumentJournaledSessionsIT.resources(new Tx(emf),2,1_000_000,LEASE,budget);
                 var attempts=new RepositoryRecoveryAttempts(new Tx(emf),budget,resources.sessions(),LEASE,TIMEOUTS,1)) {
                RepositoryCoordinatorReservation.Proposal proposal;
                try (var attempt=attempts.begin(CALLER,source.command(),source.observation())) {
                    proposal=attempt.proposal();
                    if (!phase.equals("reservation")) assertThat(attempt.advance(CALLER,CALLER,MODES,control)).isEqualTo(RepositoryRecoveryAttempts.Phase.RESERVED);
                    if (phase.equals("activation")) assertThat(attempt.advance(CALLER,CALLER,MODES,control)).isEqualTo(RepositoryRecoveryAttempts.Phase.INSTALLED);
                    assertThatThrownBy(() -> attempt.advance(CALLER,CALLER,MODES,control))
                            .isInstanceOfSatisfying(RepositoryException.class,e -> assertThat(e.code()).isEqualTo(RepositoryException.Code.CANCELLED));
                }
                assertThat(armed).isFalse(); assertThat(count(c,table)).isEqualTo(1);
                var identity=c.tx().readOnly(em -> em.createNativeQuery("SELECT successor_token FROM repository_coordinator_reservations").getSingleResult());
                Object ownerBefore=!phase.equals("reservation") ? c.tx().readOnly(em -> em.createNativeQuery("SELECT owner_nonce FROM repository_successor_installs").getSingleResult()) : null;
                if (shutdown) {
                    attempts.close();
                    assertThat(attempts.detachClosed(Duration.ZERO,key -> CALLER,NONE)).isTrue();
                    assertThat(attempts.drain()).isEqualTo(new RepositoryRecoveryAttempts.Drain(0,0));
                    assertThat(budget.reservedBytes()).isZero();
                    assertThat(count(c,"repository_coordinator_reservations")).isEqualTo(1);
                    assertThat(count(c,"repository_successor_installs")).isEqualTo(phase.equals("reservation") ? 0 : 1);
                    assertThat(count(c,"repository_successor_executions")).isEqualTo(phase.equals("activation") ? 1 : 0);
                    assertThat(c.tx().<Object>readOnly(em -> em.createNativeQuery("SELECT successor_token FROM repository_coordinator_reservations").getSingleResult())).isEqualTo(identity);
                    var progress=resources.sessions().drainRegistrations(Duration.ZERO,key -> CALLER,NONE);
                    assertThat(progress.unresolved()).isZero();
                    assertThat(resources.sessions().awaitIdle(Duration.ZERO)).isTrue();
                    assertThat(resources.sessions().attestLocalDrain(key -> CALLER,NONE)).isTrue();
                    return;
                }
                try (var retry=attempts.resume(CALLER,source.command()).orElseThrow()) {
                    assertThat(retry.proposal()).isSameAs(proposal);
                    assertThatThrownBy(() -> attempts.begin(CALLER,source.command(),source.observation())).hasMessageContaining("in use");
                    RepositoryRecoveryAttempts.Phase current;
                    do { current=retry.advance(CALLER,CALLER,MODES,NONE); } while (current!=RepositoryRecoveryAttempts.Phase.ACTIVATED);
                    attempts.close();
                    assertThat(attempts.drain()).isEqualTo(new RepositoryRecoveryAttempts.Drain(1,0));
                }
                assertThat(attempts.drain()).isEqualTo(new RepositoryRecoveryAttempts.Drain(0,0));
                assertThat(count(c,"repository_coordinator_reservations")).isEqualTo(1);
                assertThat(count(c,"repository_successor_installs")).isEqualTo(1);
                assertThat(count(c,"repository_successor_executions")).isEqualTo(1);
                assertThat(c.tx().<Object>readOnly(em -> em.createNativeQuery("SELECT successor_token FROM repository_coordinator_reservations").getSingleResult())).isEqualTo(identity);
                if (ownerBefore!=null) assertThat(c.tx().<Object>readOnly(em -> em.createNativeQuery("SELECT owner_nonce FROM repository_successor_installs").getSingleResult())).isEqualTo(ownerBefore);
                assertThat(budget.reservedBytes()).isZero();
            }
        }
    }
}
