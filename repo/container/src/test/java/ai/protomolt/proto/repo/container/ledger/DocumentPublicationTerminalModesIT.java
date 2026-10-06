package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static org.assertj.core.api.Assertions.*;

/** Real SQL journals and receipts; native revision fixture uses synthetic provider observations. */
@Testcontainers
class DocumentPublicationTerminalModesIT {
    @Container static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER = new RepositoryCaller("principal", true);
    private static final RepositoryReadControl NONE = RepositoryReadControl.NONE;
    private static final Map<String, DocumentPublicationCandidate.Mode> MODES = Map.of(
            "member-0", DocumentPublicationCandidate.Mode.TYPED, "member-1", DocumentPublicationCandidate.Mode.OPAQUE);
    private static final Map<String, DocumentPublicationCandidate.Mode> DIFFERENT = Map.of(
            "member-0", DocumentPublicationCandidate.Mode.OPAQUE, "member-1", DocumentPublicationCandidate.Mode.OPAQUE);

    @ParameterizedTest @ValueSource(booleans={false,true})
    void terminalReplaySurvivesExpiryChecksModesAndCurrentAccessWithoutRenewal(boolean rejected) {
        try (var c = context(POSTGRES)) {
            var p = journaled(c, Duration.ofSeconds(2));
            var replay = new DocumentPublicationReplay(c.tx());
            assertThat(replay.observe(CALLER, p.command(), MODES, NONE).state()).isEqualTo(DocumentPublicationReplay.State.PENDING);
            terminal(c, p, rejected);
            var expected = replay.observe(CALLER, p.command());
            expire(c, p);
            var before = owner(c, p);
            for (int i=0;i<2;i++) assertThat(replay.observe(CALLER, p.command(), MODES, NONE)).isEqualTo(expected);
            assertCode(() -> replay.observe(CALLER, p.command(), DIFFERENT, NONE), RepositoryException.Code.FAILED_PRECONDITION);
            var scoped = new RepositoryCaller("principal", false, Set.of("account"), Set.of());
            assertCode(() -> replay.observe(scoped, p.command(), MODES, NONE), RepositoryException.Code.NOT_FOUND);
            policy(c, "{\"permissions\":[{\"identityType\":\"public\",\"identity\":\"public\",\"access\":\"ACCESS_READ\"}]}");
            assertThat(replay.observe(scoped, p.command(), MODES, NONE)).isEqualTo(expected);
            policy(c, "{}");
            // Authorization precedes mode comparison, including a mismatched request.
            assertCode(() -> replay.observe(scoped, p.command(), DIFFERENT, NONE), RepositoryException.Code.NOT_FOUND);
            assertThat(owner(c, p)).containsExactly(before);
            assertThat(count(c,"repository_operation_success")).isEqualTo(rejected ? 0 : 1);
            assertThat(count(c,"repository_operation_rejection")).isEqualTo(rejected ? 1 : 0);
            assertThat(count(c,"document_events_outbox")).isEqualTo(rejected ? 0 : 2);
        }
    }

    @ParameterizedTest @ValueSource(strings={"missing","nonce","member","value","preparation-nonce"})
    void damagedModeJournalNeverReturnsReceipt(String fault) {
        try (var c = context(POSTGRES)) {
            var p=journaled(c, Duration.ofMinutes(1)); terminal(c,p,true);
            c.tx().inTransaction(em -> {
                em.createNativeQuery("ALTER TABLE repository_publication_modes DISABLE TRIGGER repository_publication_modes_guard").executeUpdate();
                em.createNativeQuery("ALTER TABLE repository_publication_preparations DISABLE TRIGGER repository_publication_preparation_guard").executeUpdate();
                String sql = switch (fault) {
                    case "missing" -> "DELETE FROM repository_publication_modes";
                    case "nonce" -> "UPDATE repository_publication_modes SET owner_nonce=gen_random_uuid()";
                    case "member" -> "UPDATE repository_publication_modes SET modes='{\"wrong\":\"TYPED\"}'::jsonb";
                    case "preparation-nonce" -> "UPDATE repository_publication_preparations SET owner_nonce=gen_random_uuid()";
                    default -> "UPDATE repository_publication_modes SET modes='{\"member-0\":\"INVALID\",\"member-1\":\"OPAQUE\"}'::jsonb";
                };
                em.createNativeQuery(sql).executeUpdate();
                em.createNativeQuery("ALTER TABLE repository_publication_modes ENABLE TRIGGER repository_publication_modes_guard").executeUpdate();
                em.createNativeQuery("ALTER TABLE repository_publication_preparations ENABLE TRIGGER repository_publication_preparation_guard").executeUpdate();
            });
            assertCode(() -> new DocumentPublicationReplay(c.tx()).observe(CALLER,p.command(),MODES,NONE), RepositoryException.Code.DATA_LOSS);
        }
    }

    @ParameterizedTest @org.junit.jupiter.params.provider.CsvSource({"false,false","false,true","true,false","true,true"})
    void terminalReplayRechecksRevokedOrRotatedCredential(boolean rejected, boolean rotated) {
        try (var c=context(POSTGRES)) {
            var p=journaled(c,Duration.ofMinutes(1)); terminal(c,p,rejected);
            policy(c,"{\"permissions\":[{\"identityType\":\"public\",\"identity\":\"public\",\"access\":\"ACCESS_READ\"}]}");
            var binding=new RepositoryCredentialBinding("terminal-mode-test",UUID.randomUUID(),1);
            var caller=new RepositoryCaller("principal",false,Set.of("account"),Set.of(),java.util.Optional.of(binding));
            var credentials=new RepositoryCredentialAuthorities(c.tx());
            var replay=new DocumentPublicationReplay(c.tx());
            assertCode(() -> replay.observe(caller,p.command(),MODES,NONE),RepositoryException.Code.UNAUTHENTICATED);
            credentials.register(CALLER,binding,caller.principalName());
            assertThat(replay.observe(caller,p.command(),MODES,NONE)).isEqualTo(replay.observe(CALLER,p.command()));
            var before=owner(c,p);
            if (rotated) credentials.rotate(CALLER,binding,caller.principalName());
            else credentials.revoke(CALLER,binding,caller.principalName());
            assertCode(() -> replay.observe(caller,p.command(),MODES,NONE),RepositoryException.Code.UNAUTHENTICATED);
            assertCode(() -> replay.observe(caller,p.command()),RepositoryException.Code.UNAUTHENTICATED);
            assertThat(owner(c,p)).containsExactly(before);
        }
    }

    @Test void unjournaledTerminalFailsClosedWhileLegacyReplayAndUnobservedRemainAvailable() {
        try (var c=context(POSTGRES)) {
            var p=prepare(c,2);
            var replay=new DocumentPublicationReplay(c.tx());
            assertThat(replay.observe(new RepositoryCaller("other",true),p.command(),MODES,NONE).state())
                    .isEqualTo(DocumentPublicationReplay.State.NOT_OBSERVED);
            var receipt=new DocumentPublicationRejections(c.tx()).cancel(CALLER,p.owner(),p.command(),NONE);
            assertThat(replay.observe(CALLER,p.command())).isEqualTo(receipt);
            assertCode(() -> replay.observe(CALLER,p.command(),MODES,NONE),RepositoryException.Code.DATA_LOSS);
            var cancelled=new RepositoryReadControl() {
                @Override public boolean isCancelled() { return true; }
                @Override public long remainingNanos() { return Long.MAX_VALUE; }
            };
            assertCode(() -> replay.observe(CALLER,p.command(),MODES,cancelled),RepositoryException.Code.CANCELLED);
        }
    }

    private static Prepared journaled(Context c, Duration lease) {
        var source=prepare(c,2);
        var command=new DocumentPublicationCommand(source.command().intent().toBuilder().setOperationId(UUID.randomUUID().toString()).build());
        var key=new RepositoryOperationLedger.Key("account","principal",command.operationId());
        var driveId=UUID.fromString(command.intent().getMembers(0).getDriveId());
        var placement=DocumentUploadPlan.Placement.sample(new DriveLedger(c.tx()).findById(driveId).orElseThrow(),"native-test",
                new ManagedBackendLedger(c.tx()).find("native-test").orElseThrow());
        var record=new DocumentPublicationPreparationRecord(key,command,DocumentPublicationSeeds.mint(key,command),Map.of(driveId,placement),lease,0);
        var budget=new PayloadBudget(32L*1024*1024);
        var claim=new RepositoryExecutionClaimLedger(c.tx()).acquire(key,command,UUID.randomUUID(),lease);
        new DocumentPublicationPreparationJournal(c.tx(),budget).save(CALLER,claim,record,NONE);
        new DocumentPublicationModesJournal(c.tx(),budget).bind(CALLER,claim,0,MODES,NONE);
        var owner=new RepositoryOperationLedger(c.tx()).admit(key,command,record.seeds().ownerNonce(),lease,claim).owner().orElseThrow();
        new DocumentOperationUploadAdmission(c.tx(),new DriveLedger(c.tx())).admit(CALLER,owner,
                DocumentOperationUploadAdmission.prepare(command,Map.of(driveId,placement),Map.of(),lease));
        assertThat(budget.reservedBytes()).isZero();
        return new Prepared(command,owner,source.sources(),Map.of());
    }
    private static void terminal(Context c, Prepared p, boolean rejected) {
        if (rejected) new DocumentPublicationRejections(c.tx()).cancel(CALLER,p.owner(),p.command(),NONE);
        else publish(c,p,Fault.NONE,em -> {});
    }
    private static void expire(Context c, Prepared p) {
        c.tx().readOnly(em -> em.createNativeQuery("SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM lease_until-clock_timestamp()))+0.02) FROM repository_operation_owners WHERE operation_id=:id")
                .setParameter("id",p.command().operationId()).getSingleResult());
    }
    private static Object[] owner(Context c, Prepared p) {
        return c.tx().readOnly(em -> (Object[])em.createNativeQuery("SELECT owner_generation,owner_token,lease_until,lease_until>clock_timestamp() FROM repository_operation_owners WHERE operation_id=:id")
                .setParameter("id",p.command().operationId()).getSingleResult());
    }
    private static void policy(Context c,String policy) {
        c.tx().inTransaction(em -> { em.createNativeQuery("UPDATE documents SET security=CAST(:policy AS jsonb)").setParameter("policy",policy).executeUpdate(); });
    }
    private static void assertCode(Runnable operation,RepositoryException.Code code) {
        assertThatThrownBy(operation::run).isInstanceOfSatisfying(RepositoryException.class,failure -> assertThat(failure.code()).isEqualTo(code));
    }
}
