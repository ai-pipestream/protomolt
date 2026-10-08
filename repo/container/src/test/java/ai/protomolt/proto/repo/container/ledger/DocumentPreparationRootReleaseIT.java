package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Persistence;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentCaptureAdmissionClosureIT.*;
import static org.assertj.core.api.Assertions.*;

/** Raw SQL atomicity qualification; no public release API or historical execution. */
@Testcontainers
class DocumentPreparationRootReleaseIT {
    @Container static final PostgreSQLContainer POSTGRES=new PostgreSQLContainer("postgres:18-alpine");
    private static final RepositoryCaller CALLER=new RepositoryCaller("principal",true);
    private static final RepositoryReadControl NONE=RepositoryReadControl.NONE;
    private static final Duration LEASE=Duration.ofMinutes(5);

    @Test void pendingAndUndrainedPreparationsCannotRelease() throws Exception {
        try (var c=context(POSTGRES); var rig=historicalInitial(c,LEASE)) {
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { insert(em,rig,""); }))
                    .hasStackTraceContaining("one exact terminal alternative");
            abandon(c,rig);
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { insert(em,rig,""); }))
                    .hasStackTraceContaining("has not drained exactly");
            assertLive(c,rig);
        }
    }

    @Test void receiptAndAllRootDeletionMustCommitTogether() throws Exception {
        try (var c=context(POSTGRES); var rig=historicalInitial(c,LEASE);
             var memory=rig.budget().reserve(32L*1024*1024)) {
            abandon(c,rig); drain(c,rig);
            var terminal=new DocumentPreparationTerminalEvidence(rig.record());
            var coverage=DocumentPreparationCaptureCoverage.prepare(rig.record(),NONE);
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { delete(em,rig); }))
                    .hasStackTraceContaining("require this transaction release");
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { insert(em,rig,""); }))
                    .hasStackTraceContaining("remove every target root atomically");
            assertLive(c,rig);
            assertThatThrownBy(() -> c.tx().inTransaction((java.util.function.Consumer<EntityManager>)em -> {
                insert(em,rig,""); delete(em,rig); throw new Abort();
            })).isInstanceOf(Abort.class);
            assertLive(c,rig);
            c.tx().inTransaction(em -> {
                terminal.lockAndRequire(em,CALLER,NONE);
                assertThat(coverage.lockAndRequireDrained(em,NONE)).isEqualTo(1);
                insert(em,rig,""); assertThat(delete(em,rig)).isEqualTo(1);
            });
            assertThat(count(c,rig,"repository_preparation_history_roots")).isZero();
            assertThat(count(c,rig,"repository_preparation_root_releases")).isEqualTo(1);
            assertThat(count(c,rig,"repository_preparation_history_sets")).isEqualTo(1);
            assertThat(count(c,rig,"repository_preparation_pin_batches")).isEqualTo(1);
            assertThat(count(c,rig,"repository_preparation_capture_drains")).isEqualTo(1);
            for (String mutation : new String[]{"DELETE FROM repository_preparation_root_releases",
                    "UPDATE repository_preparation_root_releases SET root_count=root_count"}) {
                assertThatThrownBy(() -> c.tx().inTransaction(em -> {
                    em.createNativeQuery(mutation+" WHERE operation_id=:o").setParameter("o",rig.record().command().operationId()).executeUpdate();
                })).hasStackTraceContaining("release is permanent");
            }
        }
    }

    @ParameterizedTest @ValueSource(strings={"roots", "captures", "token", "preparation"})
    void changedReceiptIdentityCannotRelease(String changed) throws Exception {
        try (var c=context(POSTGRES); var rig=historicalInitial(c,LEASE)) {
            abandon(c,rig); drain(c,rig);
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { insert(em,rig,changed); delete(em,rig); }))
                    .hasStackTraceContaining("Release");
            assertLive(c,rig);
        }
    }

    @Test void migrationPreservesExistingRootsAndRequiresExistingDrainEvidence() throws Exception {
        var legacy=legacyReaderContext();
        try (var c=legacy.context(); var rig=historicalInitial(c,LEASE)) {
            abandon(c,rig);
            assertThat(count(c,rig,"repository_preparation_history_roots")).isEqualTo(1);
            legacy.disable();
            org.flywaydb.core.Flyway.configure().dataSource(c.pool()).schemas(c.pool().getSchema())
                    .defaultSchema(c.pool().getSchema()).locations("classpath:db/migration/repo").load().migrate();
            assertLive(c,rig);
            assertThat(count(c,rig,"repository_preparation_capture_drains")).isZero();
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { insert(em,rig,""); }))
                    .hasStackTraceContaining("has not drained exactly");
            drain(c,rig);
            c.tx().inTransaction(em -> { insert(em,rig,""); assertThat(delete(em,rig)).isEqualTo(1); });
            assertThat(count(c,rig,"repository_preparation_root_releases")).isEqualTo(1);
        }
    }

    /** V110 has no host binding column; only this fixture's unbound reader registration uses its old SQL shape. */
    private static LegacyReaderContext legacyReaderContext() {
        var base=context(POSTGRES,"110");
        var enabled=new AtomicBoolean(true);
        try {
            var dataSource=legacyReaderRegistration(base.pool(),enabled);
            var emf=Persistence.createEntityManagerFactory("document-ledger",Map.of(
                    "hibernate.connection.datasource",dataSource,"hibernate.hbm2ddl.auto","validate"));
            base.emf().close();
            return new LegacyReaderContext(new Context(base.pool(),emf,new Tx(emf),false,false),enabled);
        } catch (RuntimeException|Error failure) { base.close(); throw failure; }
    }

    private record LegacyReaderContext(Context context,AtomicBoolean enabled) {
        void disable() { enabled.set(false); }
    }

    private static DataSource legacyReaderRegistration(DataSource delegate,AtomicBoolean enabled) {
        return (DataSource)Proxy.newProxyInstance(DataSource.class.getClassLoader(),new Class<?>[]{DataSource.class},
                (proxy,method,args) -> {
                    var result=invoke(delegate,method,args);
                    if (!method.getName().equals("getConnection")) return result;
                    var connection=(Connection)result;
                    return Proxy.newProxyInstance(Connection.class.getClassLoader(),new Class<?>[]{Connection.class},
                            (connectionProxy,operation,parameters) -> {
                                if (!enabled.get() || !operation.getName().equals("prepareStatement")
                                        || parameters==null || parameters.length==0 || !(parameters[0] instanceof String sql))
                                    return invoke(connection,operation,parameters);
                                var normalized=sql.replaceAll("\\s+","").toLowerCase(Locale.ROOT);
                                if (!normalized.contains("repository_reader_incarnations") || !normalized.contains("host_execution"))
                                    return invoke(connection,operation,parameters);
                                if (!normalized.equals("insertintorepository_reader_incarnations(incarnation,state,registration_nonce,host_execution)"
                                        +"values(?,'active',?,cast(?asuuid))"))
                                    throw new SQLException("Unexpected V110 reader registration statement");
                                var oldParameters=parameters.clone();
                                oldParameters[0]="INSERT INTO repository_reader_incarnations(incarnation,state,registration_nonce) "
                                        +"VALUES(?,'ACTIVE',?)";
                                var statement=(PreparedStatement)invoke(connection,operation,oldParameters);
                                return Proxy.newProxyInstance(PreparedStatement.class.getClassLoader(),
                                        new Class<?>[]{PreparedStatement.class},(statementProxy,call,values) -> {
                                            if (call.getName().startsWith("set") && values!=null && values.length>0
                                                    && values[0] instanceof Integer index && index==3) {
                                                if (call.getName().equals("setNull")
                                                        || values.length>1 && values[1]==null) return null;
                                                throw new SQLException("V110 fixture cannot register a host-bound reader");
                                            }
                                            return invoke(statement,call,values);
                                        });
                            });
                });
    }

    private static Object invoke(Object target,java.lang.reflect.Method method,Object[] args) throws Throwable {
        try { return method.invoke(target,args); }
        catch (InvocationTargetException failure) { throw failure.getCause(); }
    }

    @Test void canonicalCancellationBindsExactTerminalReceipt() throws Exception {
        try (var c=context(POSTGRES); var rig=historicalInitial(c,LEASE)) {
            var command=rig.record().command();
            try (var work=rig.sources().work()) {
                var admission=RepositoryOperationLedger.prepareHistoricalAdmission(rig.record().key(),command,
                        rig.record().seeds().ownerNonce(),LEASE,work);
                var owner=c.tx().inTransaction(em -> {
                    RepositoryExecutionClaimLedger.lockLive(em,rig.claim());
                    DocumentPublicationModesJournal.insert(em,rig.claim(),rig.record(),DocumentPublicationModesJournal.encode(command,
                            java.util.Map.of(command.intent().getMembers(0).getMemberId(),DocumentPublicationCandidate.Mode.TYPED)));
                    return admission.apply(em,rig.claim()).owner().orElseThrow();
                });
                new DocumentPublicationRejections(c.tx()).cancel(CALLER,owner,command,NONE).rejection().orElseThrow();
            }
            drain(c,rig);
            assertThatThrownBy(() -> c.tx().inTransaction(em -> { insertRejection(em,rig,true); }))
                    .hasStackTraceContaining("Release rejection differs");
            assertLive(c,rig);
            var released=DocumentPreparationRootReleases.release(c.tx(),rig.budget(),
                    CALLER,rig.record(),NONE);
            assertThat(released.terminal()).isInstanceOfSatisfying(DocumentPreparationTerminalEvidence.Outcome.class,
                    outcome -> assertThat(outcome.kind()).isEqualTo("REJECTION"));
            assertThat(count(c,rig,"repository_preparation_root_releases")).isEqualTo(1);
            assertThat(count(c,rig,"repository_preparation_history_roots")).isZero();
        }
    }

    static void insertRejection(EntityManager em,Rig rig,boolean changed) {
        assertThat(em.createNativeQuery("""
                INSERT INTO repository_preparation_root_releases(account_id,principal,operation_id,predecessor_generation,
                 preparation_sha256,command_sha256,root_count,roots_sha256,capture_count,captures_sha256,
                 terminal_kind,terminal_generation,terminal_sha256,terminal_xid)
                SELECT h.account_id,h.principal,h.operation_id,h.predecessor_generation,h.preparation_sha256,
                 h.command_sha256,h.expected_count,h.roots_sha256,1,
                 repository_preparation_capture_fingerprint(h.account_id,h.principal,h.operation_id,h.predecessor_generation),
                 'REJECTION',r.owner_generation,
                """+(changed?"sha256('wrong'::bytea)":"r.result_sha256")+",r.creation_xid "+"""
                FROM repository_preparation_history_sets h JOIN repository_operation_rejection r USING(account_id,principal,operation_id)
                WHERE h.operation_id=:o AND h.predecessor_generation=0
                """).setParameter("o",rig.record().command().operationId()).executeUpdate()).isEqualTo(1);
    }

    static void abandon(Context c,Rig rig) {
        DocumentPublicationAbandonment.abandon(c.tx(),rig.budget(),CALLER,rig.claim(),rig.record(),NONE);
    }
    static void drain(Context c,Rig rig) {
        rig.sources().close(); rig.history().close(); rig.history().release();
        rig.reads().fence(); rig.reads().attestLocalQuiescence();
        var digest=c.tx().readOnly(em -> (byte[])em.createNativeQuery(
                "SELECT pins_sha256 FROM repository_preparation_pin_batches WHERE operation_id=:o")
                .setParameter("o",rig.record().command().operationId()).getSingleResult());
        var identity=new DocumentPreparationCaptureDrain.Identity(new RepositoryCoordinatorDrain.Identity(
                rig.record().key(),rig.record().command().sha256(),rig.claim().epoch(),rig.claim().token(),rig.coordinator()),
                0,HexFormat.of().formatHex(digest));
        assertThat(DocumentPreparationCaptureDrain.recover(c.tx(),CALLER,identity,NONE).kind()).isEqualTo("QUIESCED");
    }
    static void insert(EntityManager em,Rig rig,String changed) {
        String preparation=changed.equals("preparation")?"sha256('wrong'::bytea)":"h.preparation_sha256";
        String roots=changed.equals("roots")?"sha256('wrong'::bytea)":"h.roots_sha256";
        String captures=changed.equals("captures")?"sha256('wrong'::bytea)":
                "repository_preparation_capture_fingerprint(h.account_id,h.principal,h.operation_id,h.predecessor_generation)";
        String token=changed.equals("token")?"gen_random_uuid()":"CAST(:token AS uuid)";
        var q=em.createNativeQuery("""
                INSERT INTO repository_preparation_root_releases(account_id,principal,operation_id,predecessor_generation,
                 preparation_sha256,command_sha256,root_count,roots_sha256,capture_count,captures_sha256,
                 terminal_kind,abandonment_token,abandonment_nonce)
                SELECT h.account_id,h.principal,h.operation_id,h.predecessor_generation,
                """+preparation+",h.command_sha256,h.expected_count,"+roots+",1,"+captures+",'ABANDONMENT',"+token+",CAST(:nonce AS uuid) "+"""
                FROM repository_preparation_history_sets h WHERE h.operation_id=:o AND h.predecessor_generation=0
                """).setParameter("o",rig.record().command().operationId()).setParameter("nonce",rig.record().seeds().ownerNonce());
        if (!changed.equals("token")) q.setParameter("token",rig.claim().token());
        assertThat(q.executeUpdate()).isEqualTo(1);
    }
    static int delete(EntityManager em,Rig rig) {
        return em.createNativeQuery("DELETE FROM repository_preparation_history_roots WHERE operation_id=:o AND predecessor_generation=0")
                .setParameter("o",rig.record().command().operationId()).executeUpdate();
    }
    static long count(Context c,Rig rig,String table) {
        return c.tx().readOnly(em -> ((Number)em.createNativeQuery("SELECT count(*) FROM "+table+" WHERE operation_id=:o")
                .setParameter("o",rig.record().command().operationId()).getSingleResult()).longValue());
    }
    static void assertLive(Context c,Rig rig) {
        assertThat(count(c,rig,"repository_preparation_history_roots")).isEqualTo(1);
        assertThat(count(c,rig,"repository_preparation_root_releases")).isZero();
    }
    private static final class Abort extends RuntimeException {}
}
