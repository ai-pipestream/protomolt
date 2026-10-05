package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import java.lang.reflect.*;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;

/** Lose the acknowledgement after a genuine PostgreSQL assessment commit, then discover the original coordinates. */
public final class AssessmentStageFaultProbe {
    static DocumentAssessmentCreation.Created create(DataSource database, Tx observer, RepositoryCaller caller,
            RepositoryOperationLedger.Owner owner, DocumentOperationUploadAdmission.Prepared plan,
            Map<String, DocumentSelectedAttemptLedger.Selected> selections, DocumentAssessmentEvidence evidence,
            UUID id, Instant deadline, PayloadBudget budget) {
        var armed = new AtomicBoolean();
        var source = (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(), new Class<?>[]{DataSource.class},
                (proxy, method, args) -> {
                    var result = invoke(database, method, args);
                    if (!method.getName().equals("getConnection")) return result;
                    var connection = (Connection) result;
                    return Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
                            (wrapper, action, arguments) -> {
                                var returned = invoke(connection, action, arguments);
                                if (action.getName().equals("commit") && armed.compareAndSet(true, false))
                                    throw new SQLException("Injected lost assessment stage acknowledgement", "08006");
                                return returned;
                            });
                });
        try (var emf = jakarta.persistence.Persistence.createEntityManagerFactory("document-ledger",
                Map.of("hibernate.connection.datasource", source, "hibernate.hbm2ddl.auto", "validate"))) {
            var faultTx = new Tx(emf);
            armed.set(true);
            try {
                new DocumentAssessmentCreation(faultTx, new DriveLedger(faultTx)).create(caller, owner, plan,
                        selections, evidence, id, deadline, budget, () -> {});
                throw new AssertionError("Lost stage acknowledgement returned success");
            } catch (RuntimeException failure) {
                boolean lost = false;
                for (Throwable cause = failure; cause != null; cause = cause.getCause())
                    if (cause instanceof SQLException sql && "08006".equals(sql.getSQLState())) lost = true;
                require(lost && !armed.get(), "failure followed the real database commit");
            }
        }
        // No id/hash/deadline or original evidence is supplied to discovery.
        var original = new DocumentAssessmentDiscovery(observer).discover(caller, owner, plan.plan().command(), () -> {}).orElseThrow();
        require(original.stage().assessment().equals(id) && original.stage().retainUntil().equals(deadline)
                && original.stage().manifestSha256().equals(evidence.manifestSha256(() -> {})), "original committed stage recovered");
        require(original.selections().equals(DocumentAssessmentRetainedSlots.uploadSelections(selections)),
                "original immutable attempt selection recovered");
        var exact = new DocumentAssessmentReconciliation(observer).observeRetained(caller, owner, plan.plan().command(),
                original.selections(), original.stage().assessment(), original.stage().manifestSha256(),
                original.stage().retainUntil(), budget, () -> {}).orElseThrow();
        require(exact.equals(original.stage()), "discovered coordinates pass full retained verification");
        System.out.println("ASSESSMENT_STAGE_LOST_ACK_DISCOVERY_OK");
        return exact;
    }
    private static Object invoke(Object target, Method method, Object[] arguments) throws Throwable {
        try { return method.invoke(target, arguments); }
        catch (InvocationTargetException failure) { throw failure.getCause(); }
    }
    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
