package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/** Real retained SQL inputs after validation scope closes; no registry resolver is supplied. */
public final class AssessmentReplayInputsProbe {
    static void run(Tx tx, RepositoryOperationLedger.Owner owner, DocumentPublicationCommand command,
            Map<String,DocumentAssessmentRetainedSlots.UploadSelection> selections,
            DocumentAssessmentCreation.Created stage, UUID source, String policySha) throws Exception {
        var caller = new RepositoryCaller("principal", false, Set.of("account"), Set.of());
        for (int scenario = 0; scenario < 6; scenario++) {
            var reads = new DocumentReadLedger(tx, UUID.randomUUID(), 1);
            var budget = new PayloadBudget(scenario == 1 ? 1 : 128_000_000);
            var triggered = new AtomicBoolean();
            final int mode = scenario;
            try (var capture = reads.captureAssessment(caller, owner, command, selections, stage.assessment(),
                    stage.manifestSha256(), stage.retainUntil(), new PayloadBudget(64_000_000), () -> {});
                    var corruption = mode == 4 ? corruptLocator(tx, stage.assessment()) : null) {
                var control = new RepositoryReadControl() {
                    @Override public long remainingNanos() { return Long.MAX_VALUE; }
                    @Override public boolean isCancelled() {
                        if (mode == 2 && budget.reservedBytes() > 0) { triggered.set(true); return true; }
                        if (mode == 3 && budget.reservedBytes() > 0 && triggered.compareAndSet(false, true))
                            policy(tx, source, "ACCESS_DENY");
                        if (mode == 5 && budget.reservedBytes() > 0 && triggered.compareAndSet(false, true))
                            Thread.currentThread().interrupt();
                        return false;
                    }
                };
                if (mode == 0) {
                    DocumentAssessmentReplayInputs input;
                    try (var loaded = capture.loadReplayInputs(budget, control)) {
                        input = loaded;
                        var snapshot = loaded.snapshot();
                        require(snapshot.command().canonical().equals(command.canonical()), "exact captured command");
                        require(snapshot.policy().sha256().equals(policySha), "original policy loaded by digest");
                        require(snapshot.manifest().getCommandSha256().equals(command.sha256()), "original manifest");
                        require(!snapshot.roots().isEmpty() && !snapshot.artifacts().isEmpty(), "retained roots and schemas loaded");
                        require(budget.reservedBytes() > 0, "SQL input bytes remain reserved");
                        capture.close();
                        require(reads.releaseDrained(1) == 0, "input Use keeps assessment retained");
                        require(loaded.snapshot() == snapshot, "borrowed input remains usable through held Use");
                    }
                    try { input.snapshot(); throw new AssertionError("Closed input exposed retained bytes"); }
                    catch (IllegalStateException expected) { require(expected.getMessage().contains("closed"), "closed input refused"); }
                } else {
                    try (var unexpected = capture.loadReplayInputs(budget, control)) {
                        throw new AssertionError("Failed input load was delivered");
                    } catch (RepositoryException expected) {
                        var code = mode == 1 ? RepositoryException.Code.RESOURCE_EXHAUSTED
                                : mode == 2 || mode == 5 ? RepositoryException.Code.CANCELLED
                                : mode == 3 ? RepositoryException.Code.NOT_FOUND : RepositoryException.Code.DATA_LOSS;
                        require(expected.code() == code, "capacity, cancellation or source revocation refusal");
                        if (mode == 3) require(expected.getCause() == null && expected.getSuppressed().length == 0,
                                "source revocation exposes no private storage diagnostics");
                        if (mode == 4) require(expected.getMessage().contains("root locator"), "stored locator corruption refused");
                        if (mode == 5) require(Thread.currentThread().isInterrupted(), "interrupt flag preserved");
                    }
                    if (mode == 2 || mode == 3 || mode == 5) require(triggered.get(), "fault happened after byte reservation");
                    require(budget.reservedBytes() == 0, "failed loading releases all reservations");
                }
            } finally {
                if (mode == 5) Thread.interrupted(); // Fixture owns the injected interrupt; permit SQL cleanup.
                require(budget.reservedBytes() == 0, "input scope releases byte reservations");
                require(reads.releaseDrained(1) == 1 && reads.outstandingReads() == 0, "input Use releases exact SQL session");
                if (mode == 3) policy(tx, source, "ACCESS_READ");
            }
        }
        System.out.println("ASSESSMENT_REPLAY_INPUTS_OK");
    }
    private static AutoCloseable corruptLocator(Tx tx, UUID assessment) {
        Object[] original = tx.readOnly(em -> (Object[]) em.createNativeQuery("""
                SELECT member_id,revision_ordinal,root_locator_sha256 FROM document_assessment_roots
                WHERE assessment_id=:id ORDER BY member_id,revision_ordinal,root_locator_sha256 LIMIT 1
                """).setParameter("id", assessment).getSingleResult());
        byte[] bad = new byte[32];
        require(!java.util.Arrays.equals(bad, (byte[]) original[2]), "corruption changes locator");
        // Disposable PostgreSQL only: deliberately corrupt an immutable row after capture.
        // SET LOCAL ends with each transaction; restoration targets this exact row.
        replaceLocator(tx, assessment, original, (byte[]) original[2], bad);
        return () -> replaceLocator(tx, assessment, original, bad, (byte[]) original[2]);
    }
    private static void replaceLocator(Tx tx, UUID assessment, Object[] key, byte[] expected, byte[] replacement) {
        tx.inTransaction(em -> {
            em.createNativeQuery("SET LOCAL session_replication_role=replica").executeUpdate();
            int changed = em.createNativeQuery("""
                    UPDATE document_assessment_roots SET root_locator_sha256=:replacement
                    WHERE assessment_id=:id AND member_id=:member AND revision_ordinal=:ordinal AND root_locator_sha256=:expected
                    """).setParameter("replacement", replacement).setParameter("id", assessment).setParameter("member", key[0])
                    .setParameter("ordinal", key[1]).setParameter("expected", expected).executeUpdate();
            require(changed == 1, "exact corruption fixture row updated");
        });
    }
    private static void policy(Tx tx, UUID source, String access) {
        tx.inTransaction(em -> {
            int changed = em.createNativeQuery("UPDATE documents SET security=CAST(:policy AS jsonb) WHERE node_id=:node")
                    .setParameter("policy", "{\"permissions\":[{\"identityType\":\"public\",\"identity\":\"public\",\"access\":\"" + access + "\"}]}")
                    .setParameter("node", source).executeUpdate();
            require(changed == 1, "source policy updated");
        });
    }
    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
