package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.codec.PartObject;
import ai.protomolt.proto.repo.spi.*;
import com.google.protobuf.ByteString;
import java.time.Duration;
import java.util.*;

/** Local proposal disposal after an already accepted publication wins the SQL race. */
final class HistoricalPublicationLosingSuccessorProbe implements AutoCloseable {
    private final Tx tx;
    private final RepositoryInstalledHistoricalAttempts attempts;
    private final RepositoryCaller caller;
    private final RepositoryCaller coordinator;
    private final DocumentPublicationCommand command;
    private final UUID oldId;
    private final Map<Integer, ByteString> fragments;
    private final DocumentHistoricalAssessmentSources.Work worker;
    private UUID nextId;

    HistoricalPublicationLosingSuccessorProbe(Tx tx, RepositoryInstalledHistoricalAttempts attempts,
            RepositoryCaller caller, RepositoryCaller coordinator, DocumentPublicationCommand command,
            UUID oldId, Map<Integer, ByteString> fragments, DocumentHistoricalAssessmentSources.Work worker) {
        this.tx = tx; this.attempts = attempts; this.caller = caller; this.coordinator = coordinator;
        this.command = command; this.oldId = oldId; this.fragments = fragments; this.worker = worker;
    }

    void selectWhilePublicationWaits() {
        var timeouts = new SqlTimeouts(Duration.ofSeconds(1), Duration.ofSeconds(5));
        var key = new RepositoryOperationLedger.Key(command.intent().getAccountId(), caller.principalName(), command.operationId());
        var observed = new RepositoryCoordinatorRecoveryDiscovery(tx, timeouts)
                .inspect(coordinator, key, command.sha256(), RepositoryReadControl.NONE);
        require(observed.status() == RepositoryCoordinatorRecoveryDiscovery.Status.EXPIRED_BOUND,
                "local proposal observes expired unpublished operation");
        try (var next = attempts.beginSuccessor(coordinator, caller, oldId, command,
                Map.of("a", DocumentPublicationCandidate.Mode.TYPED), observed, Duration.ofMinutes(2), timeouts)) {
            nextId = next.identity();
            require(!nextId.equals(oldId), "pending proposal has independent identity");
        }
        require(attempts.drain().equals(new RepositoryInstalledHistoricalAttempts.Drain(1, 2)),
                "old publication borrowed while pending proposal stays owned");
    }

    void verifyAndRetire(boolean oldFirst) throws Exception {
        var bodies = new HashMap<DocumentUploadPayloads.Key, PartObject>();
        for (var member : command.intent().getMembersList()) {
            for (int ordinal = 0; ordinal < member.getPartsCount(); ordinal++) {
                var part = member.getParts(ordinal);
                if (part.hasUpload()) bodies.put(new DocumentUploadPayloads.Key(member.getMemberId(), ordinal),
                        new PartObject(part.getSlot().getPart(), part.getSlot().getSubKey(),
                                Objects.requireNonNull(fragments.get(ordinal)).toByteArray(), part.getUpload().getSha256()));
            }
        }
        try (var next = attempts.resume(caller, command).orElseThrow()) {
            require(next.identity().equals(nextId), "retry still selects losing proposal");
            try {
                next.advancePreparation(coordinator, Map.of("a", DocumentPublicationCandidate.Mode.TYPED), bodies,
                        RepositoryReadControl.NONE);
                throw new AssertionError("Local successor replaced terminal publication");
            } catch (RuntimeException refused) {
                boolean terminal = false;
                for (Throwable cause = refused; cause != null; cause = cause.getCause())
                    if (cause.getMessage() != null && cause.getMessage().contains("Terminal operation cannot reserve expiration")) terminal = true;
                require(terminal, "local proposal reports terminal SQL refusal: " + refused);
            }
        }
        require(count("repository_coordinator_expirations") == 1 && count("repository_successor_installs") == 1,
                "losing proposal adds no reservation or installation");
        require(count("repository_historical_activations") == 1, "losing proposal creates no activation");
        long drains = count("repository_preparation_capture_drains");
        try (var old = attempts.resumeGeneration(coordinator, caller, command, oldId).orElseThrow()) {
            try {
                old.openExecution(coordinator, RepositoryReadControl.NONE);
                throw new AssertionError("Superseded local publisher accepted a new mutation");
            } catch (RepositoryException refused) {
                require(refused.code() == RepositoryException.Code.CONFLICT
                        && refused.getMessage().contains("disposal only"), "old generation admits disposal only");
            }
        }
        if (!oldFirst) retireNext();
        try (var old = attempts.resumeGeneration(coordinator, caller, command, oldId).orElseThrow()) {
            require(old.identity().equals(oldId), "old generation remains independently addressable");
            require(old.retireTerminal(coordinator, Duration.ZERO, RepositoryReadControl.NONE)
                    == RepositoryInstalledHistoricalAttempts.Retirement.RETAINED, "held worker prevents old retirement");
        }
        require(count("repository_preparation_capture_drains") == drains, "held worker prevents capture-drain attestation");
        require(attempts.drain().equals(new RepositoryInstalledHistoricalAttempts.Drain(0, oldFirst ? 2 : 1)),
                "held old generation remains owned");
        worker.close();
        try (var old = attempts.resumeGeneration(coordinator, caller, command, oldId).orElseThrow()) {
            require(old.retireTerminal(coordinator, Duration.ofSeconds(1), RepositoryReadControl.NONE)
                    == RepositoryInstalledHistoricalAttempts.Retirement.RETIRED, "old generation retires after worker drains");
        }
        require(attempts.resumeGeneration(coordinator, caller, command, oldId).isEmpty(), "old ID removed");
        require(count("repository_preparation_capture_drains") == drains + 1, "only drained publisher capture attested");
        if (oldFirst) {
            try (var next = attempts.resume(caller, command).orElseThrow()) {
                require(next.identity().equals(nextId), "old retirement preserves pending successor route");
            }
            retireNext();
        }
        require(count("repository_preparation_capture_drains") == drains + 1, "uninstalled proposal attests no capture");
        System.out.println(oldFirst ? "HISTORICAL_LOSER_OLD_FIRST_OK" : "HISTORICAL_LOSER_NEW_FIRST_OK");
        require(attempts.drain().equals(new RepositoryInstalledHistoricalAttempts.Drain(0, 0)), "both generations retired");
        System.out.println("HISTORICAL_LOSING_LOCAL_SUCCESSOR_RETIRED_OK");
    }

    private void retireNext() throws Exception {
        try (var next = attempts.resume(caller, command).orElseThrow()) {
            require(next.identity().equals(nextId), "selected route belongs to losing proposal");
            require(next.retireTerminal(coordinator, Duration.ofSeconds(1), RepositoryReadControl.NONE)
                    == RepositoryInstalledHistoricalAttempts.Retirement.RETIRED, "uninstalled losing proposal retires");
        }
        require(attempts.resume(caller, command).isEmpty(), "retiring proposal removes ordinary retry route");
        require(attempts.resumeGeneration(coordinator, caller, command, nextId).isEmpty(), "losing ID removed");
    }

    private long count(String table) {
        return tx.readOnly(em -> ((Number) em.createNativeQuery("SELECT count(*) FROM " + table + " WHERE operation_id=:op")
                .setParameter("op", command.operationId()).getSingleResult()).longValue());
    }
    @Override public void close() { worker.close(); }
    private static void require(boolean value, String message) { if (!value) throw new AssertionError(message); }
}
