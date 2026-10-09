package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.v1.DocumentPublicationIntent;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * A journaled publication whose coordinator never completed: the initial claim (V118
 * certificate, zero historical roots), two graceful coordinator handoffs that installed
 * unactivated successors (V93/V119), the first successor verified by lineage, the second
 * left unresolved. Every step is the production journal, coordinator and reconciliation
 * code; nothing here fabricates a certificate or a proof. The real leases expire naturally.
 */
final class RepositoryBackupRehearsalPendingOperation {
    static final Duration LEASE = Duration.ofSeconds(2);
    static final SqlTimeouts TIMEOUTS = new SqlTimeouts(Duration.ofSeconds(2), Duration.ofSeconds(10));
    static final Map<String, DocumentPublicationCandidate.Mode> MODES = Map.of("document", DocumentPublicationCandidate.Mode.TYPED);
    private static final RepositoryReadControl NONE = RepositoryReadControl.NONE;

    private RepositoryBackupRehearsalPendingOperation() {}

    /** Seed the chain against the live source ledger and record every identity it produced. */
    static Map<String, Object> seed(Tx tx, RepositoryBackupRehearsalChecks checks, RepositoryCaller caller, String generation,
            DocumentPublicationIntent intent) {
        var command = new DocumentPublicationCommand(intent);
        var key = new RepositoryOperationLedger.Key(intent.getAccountId(), caller.principalName(), command.operationId());
        var drives = new DriveLedger(tx);
        UUID driveId = UUID.fromString(intent.getMembers(0).getDriveId());
        var drive = drives.findById(driveId).orElseThrow(() -> new RepositoryBackupRehearsalFailure("Pending operation drive is missing"));
        var profile = new ManagedBackendLedger(tx).find(generation).orElseThrow(() -> new RepositoryBackupRehearsalFailure("Backend profile is missing"));
        var placements = Map.of(driveId, DocumentUploadPlan.Placement.sample(drive, generation, profile));
        var budget = new PayloadBudget(64_000_000);
        var previous = new DocumentPublicationPreparationRecord(key, command, DocumentPublicationSeeds.mint(key, command), placements, LEASE, 0);
        var incarnation = UUID.randomUUID();
        var claim = new DocumentPublicationPreparationJournal(tx, budget).acquireInitial(caller, previous, UUID.randomUUID(), incarnation, NONE);
        new DocumentPublicationModesJournal(tx, budget).bind(caller, claim, 0, MODES, NONE);
        new RepositoryOperationLedger(tx).admit(previous.key(), previous.command(), previous.seeds().ownerNonce(), LEASE, claim);
        RepositoryCoordinatorDrain.begin(tx, caller, claim, incarnation, NONE);
        var identity = new RepositoryCoordinatorDrain.Identity(claim.key(), claim.commandSha256(), claim.epoch(), claim.token(), incarnation);
        RepositoryCoordinatorLocalDrain.record(tx, caller, identity, NONE);
        var initialState = coverage(tx, key.operationId());
        checks.require(generations(initialState, "repository_preparation_coverage_certificates").equals(List.of(0L))
                && generations(initialState, "repository_preparation_coverage_unresolved").isEmpty(),
                "seed.pending.initial_certified", "journal preparation generation 0 certified atomically (V118), unresolved cleared");
        awaitLeaseExpiry(tx, key.operationId());
        var handoff = new RepositoryCoordinatorHandoff.Proposal(identity, UUID.randomUUID(), UUID.randomUUID(), LEASE);
        RepositoryCoordinatorHandoff.reserve(tx, caller, handoff, NONE);
        var first = RepositorySuccessorInstall.prepare(handoff, previous, LEASE, MODES);
        RepositorySuccessorInstall.install(tx, budget, caller, first, NONE);
        var reconciliation = new DocumentPreparationCoverageReconciliation(tx, budget, TIMEOUTS);
        var verified = reconciliation.reconcile(caller, first.next().key(), 1, NONE);
        checks.require(verified == DocumentPreparationCoverageReconciliation.Result.VERIFIED_SUCCESSOR, "seed.pending.first_successor_lineage",
                "generation 1 install edge verified by V119 lineage: " + verified);
        awaitLeaseExpiry(tx, key.operationId());
        var observed = new RepositoryCoordinatorRecoveryDiscovery(tx, TIMEOUTS).inspect(caller, key, command.sha256(), NONE);
        var unactivated = observed.unactivated().orElseThrow(() -> new RepositoryBackupRehearsalFailure("First successor is not unactivated: " + observed));
        var reservation = new RepositoryCoordinatorReservation.SupersededUnactivated(unactivated.predecessor(), UUID.randomUUID(), UUID.randomUUID(),
                LEASE, unactivated.owner(), unactivated.preparationSha256(), unactivated.installation());
        RepositoryCoordinatorSupersession.reserve(tx, caller, reservation, NONE);
        var second = RepositorySuccessorInstall.prepare(reservation, first.next(), LEASE, first.modes());
        RepositorySuccessorInstall.install(tx, budget, caller, second, NONE);
        var state = coverage(tx, key.operationId());
        checks.require(generations(state, "repository_publication_preparations").equals(List.of(0L, 1L, 2L))
                && generations(state, "repository_preparation_coverage_certificates").equals(List.of(0L))
                && generations(state, "repository_preparation_coverage_lineage").equals(List.of(1L))
                && generations(state, "repository_preparation_coverage_unresolved").equals(List.of(2L))
                && generations(state, "repository_successor_installs").equals(List.of(1L, 2L))
                && generations(state, "repository_successor_executions").isEmpty()
                && generations(state, "repository_preparation_history_sets").equals(List.of(0L))
                && generations(state, "repository_operation_success").isEmpty(),
                "seed.pending.chain", "certificate@0, lineage@1, unresolved@2, installs@1,2, no execution, no success: " + summary(state));
        checks.require(budget.reservedBytes() == 0, "seed.pending.budget_released", "preparation reservations released");
        var replay = new DocumentPublicationReplay(tx).observe(caller, command);
        checks.require(replay.state() == DocumentPublicationReplay.State.PENDING, "seed.pending.replay_pending", "exact replay observes " + replay.state());
        var record = new LinkedHashMap<String, Object>();
        record.put("operationId", key.operationId().toString());
        record.put("principal", key.principal());
        record.put("account", key.account());
        record.put("commandSha256", command.sha256());
        record.put("coverage", state);
        return record;
    }

    /** Pending and recoverable, not executable: the restored chain must match, then advance only through verification and install. */
    static void verifyRestored(Tx tx, RepositoryBackupRehearsalChecks checks, RepositoryCaller caller, Map<String, Object> record,
            DocumentPublicationIntent intent, String phase) {
        var command = new DocumentPublicationCommand(intent);
        var key = new RepositoryOperationLedger.Key(RepositoryBackupRehearsalJson.string(record, "account"),
                RepositoryBackupRehearsalJson.string(record, "principal"), command.operationId());
        checks.require(command.sha256().equals(RepositoryBackupRehearsalJson.string(record, "commandSha256")), phase + ".pending.command_identity",
                "recorded command digest reproduced from the persisted intent");
        var restored = coverage(tx, key.operationId());
        var recorded = RepositoryBackupRehearsalJson.object(record, "coverage");
        checks.require(RepositoryBackupRehearsalJson.canonical(restored).equals(RepositoryBackupRehearsalJson.canonical(recorded)), phase + ".pending.chain_identity",
                "V118/V119 rows, install edges (including install_xid) and generations equal the seed record: " + summary(restored));
        var replay = new DocumentPublicationReplay(tx).observe(caller, command);
        checks.require(replay.state() == DocumentPublicationReplay.State.PENDING, phase + ".pending.replay_pending",
                "exact replay still observes " + replay.state() + "; no result was invented");
        var discovery = new RepositoryCoordinatorRecoveryDiscovery(tx, TIMEOUTS);
        var observed = discovery.inspect(caller, key, command.sha256(), NONE);
        checks.require(observed.status() == RepositoryCoordinatorRecoveryDiscovery.Status.INSTALLED_NOT_ACTIVATED && observed.unactivated().isPresent(),
                phase + ".pending.discovery", "recovery discovery observes " + observed.status() + " with the installed successor; leases restored verbatim and expired");
        var budget = new PayloadBudget(64_000_000);
        var reconciliation = new DocumentPreparationCoverageReconciliation(tx, budget, TIMEOUTS);
        var verified = reconciliation.reconcile(caller, key, 2, NONE);
        var afterProof = coverage(tx, key.operationId());
        checks.require(verified == DocumentPreparationCoverageReconciliation.Result.VERIFIED_SUCCESSOR
                && generations(afterProof, "repository_preparation_coverage_lineage").equals(List.of(1L, 2L))
                && generations(afterProof, "repository_preparation_coverage_unresolved").isEmpty()
                && generations(afterProof, "repository_preparation_history_sets").equals(List.of(0L))
                && generations(afterProof, "repository_successor_executions").isEmpty(),
                phase + ".pending.lineage_verifiable", "generation 2 verified against the restored install edge (" + verified
                        + "): depth 2, no header, no execution; a new transaction never equals a restored install_xid");
        var unactivated = observed.unactivated().orElseThrow();
        var reservation = new RepositoryCoordinatorReservation.SupersededUnactivated(unactivated.predecessor(), UUID.randomUUID(), UUID.randomUUID(),
                LEASE, unactivated.owner(), unactivated.preparationSha256(), unactivated.installation());
        RepositoryCoordinatorSupersession.reserve(tx, caller, reservation, NONE);
        // The expired claim cannot be loaded as live; the reserved-predecessor loader is the production path.
        try (var reserved = new RepositoryReservedPreparation(tx, budget, TIMEOUTS).load(caller, caller, reservation, unactivated.owner(), NONE)) {
            var third = RepositorySuccessorInstall.prepare(reservation, reserved.record(), LEASE, reserved.modes());
            RepositorySuccessorInstall.install(tx, budget, caller, third, NONE);
        }
        var afterInstall = coverage(tx, key.operationId());
        var replayAfter = new DocumentPublicationReplay(tx).observe(caller, command);
        checks.require(generations(afterInstall, "repository_successor_installs").equals(List.of(1L, 2L, 3L))
                && generations(afterInstall, "repository_preparation_coverage_unresolved").equals(List.of(3L))
                && generations(afterInstall, "repository_successor_executions").isEmpty()
                && generations(afterInstall, "repository_operation_success").isEmpty()
                && replayAfter.state() == DocumentPublicationReplay.State.PENDING,
                phase + ".pending.recoverable_not_executed", "a third successor installed on the restored catalog; still unresolved, unexecuted and PENDING");
        checks.require(budget.reservedBytes() == 0, phase + ".pending.budget_released", "verification released its reservations");
    }

    /** The exact persisted preparation of one generation, decoded by the production codec. */
    private static DocumentPublicationPreparationRecord loadedRecord(Tx tx, PayloadBudget budget, RepositoryCaller caller,
            RepositoryOperationLedger.Key key, long generation) {
        var row = tx.readOnly(em -> (Object[]) em.createNativeQuery("""
                SELECT c.claim_epoch,c.claim_token,c.lease_until,c.command_sha256 FROM repository_execution_claims c
                WHERE c.account_id=:a AND c.principal=:p AND c.operation_id=:o
                """).setParameter("a", key.account()).setParameter("p", key.principal()).setParameter("o", key.operationId()).getSingleResult());
        var claim = new RepositoryExecutionClaimLedger.Claim(key, java.util.HexFormat.of().formatHex((byte[]) row[3]),
                ((Number) row[0]).longValue(), (UUID) row[1], (java.time.Instant) row[2]);
        try (var loaded = new DocumentPublicationPreparationJournal(tx, budget).load(caller, claim, generation, NONE)
                .orElseThrow(() -> new RepositoryBackupRehearsalFailure("Preparation generation " + generation + " is missing"))) {
            return loaded.record();
        }
    }

    static Map<String, Object> coverage(Tx tx, UUID operation) {
        try (var ledger = RepositoryBackupRehearsalLedger.fromEnvironment()) { return ledger.coverageState(operation); }
    }

    @SuppressWarnings("unchecked")
    static List<Long> generations(Map<String, Object> state, String table) {
        var values = (List<Object>) state.get(table);
        return values.stream().map(value -> ((Number) value).longValue()).toList();
    }

    static String summary(Map<String, Object> state) {
        var text = new StringBuilder();
        for (String table : List.of("repository_publication_preparations", "repository_preparation_coverage_certificates",
                "repository_preparation_coverage_lineage", "repository_preparation_coverage_unresolved", "repository_successor_installs",
                "repository_successor_executions", "repository_operation_success"))
            text.append(table.replace("repository_", "")).append('=').append(generations(state, table)).append(' ');
        return text.toString().strip();
    }

    /** The real leases expire; timestamps are never edited to force recovery. */
    private static void awaitLeaseExpiry(Tx tx, UUID operation) {
        tx.readOnly(em -> em.createNativeQuery("""
                SELECT pg_sleep(GREATEST(0,EXTRACT(EPOCH FROM (GREATEST(c.lease_until,o.lease_until)-clock_timestamp())))+0.1)
                FROM repository_execution_claims c JOIN repository_operation_owners o USING(account_id,principal,operation_id)
                WHERE c.operation_id=:operation
                """).setParameter("operation", operation).getSingleResult());
    }
}
