package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.PayloadBudget;
import ai.protomolt.proto.repo.spi.*;
import java.time.Duration;
import java.util.Set;
import java.util.List;
import java.util.UUID;
import static ai.protomolt.proto.repo.container.ledger.DocumentNativePublicationFixture.*;
import static ai.protomolt.proto.repo.container.ledger.DocumentCaptureAdmissionClosureIT.Rig;

/** A second real operation shares the retained source, not the first operation's ownership. */
final class HistoricalRetirementFixture {
    static Rig sibling(Context c, Rig original) throws Exception {
        var caller = new RepositoryCaller("principal", true);
        var command = new DocumentPublicationCommand(original.record().command().intent().toBuilder()
                .setOperationId(UUID.randomUUID().toString()).build());
        var key = new RepositoryOperationLedger.Key("account", "principal", command.operationId());
        var record = new DocumentPublicationPreparationRecord(key, command, DocumentPublicationSeeds.mint(key, command),
                original.record().placements(), Duration.ofSeconds(1), 0);
        var reads = new DocumentReadLedger(c.tx(), UUID.randomUUID());
        var history = reads.captureHistorical(caller, original.fixture().address(), original.fixture().revision());
        var sources = DocumentHistoricalAssessmentSources.open(command, caller, List.of(history), RepositoryReadControl.NONE);
        var coordinator = UUID.randomUUID();
        boolean delivered = false;
        try {
            var references = sources.references(command, () -> {});
            var pins = DocumentPreparationSourcePins.prepare(command, references, () -> {});
            var claim = c.tx().inTransaction(em -> {
                var acquired = RepositoryExecutionClaimLedger.acquireHistoricalInitialInTransaction(em, key, command,
                        UUID.randomUUID(), record.lease(), sources);
                RepositoryCoordinatorBinding.bindInitial(em, acquired, coordinator);
                var bytes = DocumentPublicationPreparationCodec.encode(record);
                DocumentPublicationPreparationJournal.insert(em, acquired.claim(), record, bytes,
                        DocumentPublicationPreparationJournal.digest(bytes), references);
                var objects = pins.pins().stream().map(DocumentHistoricalSourcePin::object).collect(java.util.stream.Collectors.toSet());
                var origins = DocumentPublicationLocks.lockIndependentOrigins(em,
                        Set.of(ai.protomolt.proto.repo.container.blob.DocumentIds.nodeId(original.fixture().address())), objects, Set.of());
                DocumentPublicationLocks.lockIndependentRetention(em, origins);
                DocumentPreparationSourcePins.insert(em, record, pins, acquired.claim(), coordinator, () -> {});
                return acquired.claim();
            });
            delivered = true;
            return new Rig(original.fixture(), reads, history, sources, record, claim, coordinator,
                    new PayloadBudget(64L * 1024 * 1024));
        } finally {
            if (!delivered) {
                sources.close(); history.close(); history.release(); reads.fence(); reads.attestLocalQuiescence();
            }
        }
    }
}
