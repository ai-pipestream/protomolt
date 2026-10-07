package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/** Immutable private preparation, not a claim, live owner or restored execution session. */
record DocumentPublicationPreparationRecord(RepositoryOperationLedger.Key key, DocumentPublicationCommand command,
        DocumentPublicationSeeds seeds, Map<UUID, DocumentUploadPlan.Placement> placements,
        Duration lease, long predecessorGeneration) {
    DocumentPublicationPreparationRecord {
        Objects.requireNonNull(key); Objects.requireNonNull(command); Objects.requireNonNull(seeds);
        Objects.requireNonNull(lease); placements = Map.copyOf(placements);
        if (predecessorGeneration < 0 || predecessorGeneration == Long.MAX_VALUE)
            throw new IllegalArgumentException("Invalid preparation predecessor");
        seeds.requireCommand(key, command);
        if (command.intent().getMembersList().stream().anyMatch(member -> member.getPartsList().stream()
                .anyMatch(ai.protomolt.proto.repo.v1.DocumentPublicationPart::hasHistoricalReuse))) {
            // Persisting selectors does not reacquire live historical source preparations.
            DocumentOperationUploadAdmission.requireLease(lease);
            DocumentUploadPlan.validatePreparationShape(command, placements, seeds.attempts());
        } else {
            // Preserve the existing full ordinary preparation validation.
            DocumentOperationUploadAdmission.prepare(command, placements, seeds.attempts(), lease, seeds.uploadTokens());
        }
    }

    DocumentOperationUploadAdmission.Prepared prepare() {
        return DocumentOperationUploadAdmission.prepare(command, placements, seeds.attempts(), lease, seeds.uploadTokens());
    }

    @Override public String toString() { return "DocumentPublicationPreparationRecord[private]"; }
}
