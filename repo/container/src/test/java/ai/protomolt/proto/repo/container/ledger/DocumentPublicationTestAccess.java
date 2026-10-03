package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.BackendIdentity;
import java.util.Map;

/** Test-only access to the internal atomic publisher; never ships in the library. */
public final class DocumentPublicationTestAccess {
    private DocumentPublicationTestAccess() {}
    public static DocumentRecord publish(Tx tx, DocumentRecord row, DocumentPartAttemptLedger.Attempt attempt,
            DriveRecord drive, BackendIdentity backend) {
        return new DocumentLedger(tx).saveVerifiedAttempt(row, null, Map.of(), attempt.id(), attempt.token(),
                new DocumentPublicationTarget(new DriveLedger(tx), drive, attempt.location().backendGeneration(), backend), (em, committed) -> {});
    }
}
