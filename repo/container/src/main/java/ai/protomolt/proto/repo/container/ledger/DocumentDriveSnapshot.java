package ai.protomolt.proto.repo.container.ledger;

import jakarta.persistence.EntityManager;
import jakarta.persistence.LockModeType;
import java.util.Objects;
import java.util.UUID;

/** Immutable selected drive state, shared by source and destination publication fences. */
record DocumentDriveSnapshot(UUID id, String account, String name, String type, String provider, String namespace, String prefix,
        String region, String credentials, String metadata, String config, String status) {
    static DocumentDriveSnapshot of(DriveRecord drive) {
        return new DocumentDriveSnapshot(Objects.requireNonNull(drive.driveId), drive.accountId, drive.name, drive.driveType,
                drive.provider, drive.bucket, drive.prefix, drive.region, drive.credentialsRef,
                drive.metadata, drive.providerConfig, drive.status);
    }

    void lock(EntityManager em, DriveLedger drives) {
        var current = em.find(DriveRecord.class, id, LockModeType.PESSIMISTIC_READ);
        if (current == null || !equals(of(current)))
            throw new DocumentPartAttemptLedger.FenceException("Document drive changed since it was sampled");
        drives.validateBackend(current);
    }
}
