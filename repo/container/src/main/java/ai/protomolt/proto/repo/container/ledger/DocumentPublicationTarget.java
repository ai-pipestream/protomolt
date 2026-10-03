package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.blob.spi.BackendIdentity;
import jakarta.persistence.EntityManager;
import jakarta.persistence.LockModeType;
import java.util.Objects;
import java.util.UUID;

/** Immutable sample of the qualified composition's selected drive and backend. */
final class DocumentPublicationTarget {
    private final UUID driveId;
    private final DriveState sampled;
    private final String generation;
    private final BackendIdentity backend;
    private final DriveLedger drives;

    DocumentPublicationTarget(DriveLedger drives, DriveRecord drive, String generation, BackendIdentity backend) {
        this.drives = Objects.requireNonNull(drives);
        this.driveId = Objects.requireNonNull(drive.driveId);
        this.sampled = DriveState.of(drive);
        this.generation = Objects.requireNonNull(generation);
        this.backend = Objects.requireNonNull(backend);
    }

    void lock(EntityManager em) {
        var current = em.find(DriveRecord.class, driveId, LockModeType.PESSIMISTIC_READ);
        if (current == null || !sampled.equals(DriveState.of(current)))
            throw new DocumentPartAttemptLedger.FenceException("Document drive changed after admission");
        drives.validateBackend(current);
    }

    /** Validate the complete physical destination before admission or provider I/O. */
    void requirePlan(EntityManager em, DocumentPartAttemptLedger.Plan plan,
            ai.protomolt.proto.repo.v1.NodeAddress address) {
        lock(em);
        var profile = ManagedBackendLedger.find(em, generation)
                .orElseThrow(() -> new IllegalArgumentException("Document backend profile is missing"));
        var location = plan.location();
        if (!generation.equals(location.backendGeneration()) || !backend.equals(profile.identity())
                || !backend.provider().equals(sampled.provider) || !"ACTIVE".equals(sampled.status)
                || !Objects.equals(sampled.account, address.getAccountId())
                || !address.getAccountId().equals(location.accountId())
                || !ai.protomolt.proto.repo.container.blob.DocumentIds.nodeId(address).equals(location.nodeId())
                || !Objects.equals(sampled.namespace, location.namespace()))
            throw new IllegalArgumentException("Document plan differs from selected drive or address");
        String prefix = ai.protomolt.proto.repo.codec.RepositoryNamespaces.under(sampled.prefix,
                "documents/" + location.accountId() + "/" + location.nodeId() + "/attempts/" + plan.attemptId() + "/");
        for (var object : plan.objects()) {
            if (!object.objectKey().startsWith(prefix) || object.objectKey().length() == prefix.length())
                throw new IllegalArgumentException("Document plan key is outside the selected drive prefix");
        }
    }

    void requireMatches(EntityManager em, DocumentRecord candidate, DocumentPartAttemptLedger.Attempt attempt) {
        var profile = ManagedBackendLedger.find(em, generation)
                .orElseThrow(() -> new DocumentPartAttemptLedger.FenceException("Document backend profile is missing"));
        if (!generation.equals(attempt.location().backendGeneration()) || !backend.equals(profile.identity())
                || !Objects.equals(sampled.provider, backend.provider())
                || !profile.storageRealm().equals(attempt.storageRealm())
                || !Objects.equals(sampled.account, candidate.accountId) || !Objects.equals(sampled.name, candidate.driveName)
                || !Objects.equals(sampled.namespace, attempt.location().namespace()) || !"ACTIVE".equals(sampled.status))
            throw new DocumentPartAttemptLedger.FenceException("Document drive or backend differs from admitted physical location");
    }

    private record DriveState(String account, String name, String provider, String namespace, String prefix,
            String region, String credentials, String metadata, String config, String status) {
        static DriveState of(DriveRecord drive) {
            return new DriveState(drive.accountId, drive.name, drive.provider, drive.bucket, drive.prefix,
                    drive.region, drive.credentialsRef, drive.metadata, drive.providerConfig, drive.status);
        }
    }
}
