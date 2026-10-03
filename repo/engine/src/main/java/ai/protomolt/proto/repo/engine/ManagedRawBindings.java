package ai.protomolt.proto.repo.engine;

import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.codec.DocumentPartCodec;
import ai.protomolt.proto.repo.container.ledger.DocumentLedger;
import ai.protomolt.proto.repo.container.ledger.DocumentRecord;
import ai.protomolt.proto.repo.container.ledger.DriveLedger;
import ai.protomolt.proto.repo.container.ledger.DriveRecord;
import ai.protomolt.proto.repo.container.ledger.RawObjectLedger;
import ai.protomolt.proto.repo.container.ledger.RawObjectRecord;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.v1.Blob;
import ai.protomolt.proto.repo.v1.BlobBag;
import ai.protomolt.proto.repo.v1.ChecksumType;
import ai.protomolt.proto.repo.v1.Document;
import ai.protomolt.proto.repo.v1.DocumentPart;
import ai.protomolt.proto.repo.v1.PartState;
import jakarta.persistence.EntityManager;
import jakarta.persistence.LockModeType;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/** Validates managed references before storage writes and again at document publication. */
final class ManagedRawBindings {
    private final DocumentLedger documents;
    private final DriveLedger drives;
    private final String backendIdentity;

    ManagedRawBindings(DocumentLedger documents, DriveLedger drives, String backendIdentity) {
        if (backendIdentity != null && backendIdentity.isBlank())
            throw new IllegalArgumentException("managedBackendIdentity must not be blank");
        this.documents = documents;
        this.drives = drives;
        this.backendIdentity = backendIdentity;
    }

    /** Ordinary writes can retain a subset of their own refs, never acquire refs by guessing coordinates. */
    Plan writing(BlobBag bag, String account, DocumentRecord destination) {
        return prepare(bag, account, destination, false, Map.of(), Map.of());
    }

    Plan admitting(BlobBag bag, String account, DocumentRecord destination, UUID rawId, UUID token,
            UUID driveId, DriveState driveState) {
        return prepare(bag, account, destination, false, Map.of(rawId, token), Map.of(driveId, driveState));
    }

    /** Read and verify the exact fragment that the partial-save path will carry forward. */
    Plan copying(BlobStore store, DriveRecord drive, DocumentRecord source, String destinationAccount) {
        return prepare(readVerifiedBag(store, drive, source.readManifest()), destinationAccount, source, true, Map.of(), Map.of());
    }

    /** Copy completion must be verified too; a source can change between GET and COPY. */
    static void verifyCopy(BlobStore store, DriveRecord drive, ai.protomolt.proto.repo.v1.DocumentManifest manifest) {
        readVerifiedBag(store, drive, manifest);
    }

    private static BlobBag readVerifiedBag(BlobStore store, DriveRecord drive,
            ai.protomolt.proto.repo.v1.DocumentManifest manifest) {
        var entries = manifest.getPartsList().stream()
                .filter(entry -> entry.getPart() == DocumentPart.DOCUMENT_PART_BLOBS).toList();
        if (entries.size() > 1) throw RepositoryErrors.failedPrecondition("Source has duplicate BLOBS fragments");
        BlobBag bag = BlobBag.getDefaultInstance();
        if (!entries.isEmpty() && entries.getFirst().getState() == PartState.PART_STATE_PRESENT) {
            var part = entries.getFirst();
            var bytes = store.get(drive.bucket, part.getObjectKey()).data();
            if (bytes.length != part.getSizeBytes() || !DocumentPartCodec.sha256Hex(bytes).equals(part.getSha256()))
                throw RepositoryErrors.failedPrecondition("Source BLOBS does not match its manifest");
            try { bag = Document.parseFrom(bytes).getBlobBag(); }
            catch (com.google.protobuf.InvalidProtocolBufferException invalid) {
                throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,
                        "Source BLOBS cannot be decoded", invalid);
            }
        }
        return bag;
    }

    private Plan prepare(BlobBag bag, String account, DocumentRecord owner, boolean exact, Map<UUID, UUID> admissions,
            Map<UUID, DriveState> admittedDrives) {
        RawObjectLedger ledger = documents.rawObjects();
        Set<UUID> previous = owner == null ? Set.of() : Set.copyOf(ledger.references(owner.nodeId));
        Map<String, RawObjectRecord> allowed = new HashMap<>();
        Set<UUID> available = new HashSet<>(previous);
        available.addAll(admissions.keySet());
        for (UUID id : available) {
            RawObjectRecord row = ledger.find(id).orElseThrow(() ->
                    RepositoryErrors.failedPrecondition("Managed reference record is missing"));
            if (allowed.put(coordinate(row.driveName, row.objectKey), row) != null)
                throw RepositoryErrors.failedPrecondition("Managed reference coordinates are ambiguous");
        }
        List<Blob> blobs = switch (bag.getBlobDataCase()) {
            case BLOB -> List.of(bag.getBlob());
            case BLOBS -> bag.getBlobs().getBlobList();
            case BLOBDATA_NOT_SET -> List.of();
        };
        Map<UUID, RawObjectRecord> selected = new HashMap<>();
        Map<UUID, DriveState> driveStates = new HashMap<>(admittedDrives);
        for (Blob blob : blobs) {
            if (!blob.hasStorageRef()) continue;
            var ref = blob.getStorageRef();
            RawObjectRecord row = allowed.get(coordinate(ref.getDriveName(), ref.getObjectKey()));
            if (row == null && !DriveKeys.isManaged(ref.getObjectKey())) continue;
            if (backendIdentity == null)
                throw new RepositoryException(RepositoryException.Code.UNSUPPORTED,
                        "Managed raw references require a qualified backend identity");
            if (row == null) throw RepositoryErrors.failedPrecondition("Managed raw reference was not admitted for this document");
            if (!row.accountId.equals(account) || !row.backendIdentity.equals(backendIdentity))
                throw RepositoryErrors.failedPrecondition("Managed raw reference account or backend differs");
            if (blob.getSizeBytes() != row.expectedSize || !blob.hasChecksum() || !blob.getChecksum().equals(row.sha256)
                    || blob.getChecksumType() != ChecksumType.CHECKSUM_TYPE_SHA256
                    || !blob.hasMimeType() || !blob.getMimeType().equals(row.contentType)
                    || !Objects.equals(ref.hasVersionId() ? ref.getVersionId() : null, row.providerVersion))
                throw RepositoryErrors.failedPrecondition("Managed raw reference byte identity differs");
            DriveRecord current = drives.findById(row.driveId).orElseThrow(() ->
                    RepositoryErrors.failedPrecondition("Managed raw drive is unavailable"));
            requireDrive(row, current);
            DriveState state = DriveState.of(current);
            DriveState previousState = driveStates.putIfAbsent(row.driveId, state);
            if (previousState != null && !previousState.equals(state))
                throw RepositoryErrors.aborted("Managed raw drive changed during preparation");
            selected.put(row.rawId, row);
        }
        if (exact && !selected.keySet().equals(previous))
            throw RepositoryErrors.failedPrecondition("Source BLOBS and managed reference set disagree");
        return new Plan(ledger, drives, owner == null ? null : owner.nodeId, previous,
                Map.copyOf(selected), Map.copyOf(driveStates), admissions);
    }

    private static String coordinate(String drive, String key) {
        // Length-prefixing avoids ambiguity without imposing restrictions on opaque keys.
        return drive.length() + ":" + drive + key;
    }

    private static void requireDrive(RawObjectRecord raw, DriveRecord drive) {
        if (drive == null || !raw.accountId.equals(drive.accountId) || !raw.driveName.equals(drive.name)
                || !raw.bucket.equals(drive.bucket))
            throw RepositoryErrors.failedPrecondition("Managed raw drive no longer resolves to its admitted location");
    }

    /** All mutable location/configuration fields, sampled after the host backend gate. */
    record DriveState(String account, String name, String provider, String bucket, String prefix,
            String region, String credentialsRef, String metadata, String config, String status) {
        static DriveState of(DriveRecord row) {
            return new DriveState(row.accountId, row.name, row.provider, row.bucket, row.prefix,
                    row.region, row.credentialsRef, row.metadata, row.providerConfig, row.status);
        }
    }

    record Plan(RawObjectLedger ledger, DriveLedger drives, UUID ownerId, Set<UUID> previous,
            Map<UUID, RawObjectRecord> selected, Map<UUID, DriveState> driveStates, Map<UUID, UUID> admissions) {
        void publish(EntityManager em, DocumentRecord destination) {
            if (ownerId != null && !new HashSet<>(RawObjectLedger.references(em, ownerId)).equals(previous))
                throw RepositoryErrors.aborted("Managed source references changed before publication");
            // Documents are already locked. Drive locks precede sorted raw locks;
            // raw garbage collection never locks a drive or document.
            var driveIds = new java.util.TreeSet<>(driveStates.keySet());
            Map<UUID, DriveRecord> lockedDrives = new HashMap<>();
            for (UUID id : driveIds) {
                DriveRecord locked = em.find(DriveRecord.class, id, LockModeType.PESSIMISTIC_READ);
                if (locked == null || !driveStates.get(id).equals(DriveState.of(locked)))
                    throw RepositoryErrors.aborted("Managed raw drive changed before publication");
                drives.validateBackend(locked);
                lockedDrives.put(id, locked);
            }
            for (RawObjectRecord raw : selected.values()) requireDrive(raw, lockedDrives.get(raw.driveId));
            try {
                ledger.replaceReferences(em, destination, selected.keySet().stream()
                        .map(id -> new RawObjectLedger.Binding(id, admissions.get(id))).toList(), admissions);
            } catch (RawObjectLedger.FenceException fenced) {
                throw new RepositoryException(RepositoryException.Code.CONFLICT,
                        "Managed raw content became unavailable before publication", fenced);
            }
        }
    }
}
