package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.v1.DocumentManifest;
import com.google.protobuf.util.JsonFormat;
import jakarta.persistence.EntityManager;
import jakarta.persistence.LockModeType;
import java.util.Objects;
import java.util.UUID;

/**
 * Immutable physical-source fence. Sampling does not authorize access: the caller
 * supplies an already-authorized row and fences applicable policy separately.
 */
public final class DocumentSourceSnapshot {
    private final UUID nodeId;
    private final long revision;
    private final UUID publicationAttempt;
    private final DocumentPublicationLedger.Publication retainedPublication;
    private final DocumentDriveSnapshot legacyDrive;
    private final DriveLedger drives;
    private final DocumentManifest manifest;
    private final String coreVersion;
    private final String coreEtag;

    private DocumentSourceSnapshot(DocumentRecord row, DocumentPublicationLedger.Publication publication,
            DocumentDriveSnapshot legacyDrive, DriveLedger drives) {
        this.nodeId = row.nodeId;
        this.revision = row.mutationRevision;
        this.retainedPublication = publication;
        this.publicationAttempt = publication == null ? null : publication.attemptId();
        this.legacyDrive = legacyDrive;
        this.drives = drives;
        var builder = DocumentManifest.newBuilder();
        if (row.partManifest == null) throw new DocumentPartAttemptLedger.FenceException("Source manifest is missing");
        try { JsonFormat.parser().merge(row.partManifest, builder); }
        catch (com.google.protobuf.InvalidProtocolBufferException invalid) {
            throw new DocumentPartAttemptLedger.FenceException("Source manifest is invalid", invalid);
        }
        manifest = builder.build();
        var address = manifest.getAddress();
        if (!manifest.hasAddress() || manifest.getDocVersion() <= 0
                || !Objects.equals(row.accountId, address.getAccountId()) || !Objects.equals(row.docId, address.getDocId())
                || !Objects.equals(row.graphId, address.getGraphId()) || !Objects.equals(row.graphAddressId, address.getGraphAddressId()))
            throw new DocumentPartAttemptLedger.FenceException("Source manifest differs from its row identity");
        coreVersion = row.versionId;
        coreEtag = row.etag;
    }

    public static DocumentSourceSnapshot legacy(Tx tx, DriveLedger drives, DocumentRecord authorized, DriveRecord selectedDrive) {
        var drive = DocumentDriveSnapshot.of(selectedDrive);
        return tx.inTransaction(em -> {
            var current = current(em, authorized.nodeId, authorized.mutationRevision);
            if (publication(em, current.nodeId) != null)
                throw new DocumentPartAttemptLedger.FenceException("Legacy source now has a managed publication");
            if (!Objects.equals(current.accountId, drive.account()) || !Objects.equals(current.driveName, drive.name())
                    || !"ACTIVE".equals(drive.status()))
                throw new DocumentPartAttemptLedger.FenceException("Legacy source differs from selected drive");
            drive.lock(em, drives);
            return new DocumentSourceSnapshot(current, null, drive, drives);
        });
    }

    /** Resolve durable provider/part identity in the same transaction as the authorized revision check. */
    public static DocumentSourceSnapshot bound(Tx tx, DocumentRecord authorized) {
        return tx.inTransaction(em -> {
            var current = current(em, authorized.nodeId, authorized.mutationRevision);
            var selected = DocumentPublicationLedger.findForRead(em, current)
                    .orElseThrow(() -> new DocumentPartAttemptLedger.FenceException("Source has no managed publication"));
            var source = new DocumentSourceSnapshot(current, selected, null, null);
            if (!source.manifest.equals(selected.manifest()))
                throw new DocumentPartAttemptLedger.FenceException("Source publication manifest changed");
            return source;
        });
    }

    public UUID nodeId() { return nodeId; }
    public long revision() { return revision; }
    public DocumentManifest manifest() { return manifest; }
    public String coreVersion() { return coreVersion; }
    public String coreEtag() { return coreEtag; }
    public boolean legacy() { return legacyDrive != null; }
    public java.util.Optional<DocumentPublicationLedger.Publication> publication() { return java.util.Optional.ofNullable(retainedPublication); }

    static java.util.List<DocumentSourceSnapshot> matching(java.util.Map<UUID, Long> expected,
            java.util.List<DocumentSourceSnapshot> snapshots) {
        var sources = snapshots.stream().sorted(java.util.Comparator.comparing(DocumentSourceSnapshot::nodeId)).toList();
        var revisions = new java.util.HashMap<UUID, Long>();
        for (var source : sources) if (revisions.put(source.nodeId(), source.revision()) != null)
            throw new IllegalArgumentException("Duplicate source snapshot");
        if (!expected.equals(revisions)) throw new IllegalArgumentException("Source snapshots differ from plan revisions");
        return sources;
    }

    /** Caller must use the same sorted source row lock order as DocumentLedger. */
    void requireCurrent(EntityManager em) {
        current(em, nodeId, revision);
        if (!Objects.equals(publicationAttempt, publication(em, nodeId)))
            throw new DocumentPartAttemptLedger.FenceException("Source publication changed");
    }

    static void lockDrives(EntityManager em, DocumentPublicationTarget target, java.util.List<DocumentSourceSnapshot> sources) {
        record Lock(UUID id, Runnable action) {}
        var locks = new java.util.ArrayList<Lock>();
        locks.add(new Lock(target.driveId(), () -> target.lock(em)));
        for (var source : sources) if (source.legacyDrive != null)
            locks.add(new Lock(source.legacyDrive.id(), () -> source.legacyDrive.lock(em, source.drives)));
        // Repeated IDs still validate every sampled state, while acquiring locks in one order.
        locks.sort(java.util.Comparator.comparing(Lock::id));
        for (var lock : locks) lock.action().run();
    }

    private static DocumentRecord current(EntityManager em, UUID node, long revision) {
        var current = em.find(DocumentRecord.class, node, LockModeType.PESSIMISTIC_READ);
        if (current == null || revision <= 0 || current.mutationRevision != revision)
            throw new DocumentLedger.RevisionConflictException();
        if (!DocumentStatus.AVAILABLE.equals(current.status) || current.pendingPurgeId != null)
            throw new DocumentPartAttemptLedger.FenceException("Source document is unavailable");
        return current;
    }

    private static UUID publication(EntityManager em, UUID node) {
        var rows = em.createNativeQuery("SELECT attempt_id FROM document_part_publications WHERE node_id=:node")
                .setParameter("node", node).getResultList();
        return rows.isEmpty() ? null : (UUID) rows.getFirst();
    }
}
