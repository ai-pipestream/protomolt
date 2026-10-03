package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.blob.spi.BlobCapability;
import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.blob.spi.ObjectReclaimer;
import ai.protomolt.proto.repo.container.archive.*;
import ai.protomolt.proto.repo.container.ledger.ManagedBackendLedger;
import ai.protomolt.proto.repo.container.ledger.Tx;
import ai.protomolt.proto.repo.engine.*;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;

/** Qualified archive components sharing the host's borrowed provider lifetime. */
final class ManagedArchiveServices {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(ManagedArchiveServices.class);
    private static final Duration ABANDONED_CLAIM_AGE = Duration.ofHours(1);
    final ArchiveObjectReader reader;
    final ArchiveObjectWriter writer;
    final ArchiveMutationOperations mutations;
    private final ArchiveCleanupLedger cleanup;
    private final ArchiveObjectRecovery recovery;

    ManagedArchiveServices(Tx tx, ArchiveLedger archive, BlobStore store, Set<BlobCapability> capabilities,
            String generation, ManagedBackendLedger.Profile profile, ObjectReclaimer reclaimer) {
        var profiles = new ManagedBackendLedger(tx);
        if (!profiles.find(generation).filter(profile::equals).isPresent())
            throw new IllegalStateException("Original archive backend profile is not bound");
        reader = new ArchiveObjectReader(new ArchiveObjectLedger(tx), (original, realm) -> {
            if (!generation.equals(original) || !profile.storageRealm().equals(realm))
                throw new IllegalStateException("Original archive backend is not configured on this host");
            return store;
        });
        writer = new ArchiveObjectWriter(new ArchiveUploadLedger(tx), store, generation, capabilities, Duration.ofMinutes(5));
        mutations = new ArchiveMutationOperations(archive, new ArchiveMutationLedger(tx), new ArchiveMutationObservations(tx));
        cleanup = new ArchiveCleanupLedger(tx);
        recovery = new ArchiveObjectRecovery(cleanup, profiles, (original, originalProfile) -> {
            if (!generation.equals(original) || !profile.equals(originalProfile))
                throw new IllegalStateException("Original archive backend is not configured on this host");
            return reclaimer;
        });
    }

    /** Both lanes are bounded; separate host loops reserve reconciliation capacity. */
    void recoverMutations(Instant inactiveBefore, int limit) {
        Instant abandoned = Instant.now().minus(ABANDONED_CLAIM_AGE);
        recover(cleanup.mutationCandidates(inactiveBefore, abandoned, limit), inactiveBefore, abandoned);
    }

    void reconcile(Instant inactiveBefore, int limit) {
        recover(cleanup.candidates(inactiveBefore, limit), inactiveBefore, Instant.now().minus(ABANDONED_CLAIM_AGE));
    }

    private void recover(java.util.List<java.util.UUID> objects, Instant cutoff, Instant abandoned) {
        for (var object : objects) {
            if (Thread.currentThread().isInterrupted()) return;
            try { recovery.recover(object, cutoff, abandoned); }
            catch (RuntimeException failure) {
                LOG.warn("Archive cleanup failed for {}; durable retry remains pending", object, failure);
            }
        }
    }
}
