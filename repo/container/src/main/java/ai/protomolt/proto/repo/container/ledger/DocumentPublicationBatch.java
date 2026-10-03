package ai.protomolt.proto.repo.container.ledger;

import jakarta.persistence.EntityManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.BiConsumer;

/**
 * Internal SQL publication primitive, not the public repository commit contract.
 * All provider work must finish before entry. Callbacks may persist outbox rows,
 * but must not perform network I/O, start another transaction, or lock unrelated
 * documents/attempts. Cross-document immutable part reuse needs a new lock audit.
 * Prepared candidate records are thread-confined for the duration of save.
 */
final class DocumentPublicationBatch {
    private DocumentPublicationBatch() {}

    record Publication(DocumentRecord candidate, Long expectedRevision, Map<UUID, Long> sources,
            UUID attemptId, UUID token, DocumentPublicationTarget target,
            List<DocumentSourceSnapshot> snapshots, Runnable check,
            BiConsumer<EntityManager, DocumentRecord> committed) {
        Publication {
            Objects.requireNonNull(candidate, "candidate");
            Objects.requireNonNull(candidate.nodeId, "nodeId");
            Objects.requireNonNull(attemptId, "attemptId");
            Objects.requireNonNull(token, "token");
            Objects.requireNonNull(target, "target");
            Objects.requireNonNull(check, "check");
            Objects.requireNonNull(committed, "committed");
            sources = Map.copyOf(sources);
            snapshots = DocumentSourceSnapshot.matching(sources, snapshots);
        }
    }

    static List<DocumentRecord> save(Tx tx, List<Publication> publications) {
        var prepared = prepare(publications);
        return tx.inTransaction(em -> { return execute(em, prepared); });
    }

    /**
     * Participate in an existing coordinator transaction. Acquire its operation
     * owner fence before entry; this method then acquires domain locks. No commit,
     * nested transaction or provider I/O occurs here. Failure marks the entire
     * transaction rollback-only even if an internal caller catches the exception.
     * Returned rows are provisional managed state until the outer commit succeeds;
     * do not expose them as a successful publication before that commit.
     * This method does not itself establish command, upload or outcome bindings.
     */
    static List<DocumentRecord> saveInTransaction(EntityManager em, List<Publication> publications) {
        if (!em.getTransaction().isActive() || em.getTransaction().getRollbackOnly())
            throw new IllegalStateException("Publication requires an active writable transaction");
        try {
            return execute(em, prepare(publications));
        } catch (RuntimeException | Error failure) {
            try { em.getTransaction().setRollbackOnly(); }
            catch (RuntimeException rollbackFailure) { failure.addSuppressed(rollbackFailure); }
            throw failure;
        }
    }

    private record Prepared(List<Publication> batch, List<Publication> ordered,
            java.util.Set<UUID> destinations, Map<UUID, Long> sources, long candidateParts) {}

    private static Prepared prepare(List<Publication> publications) {
        var batch = List.copyOf(publications);
        // Internal guard against accidentally unbounded lock/transaction growth.
        // The eventual public commit contract must specify its own full budgets.
        if (batch.isEmpty() || batch.size() > 64)
            throw new IllegalArgumentException("Publication batch requires 1 to 64 destinations");
        var destinations = new HashSet<UUID>();
        var attempts = new HashSet<UUID>();
        var sources = new HashMap<UUID, Long>();
        long newParts = 0;
        long sourceChecks = 0;
        for (var publication : batch) {
            if (!destinations.add(publication.candidate.nodeId) || !attempts.add(publication.attemptId))
                throw new IllegalArgumentException("Duplicate publication destination or attempt");
            publication.sources.forEach((id, revision) -> {
                var previous = sources.putIfAbsent(id, revision);
                if (previous != null && !previous.equals(revision))
                    throw new IllegalArgumentException("Conflicting source revisions");
            });
            if (batch.size() > 1) {
                newParts += publication.candidate.readManifest().getPartsCount();
                sourceChecks += publication.sources.size();
            }
            publication.check.run();
        }
        // Preserve the existing single-document limits. Multiple destinations
        // must not multiply the existing 10,000-part/source plan ceiling.
        if (batch.size() > 1 && (newParts > 10000 || sourceChecks > 10000))
            throw new IllegalArgumentException("Batch exceeds 10000 parts or source checks");
        final long candidateParts = newParts;
        var ordered = batch.stream().sorted(java.util.Comparator.comparing(p -> p.candidate.nodeId)).toList();
        return new Prepared(batch, ordered, java.util.Set.copyOf(destinations), Map.copyOf(sources), candidateParts);
    }

    private static List<DocumentRecord> execute(EntityManager em, Prepared prepared) {
        var batch = prepared.batch;
        var ordered = prepared.ordered;
        var destinations = prepared.destinations;
        var sources = prepared.sources;
        long candidateParts = prepared.candidateParts;
        var locked = DocumentLedger.lockRevisions(em, destinations, sources);

        if (batch.size() > 1) {
            long affectedParts = candidateParts;
            for (var id : destinations) {
                var prior = locked.get(id);
                var manifest = prior == null ? null : prior.readManifest();
                if (manifest != null) affectedParts += manifest.getPartsCount();
            }
            if (affectedParts > 10000)
                throw new IllegalArgumentException("Batch exceeds 10000 new and prior parts");
        }
        // Validate the original snapshot for every member before any merge.
        // A destination may also be another member's source at its old revision.
        for (var publication : ordered) {
            publication.check.run();
            DocumentLedger.requireRevision(locked.get(publication.candidate.nodeId), publication.expectedRevision);
            for (var source : publication.snapshots) source.requireCurrent(em);
        }
        DocumentSourceSnapshot.lockDrives(em, ordered.stream().map(Publication::target).toList(),
                ordered.stream().flatMap(p -> p.snapshots.stream()).toList());
        for (var publication : ordered) validate(em, publication, locked.get(publication.candidate.nodeId));
        var saved = new HashMap<UUID, DocumentRecord>();
        for (var publication : ordered) saved.put(publication.candidate.nodeId, publish(em, publication));
        // Cancellation during a later member must also abort earlier members.
        for (var publication : ordered) publication.check.run();
        var results = new ArrayList<DocumentRecord>(batch.size());
        for (var publication : batch) results.add(saved.get(publication.candidate.nodeId));
        return List.copyOf(results);
    }

    private static void validate(EntityManager em, Publication p, DocumentRecord prior) {
        var attempt = DocumentPartAttemptLedger.requirePublishable(em, p.attemptId, p.token,
                p.candidate, p.expectedRevision, p.sources);
        p.target.requireMatches(em, p.candidate, attempt);
        var previousManifest = prior == null ? null : prior.readManifest();
        if (prior != null && (previousManifest == null || previousManifest.getDocVersion() <= 0))
            throw new DocumentPartAttemptLedger.FenceException("Existing document requires an explicit versioned manifest");
        long previousVersion = prior == null ? 0 : previousManifest.getDocVersion();
        if (p.candidate.readManifest().getDocVersion() != Math.addExact(previousVersion, 1))
            throw new DocumentPartAttemptLedger.FenceException("Document manifest version is not the next locked version");
        p.check.run();
    }

    private static DocumentRecord publish(EntityManager em, Publication p) {
        p.check.run();
        var row = em.merge(p.candidate);
        em.flush();
        em.refresh(row);
        em.createNativeQuery("""
                INSERT INTO document_part_publication_history(attempt_id,node_id,publication_revision,body)
                SELECT :attempt,node_id,mutation_revision,document_publication_body(documents)
                FROM documents WHERE node_id=:node
                """).setParameter("attempt", p.attemptId).setParameter("node", row.nodeId).executeUpdate();
        em.createNativeQuery("""
                INSERT INTO document_part_publications(node_id,attempt_id) VALUES (:node,:attempt)
                ON CONFLICT(node_id) DO UPDATE SET attempt_id=EXCLUDED.attempt_id
                """).setParameter("node", row.nodeId).setParameter("attempt", p.attemptId).executeUpdate();
        p.committed.accept(em, row);
        p.check.run();
        return row;
    }
}
