package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.v1.DocumentManifest;
import ai.protomolt.proto.repo.v1.DocumentPart;
import jakarta.persistence.LockModeType;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/** Reads immutable physical bindings against an already-authorized document revision. */
public final class DocumentPublicationLedger {
    private final Tx tx;
    public DocumentPublicationLedger(Tx tx) { this.tx = Objects.requireNonNull(tx); }

    public record Part(DocumentPart part, String subKey, String key, long size, String sha256,
            String providerVersion, String etag) {}
    public record Publication(UUID attemptId, String generation, ManagedBackendLedger.Profile profile,
            String namespace, DocumentManifest manifest, List<Part> parts) {
        public Publication { parts = List.copyOf(parts); }
    }

    /**
     * Empty means the same current revision is explicitly unbound. A changed or
     * deleted row fails instead of returning a different body's binding under an
     * older authorization decision. Locks end before any provider I/O.
     */
    public Optional<Publication> findForRead(DocumentRecord sampled) {
        Objects.requireNonNull(sampled, "sampled");
        return tx.inTransaction(em -> { return findForRead(em, sampled); });
    }

    /** Same-transaction lookup for source capture; caller retains the row lock. */
    static Optional<Publication> findForRead(jakarta.persistence.EntityManager em, DocumentRecord sampled) {
            var current = em.find(DocumentRecord.class, sampled.nodeId, LockModeType.PESSIMISTIC_READ);
            if (current == null || current.mutationRevision != sampled.mutationRevision)
                throw new DocumentLedger.RevisionConflictException();
            List<?> bindings = em.createNativeQuery("""
                    SELECT a.attempt_id,a.backend_generation,a.storage_namespace,a.planned_count,
                        h.body=document_publication_body(d)
                    FROM document_part_publications p JOIN document_part_attempts a USING(attempt_id)
                    JOIN document_part_publication_history h USING(attempt_id)
                    JOIN documents d ON d.node_id=p.node_id
                    WHERE p.node_id=:node
                    """).setParameter("node",sampled.nodeId).getResultList();
            if (bindings.isEmpty()) return Optional.empty();
            Object[] binding = (Object[])bindings.getFirst();
            if (!Boolean.TRUE.equals(binding[4])) throw new IllegalStateException("Document publication snapshot disagrees with its row");
            UUID attempt = (UUID)binding[0];
            String generation = (String)binding[1];
            var profile = ManagedBackendLedger.find(em,generation)
                    .orElseThrow(() -> new IllegalStateException("Published document backend profile is missing"));
            var parts = new ArrayList<Part>();
            var verified = new ArrayList<DocumentPartPublication.VerifiedPart>();
            for (Object value : em.createNativeQuery("""
                    SELECT ordinal,part,sub_key,object_key,expected_size,expected_sha256,provider_version,etag
                    FROM document_part_attempt_objects WHERE attempt_id=:id AND verified ORDER BY ordinal
                    """).setParameter("id",attempt).getResultList()) {
                Object[] p=(Object[])value;
                var part=new Part(DocumentPart.forNumber(((Number)p[1]).intValue()),(String)p[2],(String)p[3],
                        ((Number)p[4]).longValue(),(String)p[5],(String)p[6],(String)p[7]);
                parts.add(part);
                verified.add(new DocumentPartPublication.VerifiedPart(((Number)p[0]).intValue(),part.part(),part.subKey(),
                        part.key(),part.size(),part.sha256(),part.providerVersion(),part.etag()));
            }
            if (parts.size()!=((Number)binding[3]).intValue())
                throw new IllegalStateException("Published document part plan is incomplete");
            DocumentPartPublication.validate(current,verified);
            return Optional.of(new Publication(attempt,generation,profile,(String)binding[2],current.readManifest(),parts));
    }
}
