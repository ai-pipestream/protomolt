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
    public record Binding(String generation, ManagedBackendLedger.Profile profile, String namespace) {
        public Binding {
            if (generation == null || generation.isBlank() || namespace == null || namespace.isBlank())
                throw new IllegalArgumentException("Published parts require original backend and namespace");
            Objects.requireNonNull(profile, "profile");
        }
    }
    public record BoundPart(Part part, Binding binding) {
        public BoundPart { Objects.requireNonNull(part); Objects.requireNonNull(binding); }
    }
    public record Publication(UUID revisionId, DocumentManifest manifest, List<BoundPart> boundParts) {
        public Publication {
            Objects.requireNonNull(revisionId); Objects.requireNonNull(manifest);
            boundParts = List.copyOf(boundParts);
            var slots=new java.util.HashSet<java.util.Map.Entry<DocumentPart,String>>();
            for (var selected : boundParts)
                if (!slots.add(java.util.Map.entry(selected.part().part(),selected.part().subKey())))
                    throw new IllegalArgumentException("Published part slots must be unique");
        }
        /** Existing full-attempt publications have one original binding for every part. */
        public Publication(UUID revisionId, String generation, ManagedBackendLedger.Profile profile,
                String namespace, DocumentManifest manifest, List<Part> parts) {
            this(revisionId, manifest, bind(parts, new Binding(generation, profile, namespace)));
        }
        public List<Part> parts() { return boundParts.stream().map(BoundPart::part).toList(); }
        private static List<BoundPart> bind(List<Part> parts, Binding binding) {
            return parts.stream().map(part -> new BoundPart(part, binding)).toList();
        }
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
                    SELECT p.attempt_id,c.revision_id,r.projection_sealed,a.planned_count,
                        r.legacy_attempt_id=p.attempt_id AND r.node_id=d.node_id
                        AND r.body=h.body AND r.body=document_publication_body(d)
                    FROM documents d LEFT JOIN document_part_publications p ON p.node_id=d.node_id
                    LEFT JOIN document_revision_current c ON c.node_id=d.node_id
                    LEFT JOIN document_revision_publications r ON r.revision_id=c.revision_id
                    LEFT JOIN document_part_publication_history h ON h.attempt_id=p.attempt_id
                    LEFT JOIN document_part_attempts a ON a.attempt_id=p.attempt_id
                    WHERE d.node_id=:node
                    """).setParameter("node",sampled.nodeId).getResultList();
            Object[] binding = (Object[])bindings.getFirst();
            if (binding[0]==null && binding[1]==null) return Optional.empty();
            if (binding[0]==null || binding[1]==null || !Boolean.TRUE.equals(binding[2]) || !Boolean.TRUE.equals(binding[4]))
                throw new IllegalStateException("Document revision projection disagrees with its publication");
            UUID revision = (UUID)binding[1];
            var rows=em.unwrap(org.hibernate.Session.class).createNativeQuery("""
                    SELECT r.revision_ordinal,r.part,r.sub_key,l.object_key,o.expected_size,o.expected_sha256,
                        o.provider_version,o.etag,l.backend_generation,l.storage_namespace,l.storage_realm
                    FROM document_revision_parts r
                    JOIN repository_physical_locations l ON l.object_id=r.object_id AND l.source_kind='DOCUMENT_PART'
                    JOIN document_part_attempt_objects o ON o.physical_object_id=l.object_id
                        AND o.attempt_id=l.source_id AND o.ordinal=l.source_ordinal AND o.verified
                        AND o.part=r.part AND o.sub_key=r.sub_key AND o.object_key=l.object_key
                        AND o.storage_namespace=l.storage_namespace AND o.storage_realm=l.storage_realm
                    JOIN document_part_attempts a ON a.attempt_id=o.attempt_id AND a.state='VERIFIED'
                        AND a.backend_generation=l.backend_generation AND a.storage_realm=l.storage_realm
                    WHERE r.revision_id=:id ORDER BY r.revision_ordinal
                    """,Object[].class).setParameter("id",revision).getResultList();
            if (rows.size()!=((Number)binding[3]).intValue())
                throw new IllegalStateException("Published document part plan is incomplete");
            var generations=rows.stream().map(row -> (String)row[8]).collect(java.util.stream.Collectors.toSet());
            var profiles=ManagedBackendLedger.requireAll(em,generations);
            var parts = new ArrayList<BoundPart>();
            var verified = new ArrayList<DocumentPartPublication.VerifiedPart>();
            var manifest=current.readManifest();
            for (var p : rows) {
                int position=((Number)p[0]).intValue();
                var part=new Part(DocumentPart.forNumber(((Number)p[1]).intValue()),(String)p[2],(String)p[3],
                        ((Number)p[4]).longValue(),(String)p[5],(String)p[6],(String)p[7]);
                if (position<0 || position>=manifest.getPartsCount()) throw new IllegalStateException("Published part position is invalid");
                var entry=manifest.getParts(position);
                if (entry.getState()!=ai.protomolt.proto.repo.v1.PartState.PART_STATE_PRESENT
                        || entry.getPart()!=part.part() || !entry.getSubKey().equals(part.subKey()))
                    throw new IllegalStateException("Published part position differs from manifest");
                var profile=profiles.get((String)p[8]);
                if (!profile.storageRealm().equals(p[10])) throw new IllegalStateException("Published backend realm differs from physical binding");
                parts.add(new BoundPart(part,new Binding((String)p[8],profile,(String)p[9])));
                verified.add(new DocumentPartPublication.VerifiedPart(verified.size(),part.part(),part.subKey(),
                        part.key(),part.size(),part.sha256(),part.providerVersion(),part.etag()));
            }
            // Legacy FULL_REVISION remains authoritative until retention/publication cutover.
            DocumentPartPublication.validate(current,verified);
            return Optional.of(new Publication(revision,manifest,parts));
    }
}
