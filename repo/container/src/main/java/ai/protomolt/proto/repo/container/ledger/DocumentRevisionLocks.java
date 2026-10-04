package ai.protomolt.proto.repo.container.ledger;

import jakarta.persistence.EntityManager;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;

/** Ordered read/write revision locks. Callers own authorization and expected-revision checks. */
final class DocumentRevisionLocks {
    private DocumentRevisionLocks() {}

    record SourceView(UUID nodeId,String accountId,String docId,String graphId,String graphAddressId,
            long mutationRevision,String status,UUID pendingPurgeId,String security) {
        static SourceView of(DocumentRecord row) {
            return new SourceView(row.nodeId,row.accountId,row.docId,row.graphId,row.graphAddressId,
                    row.mutationRevision,row.status,row.pendingPurgeId,row.security);
        }
    }
    record Locked(Map<UUID,DocumentRecord> documents,Map<UUID,SourceView> sources) {}

    static Map<UUID,DocumentRecord> lock(EntityManager em, Set<UUID> destinations, Set<UUID> sources) {
        return lock(em,destinations,sources,false).documents();
    }

    /** Source-only rows omit manifests and other payload metadata; overlapping destinations remain full rows. */
    static Locked lockForAdmission(EntityManager em,Set<UUID> destinations,Set<UUID> sources) {
        return lock(em,destinations,sources,true);
    }

    private static Locked lock(EntityManager em,Set<UUID> destinations,Set<UUID> sources,boolean leanSources) {
        if (!em.getTransaction().isActive()) throw new IllegalStateException("Revision locks require an active transaction");
        if (destinations.size()>64 || sources.size()>10000)
            throw new IllegalArgumentException("Revision locks exceed 64 destinations or 10000 sources");
        var identities=new TreeSet<>(sources); identities.addAll(destinations);
        if (identities.isEmpty()) return new Locked(Map.of(),Map.of());
        if (destinations.containsAll(sources)) {
            // An all-write set uses the established exclusive batch protocol.
            // No database capability probing or alternate correctness path.
            em.createNativeQuery("SELECT lock_document_revision_keys(CAST(:keys AS bigint[]))")
                    .setParameter("keys",keys(destinations)).getSingleResult();
        } else {
            // SQL promotes collisions to the strongest mode before acquiring any key.
            em.createNativeQuery("SELECT lock_document_admission_keys(CAST(:reads AS bigint[]),CAST(:writes AS bigint[]))")
                    .setParameter("reads",keys(sources)).setParameter("writes",keys(destinations)).getSingleResult();
        }
        var ordered=List.copyOf(identities);
        Map<UUID,DocumentRecord> locked=new HashMap<>();
        (leanSources ? destinations : identities).forEach(id -> locked.put(id,null));
        Map<UUID,SourceView> sourceViews=new HashMap<>();
        for (int start=0; start<ordered.size();) {
            boolean write=destinations.contains(ordered.get(start));
            int end=start+1;
            while (end<ordered.size() && end-start<256 && destinations.contains(ordered.get(end))==write) end++;
            String ids=ordered.subList(start,end).stream().map(UUID::toString).collect(Collectors.joining(",","{","}"));
            // Keep global Java UUID order across modes and batches. Existing
            // multi-document deletion takes that row order without advisory locks.
            if (leanSources && !write) {
                var rows=em.createNativeQuery("""
                        SELECT d.node_id,d.account_id,d.doc_id,d.graph_id,d.graph_address_id,
                            d.mutation_revision,d.status,d.pending_purge_id,d.security::text
                        FROM unnest(CAST(:ids AS uuid[])) WITH ORDINALITY AS requested(id,position)
                        JOIN documents d ON d.node_id=requested.id
                        ORDER BY requested.position FOR SHARE OF d
                        """).setParameter("ids",ids).getResultList();
                for (Object value:rows) {
                    Object[] row=(Object[])value;
                    var source=new SourceView((UUID)row[0],(String)row[1],(String)row[2],(String)row[3],(String)row[4],
                            ((Number)row[5]).longValue(),(String)row[6],(UUID)row[7],(String)row[8]);
                    sourceViews.put(source.nodeId(),source);
                }
                start=end;
                continue;
            }
            var rows=em.unwrap(org.hibernate.Session.class).createNativeQuery("""
                    SELECT d.*,d.mutation_revision AS locked_revision
                    FROM unnest(CAST(:ids AS uuid[])) WITH ORDINALITY AS requested(id,position)
                    JOIN documents d ON d.node_id=requested.id
                    ORDER BY requested.position FOR %s OF d
                    """.formatted(write ? "UPDATE" : "SHARE"),Object[].class)
                    .addEntity("d",DocumentRecord.class).addScalar("locked_revision",Long.class)
                    .setParameter("ids",ids).getResultList();
            for (Object[] result : rows) {
                DocumentRecord row=(DocumentRecord)result[0];
                if (row.mutationRevision!=(Long)result[1]) throw new DocumentLedger.RevisionConflictException();
                locked.put(row.nodeId,row);
                if (leanSources && sources.contains(row.nodeId)) sourceViews.put(row.nodeId,SourceView.of(row));
            }
            start=end;
        }
        return new Locked(locked,sourceViews);
    }

    private static String keys(Set<UUID> identities) {
        return identities.stream().mapToLong(id -> id.getMostSignificantBits()^id.getLeastSignificantBits())
                .distinct().sorted().mapToObj(Long::toString).collect(Collectors.joining(",","{","}"));
    }
}
