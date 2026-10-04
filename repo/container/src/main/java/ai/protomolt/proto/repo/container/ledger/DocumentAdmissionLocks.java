package ai.protomolt.proto.repo.container.ledger;

import jakarta.persistence.EntityManager;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;

/** Staging locks only; callers authorize all rows before comparing requested revisions. */
final class DocumentAdmissionLocks {
    private DocumentAdmissionLocks() {}

    static Map<UUID,DocumentRecord> lock(EntityManager em, Set<UUID> destinations, Set<UUID> sources) {
        if (!em.getTransaction().isActive()) throw new IllegalStateException("Admission locks require an active transaction");
        if (destinations.size()>64 || sources.size()>10000)
            throw new IllegalArgumentException("Admission exceeds 64 destinations or 10000 sources");
        var identities=new TreeSet<>(sources); identities.addAll(destinations);
        if (identities.isEmpty()) return Map.of();
        // SQL promotes collisions to the strongest mode before acquiring any key.
        em.createNativeQuery("SELECT lock_document_admission_keys(CAST(:reads AS bigint[]),CAST(:writes AS bigint[]))")
                .setParameter("reads",keys(sources)).setParameter("writes",keys(destinations)).getSingleResult();
        var ordered=List.copyOf(identities);
        Map<UUID,DocumentRecord> locked=new HashMap<>();
        identities.forEach(id -> locked.put(id,null));
        for (int start=0; start<ordered.size();) {
            boolean write=destinations.contains(ordered.get(start));
            int end=start+1;
            while (end<ordered.size() && end-start<256 && destinations.contains(ordered.get(end))==write) end++;
            String ids=ordered.subList(start,end).stream().map(UUID::toString).collect(Collectors.joining(",","{","}"));
            // Keep global Java UUID order across modes and batches. Existing
            // multi-document deletion takes that row order without advisory locks.
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
            }
            start=end;
        }
        return locked;
    }

    private static String keys(Set<UUID> identities) {
        return identities.stream().mapToLong(id -> id.getMostSignificantBits()^id.getLeastSignificantBits())
                .distinct().sorted().mapToObj(Long::toString).collect(Collectors.joining(",","{","}"));
    }
}
