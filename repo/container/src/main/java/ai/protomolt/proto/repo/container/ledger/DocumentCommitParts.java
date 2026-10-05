package ai.protomolt.proto.repo.container.ledger;

import jakarta.persistence.EntityManager;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/** Physical identity resolution for a locked native commit; no provider I/O or content decoding. */
final class DocumentCommitParts {
    private DocumentCommitParts() {}

    record Slot(String member, int ordinal) {}
    record Physical(UUID id, int part, String subKey, String key, long size, String sha256,
                    String contentType, String version, String etag, String generation, String realm, String namespace) {}
    record Bound(Map<Slot,Physical> parts, Map<String,Long> selections) {
        Bound { parts=Map.copyOf(parts); selections=Map.copyOf(selections); }
    }

    static Bound bind(EntityManager em, RepositoryOperationLedger.Owner owner, DocumentUploadPlan.Prepared plan,
            Map<String,DocumentSelectedAttemptLedger.Selected> selected, DocumentReuseAdmission.Prepared reuse, Runnable control) {
        return bind(em, owner, plan, selected, reuse, control, false);
    }

    /**
     * Retain only the proposed candidate, without touching old destination objects.
     * Caller holds operation, policy, logical/source, drive and assessment-owner
     * locks, in that order. No publication or admission authority is granted here.
     */
    static Bound bindAssessment(EntityManager em, RepositoryOperationLedger.Owner owner, DocumentUploadPlan.Prepared plan,
            Map<String,DocumentSelectedAttemptLedger.Selected> selected, DocumentReuseAdmission.Prepared reuse, Runnable control) {
        return bind(em, owner, plan, selected, reuse, control, true);
    }

    private static Bound bind(EntityManager em, RepositoryOperationLedger.Owner owner, DocumentUploadPlan.Prepared plan,
            Map<String,DocumentSelectedAttemptLedger.Selected> selected, DocumentReuseAdmission.Prepared reuse,
            Runnable control, boolean assessment) {
        control.run();
        selected = Map.copyOf(selected);
        var uploadingMembers = plan.members().stream().filter(member -> member.attempt().isPresent())
                .map(member -> member.intent().getMemberId()).collect(Collectors.toSet());
        if (!selected.keySet().equals(uploadingMembers)) throw conflict();
        var selectionRevisions=selections(em,owner,plan,selected);
        Set<UUID> attempts=selected.values().stream().map(DocumentSelectedAttemptLedger.Selected::attempt).collect(Collectors.toSet());
        var fresh=new HashMap<UUID,Map<Integer,UUID>>();
        if (!attempts.isEmpty()) {
            var rows=em.createNativeQuery("""
                    SELECT attempt_id,revision_ordinal,physical_object_id FROM document_part_attempt_objects
                    WHERE attempt_id=ANY(CAST(:attempts AS uuid[])) LIMIT 10001
                    """).setParameter("attempts",ids(attempts)).getResultList();
            if (rows.size()>10000) throw conflict();
            for (Object value:rows) {
                control.run();
                Object[] row=(Object[])value;
                if (row[2]==null || fresh.computeIfAbsent((UUID)row[0],ignored -> new HashMap<>())
                        .put(((Number)row[1]).intValue(),(UUID)row[2])!=null) throw conflict();
            }
        }
        var destinations=new HashSet<UUID>();
        var claims=new HashMap<Slot,UUID>();
        for (var member:plan.members()) {
            destinations.add(member.nodeId());
            String id=member.intent().getMemberId();
            int uploads=0;
            for (int i=0;i<member.intent().getPartsCount();i++) {
                control.run();
                var part=member.intent().getParts(i);
                if (part.hasEmpty()) continue;
                UUID object;
                if (part.hasReuse()) object=UUID.fromString(part.getReuse().getObject().getObjectId());
                else {
                    uploads++;
                    object=fresh.getOrDefault(selected.get(id).attempt(),Map.of()).get(i);
                    if (object==null) throw conflict();
                }
                claims.put(new Slot(id,i),object);
            }
            if (uploads>0 && fresh.getOrDefault(selected.get(id).attempt(),Map.of()).size()!=uploads) throw conflict();
        }
        var objects=Set.copyOf(claims.values());
        control.run();
        DocumentPublicationLocks.IndependentOrigins locks = null;
        if (assessment) {
            // V65 locks every source before any retention row. Unlike publication,
            // assessment does not replace current pointers or release old bytes.
            em.createNativeQuery("SELECT lock_repository_retention_set(CAST(:objects AS uuid[]))")
                    .setParameter("objects", ids(objects)).getSingleResult();
        } else {
            locks=DocumentPublicationLocks.lockIndependentOrigins(em,destinations,objects,attempts);
            DocumentPublicationLocks.lockIndependentRetention(em,locks);
        }
        // All origin rows were acquired in global order, including fresh attempts.
        for (var selection:selected.values())
            if (!DocumentSelectedAttemptLedger.lockSelected(em,owner,selection).state().equals("VERIFIED")) throw conflict();
        DocumentReuseAdmission.requireBoundSources(em,reuse);
        control.run();
        var physical=read(em,owner.key().account(),objects);
        var expectedUploads=new HashMap<Slot,DocumentPartAttemptLedger.PlannedObject>();
        for (var member:plan.members()) member.attempt().ifPresent(attempt -> {
            for (var upload:attempt.uploads()) expectedUploads.put(new Slot(member.intent().getMemberId(),upload.revisionOrdinal()),upload.object());
        });
        var parts=new HashMap<Slot,Physical>();
        for (var member:plan.members()) for (int i=0;i<member.intent().getPartsCount();i++) {
            control.run();
            var declaration=member.intent().getParts(i);
            if (declaration.hasEmpty()) continue;
            var slot=new Slot(member.intent().getMemberId(),i);
            var actual=physical.get(claims.get(slot));
            if (actual==null || actual.part()!=declaration.getSlot().getPartValue()
                    || !actual.subKey().equals(declaration.getSlot().getSubKey())) throw conflict();
            if (declaration.hasUpload()) {
                var expected=expectedUploads.get(slot);
                if (!actual.key().equals(expected.objectKey()) || actual.size()!=expected.size()
                        || !actual.sha256().equals(expected.sha256()) || !actual.contentType().equals(expected.contentType())
                        || !actual.generation().equals(member.placement().generation())
                        || !actual.realm().equals(member.placement().profile().storageRealm())
                        || !actual.namespace().equals(member.placement().drive().namespace())) throw conflict();
            }
            parts.put(slot,actual);
        }
        if (locks != null) locks.requirePlan(destinations,objects,attempts);
        return new Bound(parts,selectionRevisions);
    }

    private static Map<String,Long> selections(EntityManager em, RepositoryOperationLedger.Owner owner,
            DocumentUploadPlan.Prepared plan, Map<String,DocumentSelectedAttemptLedger.Selected> selected) {
        var rows=em.createNativeQuery("""
                SELECT a.member_id,a.node_id,a.sampled_revision,a.drive_id,a.backend_generation,a.storage_realm,
                    a.storage_namespace,a.upload_count,h.selection_revision,h.attempt_id,
                    a.drive_sha256=document_operation_drive_digest_v1(d)
                FROM document_operation_selections a JOIN document_operation_selection_current c
                    USING(account_id,principal,operation_id,owner_generation,member_id)
                JOIN document_operation_selection_attempts h
                    USING(account_id,principal,operation_id,owner_generation,member_id,selection_revision)
                JOIN drives d ON d.drive_id=a.drive_id
                WHERE a.account_id=:account AND a.principal=:principal AND a.operation_id=:operation AND a.owner_generation=:generation
                LIMIT 65
                """).setParameter("account",owner.key().account()).setParameter("principal",owner.key().principal())
                .setParameter("operation",owner.key().operationId()).setParameter("generation",owner.generation()).getResultList();
        if (rows.size()!=plan.members().size()) throw conflict();
        var byId=new HashMap<String,Object[]>();
        for (Object value:rows) { var row=(Object[])value; if (byId.put((String)row[0],row)!=null) throw conflict(); }
        var result=new HashMap<String,Long>();
        for (var member:plan.members()) {
            var row=byId.get(member.intent().getMemberId());
            var selection=selected.get(member.intent().getMemberId());
            int uploads=member.attempt().map(a -> a.uploads().size()).orElse(0);
            if (row==null || !member.nodeId().equals(row[1])
                    || member.intent().getDestination().getExpectedMutationRevision()!=((Number)row[2]).longValue()
                    || !member.placement().drive().id().equals(row[3]) || !member.placement().generation().equals(row[4])
                    || !member.placement().profile().storageRealm().equals(row[5])
                    || !member.placement().drive().namespace().equals(row[6]) || uploads!=((Number)row[7]).intValue()
                    || !Boolean.TRUE.equals(row[10])) throw conflict();
            long revision=((Number)row[8]).longValue();
            if (uploads==0 ? row[9]!=null || selection!=null : selection==null
                    || !selection.member().equals(member.intent().getMemberId()) || selection.revision()!=revision
                    || !selection.attempt().equals(row[9]) || !member.attempt().orElseThrow().id().equals(row[9])) throw conflict();
            result.put(member.intent().getMemberId(),revision);
        }
        return result;
    }

    private static Map<UUID,Physical> read(EntityManager em,String account,Set<UUID> ids) {
        var rows=em.createNativeQuery("""
                SELECT l.object_id,o.part,o.sub_key,l.object_key,o.expected_size,o.expected_sha256,o.content_type,
                    o.provider_version,o.etag,l.backend_generation,l.storage_realm,l.storage_namespace
                FROM repository_physical_locations l JOIN document_part_attempt_objects o
                    ON o.attempt_id=l.source_id AND o.ordinal=l.source_ordinal AND o.physical_object_id=l.object_id
                JOIN document_part_attempts a ON a.attempt_id=o.attempt_id
                JOIN repository_object_retention r USING(object_id)
                WHERE l.object_id=ANY(CAST(:ids AS uuid[])) AND l.source_kind='DOCUMENT_PART' AND a.account_id=:account
                    AND a.state='VERIFIED' AND o.verified AND NOT r.retiring AND NOT r.reclaiming
                    AND o.object_key=l.object_key AND o.storage_realm=l.storage_realm AND o.storage_namespace=l.storage_namespace
                    AND a.backend_generation=l.backend_generation AND a.storage_realm=l.storage_realm
                    AND NOT EXISTS(SELECT 1 FROM document_part_attempt_cleanup c WHERE c.attempt_id=a.attempt_id)
                """).setParameter("ids",ids(ids)).setParameter("account",account).getResultList();
        if (rows.size()!=ids.size()) throw conflict();
        var result=new HashMap<UUID,Physical>();
        for (Object value:rows) {
            var r=(Object[])value;
            var part=new Physical((UUID)r[0],((Number)r[1]).intValue(),(String)r[2],(String)r[3],((Number)r[4]).longValue(),
                    (String)r[5],(String)r[6],(String)r[7],(String)r[8],(String)r[9],(String)r[10],(String)r[11]);
            if (result.put(part.id(),part)!=null) throw conflict();
        }
        return result;
    }

    private static String ids(Collection<UUID> ids) { return ids.stream().map(UUID::toString).collect(Collectors.joining(",","{","}")); }
    private static DocumentPartAttemptLedger.FenceException conflict() {
        return new DocumentPartAttemptLedger.FenceException("Native publication differs from selected or retained physical evidence");
    }
}
