package ai.protomolt.proto.repo.container.ledger;

import jakarta.persistence.EntityManager;
import java.util.ArrayList;
import java.util.List;

/** Same-owner, verified initial selection reuse. Never adopts partial uploads or performs provider I/O. */
final class DocumentUploadAdmissionReplay {
    private DocumentUploadAdmissionReplay() {}

    static boolean hasSelections(EntityManager em, RepositoryOperationLedger.Owner owner) {
        return (Boolean) em.createNativeQuery("""
                SELECT EXISTS(SELECT 1 FROM document_operation_selections
                  WHERE account_id=:a AND principal=:p AND operation_id=:o AND owner_generation=:g)
                """).setParameter("a", owner.key().account()).setParameter("p", owner.key().principal())
                .setParameter("o", owner.key().operationId()).setParameter("g", owner.generation()).getSingleResult();
    }

    /** Caller holds the owner, authorization and placement locks and has compared every initial selection. */
    static List<DocumentPartAttemptLedger.Attempt> requireVerified(EntityManager em, RepositoryOperationLedger.Owner owner,
            List<DocumentOperationUploadAdmission.EncodedMember> uploads) {
        var ids = uploads.stream().map(u -> u.member().attempt().orElseThrow().id()).toList();
        if (!ids.isEmpty() && DocumentPartAttemptLedger.lockAll(em, ids).size() != ids.size()) throw conflict();
        var result = new ArrayList<DocumentPartAttemptLedger.Attempt>();
        for (var upload : uploads) {
            var member = upload.member();
            var plan = member.attempt().orElseThrow();
            var selection = new DocumentSelectedAttemptLedger.Selected(member.intent().getMemberId(), 1, plan.id(), upload.token());
            var actual = DocumentSelectedAttemptLedger.lockSelected(em, owner, selection);
            if (!actual.state().equals("VERIFIED") || !actual.location().equals(plan.location())
                    || !actual.storageRealm().equals(member.placement().profile().storageRealm())
                    || actual.sampledRevision() != member.intent().getDestination().getExpectedMutationRevision()
                    || actual.plannedCount() != plan.uploads().size()) throw conflict();
            boolean identity = (Boolean) em.createNativeQuery("""
                    SELECT EXISTS(SELECT 1 FROM document_part_attempts a WHERE a.attempt_id=:id
                      AND a.operation_principal=:p AND a.operation_id=:o AND a.operation_generation=:g
                      AND a.member_id=:member AND a.drive_id=:drive AND a.source_count=:sources
                      AND (SELECT count(*) FROM document_part_attempt_objects WHERE attempt_id=a.attempt_id)=a.planned_count
                      AND (SELECT count(*) FROM document_part_attempt_sources WHERE attempt_id=a.attempt_id)=a.source_count)
                    """).setParameter("id", plan.id()).setParameter("p", owner.key().principal())
                    .setParameter("o", owner.key().operationId()).setParameter("g", owner.generation())
                    .setParameter("member", member.intent().getMemberId()).setParameter("drive", member.placement().drive().id())
                    .setParameter("sources", member.sources().size()).getSingleResult();
            if (!identity) throw conflict();
            for (String batch : upload.encoded().objects()) {
                boolean matches = (Boolean) em.createNativeQuery("""
                        WITH expected AS (SELECT * FROM jsonb_to_recordset(CAST(:rows AS jsonb)) q(
                          ordinal integer,revision_ordinal integer,part integer,sub_key text,object_key text,
                          expected_size bigint,expected_sha256 text,content_type text))
                        SELECT NOT EXISTS(SELECT 1 FROM expected q LEFT JOIN document_part_attempt_objects p
                          ON p.attempt_id=:id AND p.ordinal=q.ordinal WHERE p.attempt_id IS NULL
                          OR ROW(p.revision_ordinal,p.part,p.sub_key,p.object_key,p.expected_size,p.expected_sha256,p.content_type)
                             IS DISTINCT FROM ROW(q.revision_ordinal,q.part,q.sub_key,q.object_key,q.expected_size,q.expected_sha256,q.content_type)
                          OR p.storage_realm<>:realm OR p.storage_namespace<>:namespace OR NOT p.verified)
                        """).setParameter("rows", batch).setParameter("id", plan.id())
                        .setParameter("realm", actual.storageRealm()).setParameter("namespace", actual.location().namespace())
                        .getSingleResult();
                if (!matches) throw conflict();
            }
            for (String batch : upload.encoded().sources()) {
                boolean matches = (Boolean) em.createNativeQuery("""
                        WITH expected AS (SELECT * FROM jsonb_to_recordset(CAST(:rows AS jsonb)) q(source_node_id uuid,revision bigint))
                        SELECT NOT EXISTS(SELECT 1 FROM expected q LEFT JOIN document_part_attempt_sources s
                          ON s.attempt_id=:id AND s.source_node_id=q.source_node_id
                          WHERE s.attempt_id IS NULL OR s.revision<>q.revision)
                        """).setParameter("rows", batch).setParameter("id", plan.id()).getSingleResult();
                if (!matches) throw conflict();
            }
            result.add(actual);
        }
        // Repeat DB-time checks after all plan comparisons, without extending an expired lease.
        for (var upload : uploads) DocumentSelectedAttemptLedger.lockSelected(em, owner,
                new DocumentSelectedAttemptLedger.Selected(upload.member().intent().getMemberId(), 1,
                        upload.member().attempt().orElseThrow().id(), upload.token()));
        RepositoryOperationLedger.fenceLiveOwner(em, owner);
        return List.copyOf(result);
    }

    private static DocumentPartAttemptLedger.FenceException conflict() {
        return new DocumentPartAttemptLedger.FenceException("Initial upload is not an exact live verified retry");
    }
}
