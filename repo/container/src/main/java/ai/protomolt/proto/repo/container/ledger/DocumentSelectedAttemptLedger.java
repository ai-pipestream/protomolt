package ai.protomolt.proto.repo.container.ledger;

import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import jakarta.persistence.EntityManager;
import java.time.Duration;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Internal NEW_CONTENT lifecycle. Observations must come from trusted measured
 * provider read-back; this class performs no provider I/O or publication and does
 * not replace policy/schema admission. Displaced selections cannot record work.
 */
final class DocumentSelectedAttemptLedger {
    static final int MAX_OBSERVATIONS = 256;
    private final Tx tx;

    DocumentSelectedAttemptLedger(Tx tx) { this.tx = Objects.requireNonNull(tx); }

    record Selected(String member, long revision, UUID attempt, UUID token) {
        Selected {
            if (member == null || !member.matches("[a-zA-Z0-9_.-]{1,128}") || revision < 1)
                throw new IllegalArgumentException("Invalid document selection identity");
            Objects.requireNonNull(attempt); Objects.requireNonNull(token);
        }
        @Override public String toString() { return "Selected[member="+member+", revision="+revision+", attempt="+attempt+"]"; }
    }

    record Observation(String key, long size, String sha256, String contentType, String version, String etag) {
        Observation {
            text(key, 2048); text(contentType, 1024);
            if (size < 0 || sha256 == null || !sha256.matches("[0-9a-f]{64}"))
                throw new IllegalArgumentException("Invalid measured document content identity");
            if ((version != null && version.length() > 8192) || (etag != null && etag.length() > 8192))
                throw new IllegalArgumentException("Provider identity exceeds observation bound");
        }
    }

    /** One owner fence for at most 64 attempts, locked in deterministic UUID order. */
    List<DocumentPartAttemptLedger.Attempt> renew(RepositoryOperationLedger.Owner owner, List<Selected> selected, Duration lease) {
        Objects.requireNonNull(owner); Objects.requireNonNull(lease);
        if (lease.compareTo(Duration.ofSeconds(1)) < 0 || lease.compareTo(Duration.ofDays(1)) > 0)
            throw new IllegalArgumentException("Lease must be between one second and one day");
        if (selected.isEmpty() || selected.size() > 64)
            throw new IllegalArgumentException("Renewal requires one to 64 distinct selected attempts");
        // Canonical UUID text has PostgreSQL's unsigned UUID order, unlike UUID.compareTo.
        var ordered = List.copyOf(selected).stream().sorted(Comparator.comparing(selection -> selection.attempt().toString())).toList();
        if (ordered.isEmpty() || ordered.size() > 64
                || ordered.stream().map(Selected::attempt).distinct().count() != ordered.size())
            throw new IllegalArgumentException("Renewal requires one to 64 distinct selected attempts");
        var ids = ordered.stream().map(Selected::attempt).toList();
        String encoded = encodeSelections(ordered);
        return tx.inTransaction(em -> {
            RepositoryOperationLedger.fenceLiveOwner(em, owner);
            if (DocumentPartAttemptLedger.lockAll(em, ids).size() != ids.size()) throw conflict();
            requireSelections(em, owner, encoded, ids.size());
            int changed = em.createNativeQuery("""
                    UPDATE document_part_attempts SET lease_until=GREATEST(lease_until,clock_timestamp()+(:millis * interval '1 millisecond'))
                    WHERE attempt_id IN (:ids)
                    """).setParameter("millis", lease.toMillis())
                    .setParameter("ids", ids).executeUpdate();
            if (changed != ordered.size()) throw conflict();
            var result = DocumentPartAttemptLedger.lockAll(em, ids);
            requireSelections(em, owner, encoded, ids.size());
            requireOwner(em, owner);
            return result;
        });
    }

    /** Any missing key or identity mismatch rolls back the entire bounded batch. */
    DocumentPartAttemptLedger.Attempt verifyBatch(RepositoryOperationLedger.Owner owner, Selected selection,
            List<Observation> observations) {
        Objects.requireNonNull(owner); Objects.requireNonNull(selection);
        if (observations.isEmpty() || observations.size() > MAX_OBSERVATIONS)
            throw new IllegalArgumentException("Verification requires one to 256 observations");
        var rows = List.copyOf(observations);
        if (rows.isEmpty() || rows.size() > MAX_OBSERVATIONS)
            throw new IllegalArgumentException("Verification requires one to 256 observations");
        var keys = new HashSet<String>();
        for (var row : rows) if (!keys.add(row.key()))
            throw new IllegalArgumentException("Duplicate verification key");
        String encoded = encode(rows);
        return tx.inTransaction(em -> {
            RepositoryOperationLedger.fenceLiveOwner(em, owner);
            lockSelected(em, owner, selection);
            int changed = em.createNativeQuery("""
                    UPDATE document_part_attempt_objects p SET verified=true,provider_version=q.version,etag=q.etag
                    FROM jsonb_to_recordset(CAST(:rows AS jsonb)) q(key text,size bigint,sha256 text,content_type text,version text,etag text)
                    WHERE p.attempt_id=:id AND p.object_key=q.key AND p.expected_size=q.size
                      AND p.expected_sha256=q.sha256 AND p.content_type=q.content_type
                      AND (NOT p.verified OR (p.provider_version IS NOT DISTINCT FROM q.version AND p.etag IS NOT DISTINCT FROM q.etag))
                    """).setParameter("id", selection.attempt()).setParameter("rows", encoded).executeUpdate();
            if (changed != rows.size()) throw new DocumentPartAttemptLedger.FenceException("Observed document parts differ from admitted or verified identity");
            em.createNativeQuery("""
                    UPDATE document_part_attempts SET state='VERIFIED' WHERE attempt_id=:id AND state='STAGING'
                      AND NOT EXISTS(SELECT 1 FROM document_part_attempt_objects WHERE attempt_id=:id AND NOT verified)
                    """).setParameter("id", selection.attempt()).executeUpdate();
            var result = lockSelected(em, owner, selection);
            requireOwner(em, owner);
            return result;
        });
    }

    static DocumentPartAttemptLedger.Attempt lockSelected(EntityManager em,
            RepositoryOperationLedger.Owner owner, Selected selection) {
        var attempt = DocumentPartAttemptLedger.read(em, selection.attempt(), true).orElseThrow(DocumentSelectedAttemptLedger::conflict);
        if (!attempt.planKind().equals("NEW_CONTENT") || !attempt.token().equals(selection.token())
                || !(attempt.state().equals("STAGING") || attempt.state().equals("VERIFIED"))) throw conflict();
        boolean current = (Boolean) em.createNativeQuery("""
                SELECT EXISTS(SELECT 1 FROM document_operation_selection_current c
                    JOIN document_operation_selection_attempts h USING(account_id,principal,operation_id,owner_generation,member_id,selection_revision)
                    JOIN document_part_attempts a ON a.attempt_id=h.attempt_id
                    WHERE c.account_id=:account AND c.principal=:principal AND c.operation_id=:operation
                      AND c.owner_generation=:generation AND c.member_id=:member AND c.selection_revision=:revision
                      AND h.attempt_id=:attempt AND a.lease_until>clock_timestamp()
                      AND NOT EXISTS(SELECT 1 FROM document_part_attempt_cleanup WHERE attempt_id=a.attempt_id))
                """).setParameter("account", owner.key().account()).setParameter("principal", owner.key().principal())
                .setParameter("operation", owner.key().operationId()).setParameter("generation", owner.generation())
                .setParameter("member", selection.member()).setParameter("revision", selection.revision())
                .setParameter("attempt", selection.attempt()).getSingleResult();
        if (!current) throw conflict();
        return attempt;
    }

    private static void requireOwner(EntityManager em, RepositoryOperationLedger.Owner owner) {
        em.createNativeQuery("SELECT require_repository_operation_write_fence(:account,:principal,:operation,:generation)")
                .setParameter("account", owner.key().account()).setParameter("principal", owner.key().principal())
                .setParameter("operation", owner.key().operationId()).setParameter("generation", owner.generation()).getSingleResult();
    }

    private static void requireSelections(EntityManager em, RepositoryOperationLedger.Owner owner, String encoded, int count) {
        boolean current = (Boolean) em.createNativeQuery("""
                SELECT count(*)=:count FROM jsonb_to_recordset(CAST(:rows AS jsonb)) q(member text,revision bigint,attempt uuid,token uuid)
                JOIN document_operation_selection_current c ON c.member_id=q.member AND c.selection_revision=q.revision
                JOIN document_operation_selection_attempts h USING(account_id,principal,operation_id,owner_generation,member_id,selection_revision)
                JOIN document_part_attempts a ON a.attempt_id=h.attempt_id AND a.attempt_id=q.attempt AND a.lease_token=q.token
                WHERE c.account_id=:account AND c.principal=:principal AND c.operation_id=:operation AND c.owner_generation=:generation
                  AND a.plan_kind='NEW_CONTENT' AND a.state IN ('STAGING','VERIFIED') AND a.lease_until>clock_timestamp()
                  AND NOT EXISTS(SELECT 1 FROM document_part_attempt_cleanup WHERE attempt_id=a.attempt_id)
                """).setParameter("rows", encoded).setParameter("count", count)
                .setParameter("account", owner.key().account()).setParameter("principal", owner.key().principal())
                .setParameter("operation", owner.key().operationId()).setParameter("generation", owner.generation()).getSingleResult();
        if (!current) throw conflict();
    }

    private static String encodeSelections(List<Selected> selected) {
        var rows = ListValue.newBuilder();
        for (var selection : selected) rows.addValues(Value.newBuilder().setStructValue(Struct.newBuilder()
                .putFields("member", textValue(selection.member())).putFields("revision", textValue(Long.toString(selection.revision())))
                .putFields("attempt", textValue(selection.attempt().toString())).putFields("token", textValue(selection.token().toString()))));
        try { return JsonFormat.printer().omittingInsignificantWhitespace().print(rows); }
        catch (com.google.protobuf.InvalidProtocolBufferException invalid) {
            throw new IllegalArgumentException("Cannot encode selected attempts", invalid);
        }
    }

    private static String encode(List<Observation> rows) {
        var list = ListValue.newBuilder();
        for (var row : rows) {
            var value = Struct.newBuilder().putFields("key", textValue(row.key())).putFields("size", textValue(Long.toString(row.size())))
                    .putFields("sha256", textValue(row.sha256())).putFields("content_type", textValue(row.contentType()));
            if (row.version() != null) value.putFields("version", textValue(row.version()));
            if (row.etag() != null) value.putFields("etag", textValue(row.etag()));
            list.addValues(Value.newBuilder().setStructValue(value));
        }
        try { return JsonFormat.printer().omittingInsignificantWhitespace().print(list); }
        catch (com.google.protobuf.InvalidProtocolBufferException invalid) {
            throw new IllegalArgumentException("Cannot encode document verification batch", invalid);
        }
    }

    private static Value textValue(String value) { return Value.newBuilder().setStringValue(value).build(); }
    private static void text(String value, int max) {
        if (value == null || value.isBlank() || value.length() > max) throw new IllegalArgumentException("Invalid observation text");
    }
    private static DocumentPartAttemptLedger.FenceException conflict() {
        return new DocumentPartAttemptLedger.FenceException("Document attempt is not the exact live selection or belongs to another worker");
    }
}
