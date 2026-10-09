package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.container.blob.DocumentIds;
import jakarta.persistence.EntityManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CancellationException;

/**
 * Retains an already checked member's complete schema evidence in its native
 * revision transaction. This storage primitive grants neither admission nor
 * authorization; it does not relax the policy guards or enable typed publication.
 */
final class DocumentSchemaRetention {
    private record Fragment(int ordinal, int part, String subKey, long size, String sha256) {}
    private final DocumentSchemaBatch batch;
    private final DocumentSchemaAdmission.Proof proof;
    private final List<Fragment> fragments;
    private final List<String> artifacts;
    private final List<DocumentSchemaAdmission.Reference> references;
    private final List<DocumentSchemaAdmission.RootEvidence> roots;

    private DocumentSchemaRetention(DocumentSchemaBatch batch, DocumentSchemaAdmission.Proof proof) {
        this.batch = batch;
        this.proof = proof;
        var member = proof.member();
        fragments = proof.fragments().keySet().stream().sorted().map(ordinal -> {
            var part = member.getParts(ordinal);
            var object = part.hasHistoricalReuse() ? part.getHistoricalReuse().getObject() : part.getReuse().getObject();
            long size = part.hasUpload() ? part.getUpload().getSizeBytes() : object.getSizeBytes();
            String sha = part.hasUpload() ? part.getUpload().getSha256() : object.getSha256();
            return new Fragment(ordinal, part.getSlot().getPartValue(), part.getSlot().getSubKey(), size, sha);
        }).toList();
        artifacts = proof.artifacts().keySet().stream().sorted().toList();
        references = proof.references().stream().sorted(Comparator.comparing(DocumentSchemaAdmission.Reference::typeUrl)
                .thenComparing(DocumentSchemaAdmission.Reference::descriptorSha256)).toList();
        roots = proof.roots().stream().sorted(Comparator.comparingInt(DocumentSchemaAdmission.RootEvidence::ordinal)
                .thenComparing(DocumentSchemaAdmission.RootEvidence::locatorSha256)).toList();
    }

    /** All descriptor resolution, validation and raw-byte hashing already finished outside the SQL transaction. */
    static DocumentSchemaRetention prepare(DocumentSchemaBatch batch, String memberId) {
        Objects.requireNonNull(batch); Objects.requireNonNull(memberId);
        var proof = batch.proofs().get(memberId);
        if (proof == null) throw new IllegalArgumentException("Schema retention requires a checked member proof");
        return new DocumentSchemaRetention(batch, proof);
    }

    /**
     * Caller holds owner, policy, physical publication and complete batch artifact
     * locks, in that order. Invoke after revision parts exist and before sealing.
     * Failure poisons the transaction even if an internal caller catches it.
     */
    void write(EntityManager em, RepositoryOperationLedger.Owner owner, UUID revision, Runnable control) {
        Objects.requireNonNull(em); Objects.requireNonNull(owner); Objects.requireNonNull(revision);
        if (!em.getTransaction().isActive() || em.getTransaction().getRollbackOnly())
            throw new IllegalStateException("Schema retention requires an active writable transaction");
        try {
            active(control);
            if (!owner.key().account().equals(batch.command().intent().getAccountId())
                    || !owner.key().operationId().equals(batch.command().operationId()))
                throw new IllegalArgumentException("Schema retention differs from operation scope");
            em.createNativeQuery("SELECT require_repository_operation_write_fence(:account,:principal,:operation,:generation)")
                    .setParameter("account", owner.key().account()).setParameter("principal", owner.key().principal())
                    .setParameter("operation", owner.key().operationId()).setParameter("generation", owner.generation()).getSingleResult();
            RepositoryOperationLedger.requireCommand(em, owner.key(), batch.command());
            requireRevision(em, owner, revision);
            requireFragments(em, revision, control);
            em.unwrap(org.hibernate.Session.class).doWork(connection -> {
                try (var statement = connection.prepareStatement("""
                        INSERT INTO document_revision_schema_artifacts
                        (revision_id,account_id,principal,operation_id,owner_generation,artifact_sha256)
                        VALUES(?,?,?,?,?,?)
                        """)) {
                    execute(statement, artifacts, (s, hash) -> { bindOwner(s, owner, revision); s.setBytes(6, hex(hash)); }, control);
                }
                try (var statement = connection.prepareStatement("""
                        INSERT INTO document_revision_schema_assets
                        (revision_id,account_id,principal,operation_id,owner_generation,type_url,type_url_sha256,
                         descriptor_sha256,metadata_sha256,metadata_codec,metadata_version,source_sha256)
                        VALUES(?,?,?,?,?,?,sha256(convert_to(?,'UTF8')),?,?,?,?,?)
                        """)) {
                    execute(statement, references, (s, reference) -> {
                        bindOwner(s, owner, revision);
                        s.setString(6, reference.typeUrl()); s.setString(7, reference.typeUrl());
                        s.setBytes(8, hex(reference.descriptorSha256())); s.setBytes(9, hex(reference.metadataSha256()));
                        s.setString(10, reference.metadataCodec()); s.setInt(11, reference.metadataVersion());
                        s.setBytes(12, reference.sourceSha256().map(DocumentSchemaRetention::hex).orElse(null));
                    }, control);
                }
                try (var statement = connection.prepareStatement("""
                        INSERT INTO document_revision_schema_evidence
                        (revision_id,account_id,principal,operation_id,owner_generation,revision_ordinal,
                         root_locator_sha256,fragment_sha256,fragment_size,evidence_codec,evidence_version,evidence_bytes,evidence_sha256)
                        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)
                        """)) {
                    var byOrdinal = fragments.stream().collect(java.util.stream.Collectors.toMap(Fragment::ordinal, value -> value));
                    execute(statement, roots, (s, root) -> {
                        bindOwner(s, owner, revision);
                        var fragment = byOrdinal.get(root.ordinal());
                        s.setInt(6, root.ordinal()); s.setBytes(7, hex(root.locatorSha256()));
                        s.setBytes(8, hex(fragment.sha256())); s.setLong(9, fragment.size());
                        s.setString(10, root.encoded().codec()); s.setInt(11, root.encoded().version());
                        s.setBytes(12, root.encoded().bytes().toByteArray()); s.setBytes(13, hex(root.encoded().sha256()));
                    }, control);
                }
            });
            active(control);
        } catch (RuntimeException | Error failure) {
            try { em.getTransaction().setRollbackOnly(); }
            catch (RuntimeException rollbackFailure) { failure.addSuppressed(rollbackFailure); }
            throw failure;
        }
    }

    private void requireRevision(EntityManager em, RepositoryOperationLedger.Owner owner, UUID revision) {
        var rows = em.createNativeQuery("""
                SELECT c.member_id,c.node_id FROM document_revision_commits c
                JOIN document_revision_publications r USING(revision_id)
                WHERE c.revision_id=:revision AND c.account_id=:account AND c.principal=:principal
                 AND c.operation_id=:operation AND c.owner_generation=:generation
                 AND c.creation_xid=pg_current_xact_id() AND r.projection_xid=pg_current_xact_id()
                 AND r.native_binding=c.revision_id AND NOT r.projection_sealed
                """).setParameter("revision", revision).setParameter("account", owner.key().account())
                .setParameter("principal", owner.key().principal()).setParameter("operation", owner.key().operationId())
                .setParameter("generation", owner.generation()).getResultList();
        if (rows.size() != 1) throw new IllegalArgumentException("Schema retention requires the current unsealed member revision");
        var row = (Object[]) rows.getFirst();
        if (!proof.member().getMemberId().equals(row[0])
                || !DocumentIds.nodeId(proof.member().getDestination().getAddress()).equals(row[1]))
            throw new IllegalArgumentException("Schema proof differs from revision member identity");
    }

    /** Verify every nonempty fragment, including parts that contain no Any root. */
    private void requireFragments(EntityManager em, UUID revision, Runnable control) {
        var rows = em.createNativeQuery("""
                SELECT p.revision_ordinal,p.part,p.sub_key,o.expected_size,o.expected_sha256,
                 a.account_id=:account AND a.state='VERIFIED' AND o.verified
                 AND o.part=p.part AND o.sub_key=p.sub_key
                 AND a.backend_generation=l.backend_generation AND a.storage_realm=l.storage_realm
                 AND a.storage_namespace=l.storage_namespace AND o.storage_realm=l.storage_realm
                 AND o.storage_namespace=l.storage_namespace AND o.object_key=l.object_key AS qualified
                FROM document_revision_parts p LEFT JOIN repository_physical_locations l
                 ON l.object_id=p.object_id AND l.source_kind='DOCUMENT_PART'
                LEFT JOIN document_part_attempt_objects o ON o.physical_object_id=l.object_id
                 AND o.attempt_id=l.source_id AND o.ordinal=l.source_ordinal
                LEFT JOIN document_part_attempts a ON a.attempt_id=o.attempt_id
                WHERE p.revision_id=:revision
                ORDER BY p.revision_ordinal
                """).setParameter("revision", revision).setParameter("account", batch.command().intent().getAccountId()).getResultList();
        if (rows.size() != fragments.size()) throw new IllegalArgumentException("Schema proof fragment set differs from revision parts");
        for (int i = 0; i < fragments.size(); i++) {
            active(control);
            var expected = fragments.get(i); var row = (Object[]) rows.get(i);
            if (!Boolean.TRUE.equals(row[5]) || expected.ordinal() != ((Number) row[0]).intValue() || expected.part() != ((Number) row[1]).intValue()
                    || !expected.subKey().equals(row[2]) || expected.size() != ((Number) row[3]).longValue()
                    || !expected.sha256().equals(row[4]))
                throw new IllegalArgumentException("Schema proof fragment identity differs from selected physical bytes");
        }
    }

    private static void bindOwner(PreparedStatement statement, RepositoryOperationLedger.Owner owner, UUID revision) throws SQLException {
        statement.setObject(1, revision); statement.setString(2, owner.key().account()); statement.setString(3, owner.key().principal());
        statement.setObject(4, owner.key().operationId()); statement.setLong(5, owner.generation());
    }
    @FunctionalInterface private interface Binder<T> { void bind(PreparedStatement statement, T value) throws SQLException; }
    private static <T> void execute(PreparedStatement statement, List<T> values, Binder<T> binder, Runnable control) throws SQLException {
        int pending = 0;
        for (var value : values) {
            active(control); binder.bind(statement, value); statement.addBatch(); pending++;
            if (pending == 16) { flush(statement, pending, control); pending = 0; }
        }
        if (pending > 0) flush(statement, pending, control);
    }
    private static void flush(PreparedStatement statement, int expected, Runnable control) throws SQLException {
        active(control);
        var counts = statement.executeBatch();
        if (counts.length != expected) throw new SQLException("Incomplete schema retention batch result");
        for (int count : counts) if (count != 1 && count != Statement.SUCCESS_NO_INFO)
            throw new SQLException("Schema retention insert did not succeed");
        statement.clearBatch(); active(control);
    }
    private static byte[] hex(String hash) { return HexFormat.of().parseHex(hash); }
    private static void active(Runnable control) {
        Objects.requireNonNull(control);
        if (Thread.currentThread().isInterrupted()) throw new CancellationException("Schema retention interrupted");
        control.run();
    }
}
