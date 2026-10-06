package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.container.blob.DocumentIds;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.v1.NodeAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Internal diagnostic snapshot, never a pruning authorization or a retained handle.
 * One SQL statement observes metadata without loading payloads or schema artifacts.
 * Counts include durable claims even when their owner is expired or terminal: this
 * reader cannot infer quiescence or release. Results may be stale immediately.
 */
final class DocumentRevisionRetentionInventory {
    enum UnresolvedReachability { PREPARATION_JOURNAL_SELECTORS, FUTURE_CONTENT_REPOSITORY_REFERENCES }
    record ObjectReferences(UUID object, boolean reclaiming, boolean retiring, long historicalRevisions, long currentRevisions,
            long archiveVersions, long documentReaders, long archiveReaders, long assessments, long mirrors) {}
    record ArtifactReferences(String sha256, long revisions, long operationClaims, long assessments) {}
    record Snapshot(UUID revision, boolean current, long sourceReadPins, long sourceAssessmentSlots,
            List<ObjectReferences> objects, List<ArtifactReferences> artifacts,
            List<UnresolvedReachability> unresolved) {
        Snapshot { objects = List.copyOf(objects); artifacts = List.copyOf(artifacts); unresolved = List.copyOf(unresolved); }
    }
    private final Tx tx;
    DocumentRevisionRetentionInventory(Tx tx) { this.tx = Objects.requireNonNull(tx); }

    Snapshot inspect(RepositoryCaller caller, NodeAddress address, UUID revision, RepositoryReadControl control) {
        Objects.requireNonNull(caller); Objects.requireNonNull(address); Objects.requireNonNull(revision);
        Objects.requireNonNull(control).check();
        // Global sharing counts are administrative diagnostics, not a scoped document read API.
        if (!caller.processAuthority()) throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED,
                "Revision retention inventory requires process authority");
        var rows = tx.readOnly(em -> em.createNativeQuery(SQL).setParameter("revision", revision)
                .setParameter("node", DocumentIds.nodeId(address))
                .setParameter("objectLimit", DocumentPublicationCommand.MAX_PARTS + 1).getResultList());
        control.check();
        if (rows.isEmpty()) throw new RepositoryException(RepositoryException.Code.NOT_FOUND, "Revision not found");
        boolean current = false, header = false;
        long readers = 0, slots = 0;
        var objects = new ArrayList<ObjectReferences>();
        var artifacts = new ArrayList<ArtifactReferences>();
        for (Object value : rows) {
            control.check();
            var row = (Object[]) value;
            switch ((String) row[0]) {
                case "REVISION" -> { header = true; current = (Boolean) row[2]; readers = count(row, 4); slots = count(row, 5); }
                case "OBJECT" -> {
                    if (row[3] == null) throw new RepositoryException(RepositoryException.Code.DATA_LOSS,
                            "Published object has no retention record");
                    objects.add(new ObjectReferences(UUID.fromString((String) row[1]), (Boolean) row[3], (Boolean) row[11],
                            count(row, 4), count(row, 5), count(row, 6), count(row, 7), count(row, 8), count(row, 9), count(row, 10)));
                }
                case "ARTIFACT" -> artifacts.add(new ArtifactReferences((String) row[1], count(row, 4), count(row, 6), count(row, 9)));
                default -> throw new IllegalStateException("Unknown retention inventory row");
            }
        }
        if (!header) throw new RepositoryException(RepositoryException.Code.DATA_LOSS, "Revision inventory header missing");
        if (objects.size() > DocumentPublicationCommand.MAX_PARTS || artifacts.size() > 64)
            throw new RepositoryException(RepositoryException.Code.RESOURCE_EXHAUSTED, "Revision inventory exceeds bounds");
        return new Snapshot(revision, current, readers, slots, objects, artifacts, List.of(UnresolvedReachability.values()));
    }

    private static long count(Object[] row, int column) { return ((Number) row[column]).longValue(); }

    private static final String SQL = """
            WITH revision AS MATERIALIZED (
              SELECT revision_id,node_id FROM document_revision_publications
              WHERE revision_id=:revision AND node_id=:node AND projection_sealed
            ), objects AS MATERIALIZED (
              SELECT DISTINCT p.object_id FROM document_revision_parts p JOIN revision USING(revision_id)
              ORDER BY p.object_id LIMIT :objectLimit
            ), artifacts AS MATERIALIZED (
              SELECT a.account_id,a.artifact_sha256 FROM document_revision_schema_artifacts a JOIN revision USING(revision_id)
              ORDER BY a.artifact_sha256 LIMIT 65
            )
            SELECT 'REVISION' AS kind,r.revision_id::text AS identity,
              EXISTS(SELECT 1 FROM document_revision_current c WHERE c.revision_id=r.revision_id) AS current_revision,
              false AS reclaiming,
              (SELECT count(*) FROM document_read_pins p WHERE p.source_revision=r.revision_id) AS history_count,
              (SELECT count(*) FROM document_assessment_slots s WHERE s.source_revision=r.revision_id) AS current_count,
              0::bigint AS archive_count,0::bigint AS document_readers,0::bigint AS archive_readers,
              0::bigint AS assessment_count,0::bigint AS mirrors,false AS retiring
            FROM revision r
            UNION ALL
            SELECT 'OBJECT',o.object_id::text,false,t.reclaiming,
              (SELECT count(DISTINCT p.revision_id) FROM document_revision_parts p
               JOIN document_revision_publications r USING(revision_id) WHERE p.object_id=o.object_id AND r.projection_sealed),
              (SELECT count(DISTINCT c.node_id) FROM document_revision_parts p
               JOIN document_revision_current c USING(revision_id) WHERE p.object_id=o.object_id),
              (SELECT count(*) FROM archive_version_object_refs a WHERE a.object_id=o.object_id),
              (SELECT count(*) FROM document_read_pins p WHERE p.object_id=o.object_id),
              (SELECT count(*) FROM archive_read_pins p WHERE p.object_id=o.object_id),
              (SELECT count(*) FROM document_assessment_objects a WHERE a.object_id=o.object_id),
              (SELECT count(*) FROM repository_object_references r WHERE r.object_id=o.object_id),t.retiring
            FROM objects o LEFT JOIN repository_object_retention t USING(object_id)
            UNION ALL
            SELECT 'ARTIFACT',encode(a.artifact_sha256,'hex'),false,false,
              (SELECT count(*) FROM document_revision_schema_artifacts r WHERE r.account_id=a.account_id AND r.artifact_sha256=a.artifact_sha256),
              0::bigint,
              (SELECT count(*) FROM repository_schema_artifact_claims c WHERE c.account_id=a.account_id AND c.artifact_sha256=a.artifact_sha256),
              0::bigint,0::bigint,
              (SELECT count(*) FROM document_assessment_artifacts s WHERE s.account_id=a.account_id AND s.artifact_sha256=a.artifact_sha256),
              0::bigint,false
            FROM artifacts a
            ORDER BY kind,identity
            """;
}
