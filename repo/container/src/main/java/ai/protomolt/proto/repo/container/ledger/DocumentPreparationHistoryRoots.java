package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.container.blob.DocumentIds;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.TreeSet;

/** Derived revision liveness, never source authorization or a restored execution capability. */
final class DocumentPreparationHistoryRoots {
    private DocumentPreparationHistoryRoots() {}
    record Root(String node, String revision) {}
    enum Coverage { UNKNOWN, EXACT }

    /** Reconcile the SQL projection against decoded canonical intent before relying on its liveness set. */
    static Coverage coverage(EntityManager em, DocumentPublicationPreparationRecord record, byte[] preparationDigest) {
        var rows = scope(em.createNativeQuery("""
                SELECT preparation_sha256,command_sha256,expected_count,roots_sha256,sealed
                FROM repository_preparation_history_sets
                WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:g
                """), record).getResultList();
        if (rows.isEmpty()) return Coverage.UNKNOWN;
        Object[] row = (Object[]) rows.getFirst();
        var expected = roots(record.command());
        if (!MessageDigest.isEqual((byte[]) row[0], preparationDigest)
                || !MessageDigest.isEqual((byte[]) row[1], HexFormat.of().parseHex(record.command().sha256()))
                || ((Number) row[2]).intValue() != expected.size()
                || !MessageDigest.isEqual((byte[]) row[3], digest(expected))
                || !Boolean.TRUE.equals(row[4])) {
            throw new ai.protomolt.proto.repo.spi.RepositoryException(
                    ai.protomolt.proto.repo.spi.RepositoryException.Code.DATA_LOSS,
                    "Preparation history projection differs from its canonical command");
        }
        return Coverage.EXACT;
    }

    static List<Root> roots(DocumentPublicationCommand command) {
        // Canonical UUID text ordering agrees with PostgreSQL's unsigned UUID order.
        var pairs = new TreeSet<String>();
        for (var member : command.intent().getMembersList()) for (var part : member.getPartsList()) {
            if (part.hasHistoricalReuse()) {
                var source = part.getHistoricalReuse();
                pairs.add(DocumentIds.nodeId(source.getSource()) + "/" + source.getRevisionId());
            }
        }
        return pairs.stream().map(pair -> new Root(pair.substring(0, 36), pair.substring(37))).toList();
    }

    static byte[] digest(List<Root> roots) {
        try {
            var digest = MessageDigest.getInstance("SHA-256");
            digest.update("protomolt/preparation-history/v1\n".getBytes(StandardCharsets.UTF_8));
            for (var root : roots) digest.update((root.node() + "/" + root.revision() + "\n").getBytes(StandardCharsets.UTF_8));
            return digest.digest();
        } catch (NoSuchAlgorithmException unavailable) {
            throw new IllegalStateException("SHA-256 is unavailable", unavailable);
        }
    }

    /** New preparation only. Caller owns the live claim and ordered revision locks. */
    static void insert(EntityManager em, DocumentPublicationPreparationRecord record, byte[] preparationDigest,
            List<DocumentHistoricalReferenceAdmission.Prepared> sources) {
        DocumentHistoricalReferenceAdmission.requireComplete(record.command(), sources, () -> {});
        var roots = roots(record.command());
        scope(em.createNativeQuery("""
                INSERT INTO repository_preparation_history_sets(account_id,principal,operation_id,predecessor_generation,
                  preparation_sha256,command_sha256,expected_count,roots_sha256)
                VALUES(:a,:p,:o,:g,:preparation,:command,:count,:digest)
                """), record).setParameter("preparation", preparationDigest)
                .setParameter("command", HexFormat.of().parseHex(record.command().sha256()))
                .setParameter("count", roots.size()).setParameter("digest", digest(roots)).executeUpdate();
        if (!roots.isEmpty()) {
            // Only normalized UUIDs enter this bounded JSON projection, never arbitrary text.
            String rows = roots.stream().map(root -> "{\"node\":\"" + root.node() + "\",\"revision\":\"" + root.revision() + "\"}")
                    .collect(java.util.stream.Collectors.joining(",", "[", "]"));
            scope(em.createNativeQuery("""
                    INSERT INTO repository_preparation_history_roots(account_id,principal,operation_id,predecessor_generation,node_id,revision_id)
                    SELECT :a,:p,:o,:g,CAST(r.node AS uuid),CAST(r.revision AS uuid)
                    FROM jsonb_to_recordset(CAST(:rows AS jsonb)) AS r(node text,revision text)
                    ORDER BY r.node,r.revision
                    """), record).setParameter("rows", rows).executeUpdate();
        }
        scope(em.createNativeQuery("""
                UPDATE repository_preparation_history_sets SET sealed=true
                WHERE account_id=:a AND principal=:p AND operation_id=:o AND predecessor_generation=:g
                """), record).executeUpdate();
    }

    private static Query scope(Query query, DocumentPublicationPreparationRecord record) {
        return query.setParameter("a", record.key().account()).setParameter("p", record.key().principal())
                .setParameter("o", record.key().operationId()).setParameter("g", record.predecessorGeneration());
    }
}
