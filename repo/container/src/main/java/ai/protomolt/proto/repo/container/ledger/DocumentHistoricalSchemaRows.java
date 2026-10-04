package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.admission.DocumentSchemaAdmission;
import ai.protomolt.proto.repo.container.blob.DocumentIds;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.v1.NodeAddress;
import com.google.protobuf.ByteString;
import jakarta.persistence.EntityManager;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/** Bounded internal storage snapshot; the caller must hold current document READ authorization. */
final class DocumentHistoricalSchemaRows {
    record Header(UUID operation, String member, String commandSha, String policySha,
            String containerUrlSha, String containerDescriptorSha,
            String commandCodec, int commandVersion, ByteString command,
            String policyCodec, int policyVersion, ByteString policy) {}
    record Root(int ordinal, String locatorSha, String fragmentSha, long fragmentSize,
            DocumentSchemaAdmission.EncodedEvidence evidence) {}
    record Snapshot(Header header, List<DocumentSchemaAdmission.Reference> references,
            List<Root> roots, Map<String, ByteString> artifacts) {
        Snapshot { references = List.copyOf(references); roots = List.copyOf(roots); artifacts = Map.copyOf(artifacts); }
    }
    private DocumentHistoricalSchemaRows() {}

    static Snapshot capture(EntityManager em, NodeAddress address, UUID revision, Runnable control) {
        return capture(em, address, revision, control, ignored -> {});
    }

    /** reserve is an internal nonblocking byte-budget operation, never provider work. */
    static Snapshot capture(EntityManager em, NodeAddress address, UUID revision, Runnable control,
            java.util.function.LongConsumer reserve) {
        control.run();
        var rows = em.createNativeQuery("""
                SELECT r.native_binding,r.projection_sealed,c.account_id,c.admission_mode,a.decision,
                 (c.node_id=r.node_id AND a.node_id=r.node_id AND a.account_id=c.account_id
                  AND a.principal=c.principal AND a.operation_id=c.operation_id AND a.owner_generation=c.owner_generation
                  AND a.member_id=c.member_id AND a.selection_revision=c.selection_revision
                  AND c.publication_revision=r.publication_revision AND s.command_sha256=a.command_sha256
                  AND a.creation_xid=c.creation_xid AND c.creation_xid=r.projection_xid
                  AND s.creation_xid=c.creation_xid AND a.metadata=c.metadata_snapshot AND a.body=r.body
                  AND s.command_codec='document-publication' AND s.command_version=1),
                 a.container_type_url_sha256,a.container_descriptor_sha256,
                 octet_length(o.command),octet_length(p.policy_bytes)
                FROM document_revision_publications r
                LEFT JOIN document_revision_commits c ON c.revision_id=r.revision_id
                LEFT JOIN document_revision_schema_admissions a ON a.revision_id=r.revision_id
                LEFT JOIN repository_operation_success s ON s.account_id=c.account_id AND s.principal=c.principal
                 AND s.operation_id=c.operation_id AND s.owner_generation=c.owner_generation
                LEFT JOIN repository_operations o ON o.account_id=c.account_id AND o.principal=c.principal AND o.operation_id=c.operation_id
                LEFT JOIN document_schema_policies p ON p.account_id=a.account_id AND p.policy_sha256=a.policy_sha256
                WHERE r.revision_id=:revision AND r.node_id=:node
                """).setParameter("revision", revision).setParameter("node", DocumentIds.nodeId(address)).getResultList();
        if (rows.isEmpty()) throw new RepositoryException(RepositoryException.Code.NOT_FOUND, "Document revision is unavailable");
        Object[] state = (Object[]) rows.getFirst();
        if (state[0] == null) throw unsupported("Revision has no retained native schema admission");
        if (!revision.equals(state[0]) || !Boolean.TRUE.equals(state[1]) || !address.getAccountId().equals(state[2]))
            throw invalid("Historical revision binding is invalid");
        if ("OPAQUE".equals(state[3]) && (state[4] == null || "OPAQUE".equals(state[4])))
            throw unsupported("Revision was preserved without typed schema admission");
        if (!"TYPED".equals(state[3]) || !"TYPED".equals(state[4]) || !Boolean.TRUE.equals(state[5]))
            throw invalid("Historical typed admission or terminal binding is invalid");
        if (state[6] == null && state[7] == null) throw unsupported("Historical container schema role is unknown");
        if (state[6] == null || state[7] == null) throw invalid("Historical container schema role is incomplete");
        if (!(state[8] instanceof Number commandSize) || commandSize.longValue() < 1 || commandSize.longValue() > 1048576
                || !(state[9] instanceof Number policySize) || policySize.longValue() < 1 || policySize.longValue() > 524288)
            throw invalid("Historical command or policy size is invalid");
        reserve.accept(2 * (commandSize.longValue() + policySize.longValue()));
        control.run();
        var headers = em.createNativeQuery("""
                SELECT a.operation_id,a.member_id,encode(a.command_sha256,'hex'),encode(a.policy_sha256,'hex'),
                 o.command_codec,o.command_version,
                 CASE WHEN octet_length(o.command) BETWEEN 1 AND 1048576 THEN o.command END,
                 p.policy_codec,p.policy_version,
                 CASE WHEN octet_length(p.policy_bytes) BETWEEN 1 AND 524288 THEN p.policy_bytes END,
                 a.manifest=document_schema_retention_manifest_v1(a.revision_id),
                 o.command_sha256=a.command_sha256
                FROM document_revision_schema_admissions a
                JOIN repository_operations o USING(account_id,principal,operation_id)
                JOIN document_schema_policies p ON p.account_id=a.account_id AND p.policy_sha256=a.policy_sha256
                WHERE a.revision_id=:revision AND a.account_id=:account
                """).setParameter("revision", revision).setParameter("account", address.getAccountId()).getResultList();
        if (headers.size() != 1) throw invalid("Historical command or policy is missing");
        Object[] h = (Object[]) headers.getFirst();
        if (h[6] == null || h[9] == null || !Boolean.TRUE.equals(h[10]) || !Boolean.TRUE.equals(h[11]))
            throw invalid("Historical command, policy or retained manifest is invalid");
        var header = new Header((UUID) h[0], (String) h[1], (String) h[2], (String) h[3], hex(state[6]), hex(state[7]),
                (String) h[4], ((Number) h[5]).intValue(), bytes(h[6]),
                (String) h[7], ((Number) h[8]).intValue(), bytes(h[9]));
        var artifacts = artifacts(em, address.getAccountId(), revision, control, reserve);
        var references = references(em, address.getAccountId(), revision, control);
        var roots = roots(em, address.getAccountId(), revision, control, reserve);
        return new Snapshot(header, references, roots, artifacts);
    }

    private static Map<String, ByteString> artifacts(EntityManager em, String account, UUID revision, Runnable control,
            java.util.function.LongConsumer reserve) {
        control.run();
        Object[] budget = (Object[]) em.createNativeQuery("""
                SELECT count(*),count(a.artifact_sha256),COALESCE(sum(octet_length(a.artifact_bytes)),0)
                FROM document_revision_schema_artifacts r LEFT JOIN repository_schema_artifacts a
                 ON a.account_id=r.account_id AND a.account_id=:account AND a.artifact_sha256=r.artifact_sha256
                WHERE r.revision_id=:revision
                """).setParameter("revision", revision).setParameter("account", account).getSingleResult();
        long count = ((Number) budget[0]).longValue();
        if (count < 1 || count > 64 || ((Number) budget[1]).longValue() != count
                || ((Number) budget[2]).longValue() > 64L * 1024 * 1024)
            throw invalid("Historical schema artifact set is missing or exceeds limits");
        reserve.accept(2 * ((Number) budget[2]).longValue());
        var rows = em.createNativeQuery("""
                SELECT encode(a.artifact_sha256,'hex'),a.artifact_bytes FROM document_revision_schema_artifacts r
                JOIN repository_schema_artifacts a ON a.account_id=r.account_id AND a.artifact_sha256=r.artifact_sha256
                WHERE r.revision_id=:revision AND r.account_id=:account ORDER BY a.artifact_sha256 LIMIT 65
                """).setParameter("revision", revision).setParameter("account", account).getResultList();
        if (rows.size() != count) throw invalid("Historical schema artifact set changed");
        var result = new LinkedHashMap<String, ByteString>();
        for (Object value : rows) { control.run(); var row = (Object[]) value; result.put((String) row[0], bytes(row[1])); }
        return result;
    }

    private static List<DocumentSchemaAdmission.Reference> references(EntityManager em, String account, UUID revision, Runnable control) {
        control.run();
        var rows = em.createNativeQuery("""
                SELECT type_url,encode(descriptor_sha256,'hex'),metadata_codec,metadata_version,
                 encode(metadata_sha256,'hex'),encode(source_sha256,'hex'),account_id
                FROM document_revision_schema_assets WHERE revision_id=:revision
                ORDER BY type_url_sha256,descriptor_sha256 LIMIT 65
                """).setParameter("revision", revision).getResultList();
        if (rows.isEmpty() || rows.size() > 64) throw invalid("Historical schema association set is missing or exceeds limits");
        var result = new ArrayList<DocumentSchemaAdmission.Reference>(rows.size());
        for (Object value : rows) {
            control.run(); var row = (Object[]) value;
            if (!account.equals(row[6])) throw invalid("Historical schema association account differs");
            result.add(new DocumentSchemaAdmission.Reference((String) row[0], (String) row[1], (String) row[2],
                    ((Number) row[3]).intValue(), (String) row[4], Optional.ofNullable((String) row[5])));
        }
        return result;
    }

    private static List<Root> roots(EntityManager em, String account, UUID revision, Runnable control,
            java.util.function.LongConsumer reserve) {
        control.run();
        Object[] budget = (Object[]) em.createNativeQuery("""
                SELECT count(*),COALESCE(sum(octet_length(evidence_bytes)),0),count(*) FILTER(WHERE account_id=:account)
                FROM document_revision_schema_evidence WHERE revision_id=:revision
                """).setParameter("revision", revision).setParameter("account", account).getSingleResult();
        long count = ((Number) budget[0]).longValue();
        if (count > 1024 || ((Number) budget[1]).longValue() > 16L * 1024 * 1024 || ((Number) budget[2]).longValue() != count)
            throw invalid("Historical schema evidence exceeds limits or account scope");
        reserve.accept(2 * ((Number) budget[1]).longValue());
        var rows = em.createNativeQuery("""
                SELECT revision_ordinal,encode(root_locator_sha256,'hex'),encode(fragment_sha256,'hex'),fragment_size,
                 evidence_codec,evidence_version,evidence_bytes,encode(evidence_sha256,'hex')
                FROM document_revision_schema_evidence WHERE revision_id=:revision
                ORDER BY revision_ordinal,root_locator_sha256 LIMIT 1025
                """).setParameter("revision", revision).getResultList();
        if (rows.size() != count) throw invalid("Historical schema evidence set changed");
        var result = new ArrayList<Root>(rows.size());
        for (Object value : rows) {
            control.run(); var row = (Object[]) value;
            result.add(new Root(((Number) row[0]).intValue(), (String) row[1], (String) row[2], ((Number) row[3]).longValue(),
                    new DocumentSchemaAdmission.EncodedEvidence((String) row[4], ((Number) row[5]).intValue(), bytes(row[6]), (String) row[7])));
        }
        return result;
    }

    private static ByteString bytes(Object bytes) { return ByteString.copyFrom((byte[]) bytes); }
    private static String hex(Object bytes) { return HexFormat.of().formatHex((byte[]) bytes); }
    static RepositoryException invalid(String message) { return new RepositoryException(RepositoryException.Code.DATA_LOSS, message); }
    private static RepositoryException unsupported(String message) {
        return new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION, message);
    }
}
