package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.container.blob.DocumentIds;
import ai.protomolt.proto.repo.v1.NodeAddress;
import jakarta.persistence.EntityManager;
import java.util.UUID;

/** Reads an explicit retained admission mode; never infers opacity from missing schemas. */
final class DocumentHistoricalOpaqueAdmission {
    private DocumentHistoricalOpaqueAdmission() {}

    /** Caller holds current source READ and an exact live historical Use. No payload is decoded. */
    static void requireOpaque(EntityManager em, NodeAddress address, UUID revision, Runnable control) {
        control.run();
        var rows = em.createNativeQuery("""
                SELECT c.admission_mode,a.decision,
                 (r.native_binding=r.revision_id AND r.projection_sealed
                  AND c.account_id=:account AND a.account_id=c.account_id
                  AND c.node_id=r.node_id AND a.node_id=r.node_id
                  AND a.principal=c.principal AND a.operation_id=c.operation_id
                  AND a.owner_generation=c.owner_generation AND a.member_id=c.member_id
                  AND a.selection_revision=c.selection_revision AND c.publication_revision=r.publication_revision
                  AND s.command_sha256=a.command_sha256 AND o.command_sha256=a.command_sha256
                  AND a.creation_xid=c.creation_xid AND c.creation_xid=r.projection_xid
                  AND s.creation_xid=c.creation_xid AND a.metadata=c.metadata_snapshot AND a.body=r.body
                  AND s.command_codec='document-publication' AND s.command_version=1
                  AND CASE WHEN a.revision_id IS NOT NULL AND r.native_binding=r.revision_id
                      THEN a.manifest=document_schema_retention_manifest_v1(a.revision_id) ELSE false END),
                 a.container_type_url_sha256,a.container_descriptor_sha256
                FROM document_revision_publications r
                LEFT JOIN document_revision_commits c ON c.revision_id=r.revision_id
                LEFT JOIN document_revision_schema_admissions a ON a.revision_id=r.revision_id
                LEFT JOIN repository_operation_success s ON s.account_id=c.account_id AND s.principal=c.principal
                 AND s.operation_id=c.operation_id AND s.owner_generation=c.owner_generation
                LEFT JOIN repository_operations o ON o.account_id=c.account_id AND o.principal=c.principal
                 AND o.operation_id=c.operation_id
                WHERE r.revision_id=:revision AND r.node_id=:node
                """).setParameter("account", address.getAccountId()).setParameter("revision", revision)
                .setParameter("node", DocumentIds.nodeId(address)).getResultList();
        control.run();
        if (rows.size() != 1) throw DocumentHistoricalSchemaRows.invalid("Historical admission is unavailable");
        var row = (Object[]) rows.getFirst();
        if (row[0] == null || row[1] == null)
            throw DocumentHistoricalSchemaRows.unsupported("Historical source has no explicit retained admission");
        if (!Boolean.TRUE.equals(row[2]) || !row[0].equals(row[1]))
            throw DocumentHistoricalSchemaRows.invalid("Historical admission binding is invalid");
        if (!"OPAQUE".equals(row[0]))
            throw DocumentHistoricalSchemaRows.unsupported("Historical source cannot be downgraded to opaque mode");
        if (row[3] != null || row[4] != null)
            throw DocumentHistoricalSchemaRows.invalid("Opaque historical admission has a typed container role");
    }
}
