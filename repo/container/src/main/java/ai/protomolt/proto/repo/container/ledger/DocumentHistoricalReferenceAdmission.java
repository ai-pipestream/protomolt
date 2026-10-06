package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.container.blob.DocumentIds;
import ai.protomolt.proto.repo.spi.RepositoryReadControl;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
import ai.protomolt.proto.repo.v1.PublicationHistoricalReuse;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import jakarta.persistence.EntityManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/** Exact historical reference checks inside a locked publication transaction; no provider work. */
final class DocumentHistoricalReferenceAdmission {
    private DocumentHistoricalReferenceAdmission() {}
    private record Batch(String json, int count) {}

    /** Complete distinct selector identity; Uses remain borrowed from their existing owners. */
    static List<Prepared> requireComplete(DocumentPublicationCommand command, List<Prepared> sources, Runnable control) {
        if (sources.size() > DocumentPublicationCommand.MAX_PARTS) throw mismatch();
        sources = List.copyOf(sources);
        var declared = new java.util.HashSet<PublicationHistoricalReuse>();
        for (var member : command.intent().getMembersList()) for (var part : member.getPartsList()) {
            control.run();
            if (part.hasHistoricalReuse()) declared.add(part.getHistoricalReuse());
        }
        var supplied = new java.util.HashSet<PublicationHistoricalReuse>();
        int count = 0; long bytes = 0;
        for (var source : sources) for (var selector : source.selectors()) {
            control.run();
            if (++count > DocumentPublicationCommand.MAX_PARTS
                    || (bytes += selector.getSerializedSize()) > DocumentPublicationCommand.MAX_COMMAND_BYTES
                    || !selector.getSource().getAccountId().equals(command.intent().getAccountId())) throw mismatch();
            supplied.add(selector);
        }
        if (!declared.equals(supplied)) throw mismatch();
        control.run(); sources.forEach(source -> source.selectors());
        return sources;
    }

    private static DocumentPartAttemptLedger.FenceException mismatch() {
        return new DocumentPartAttemptLedger.FenceException("Historical preparations differ from complete command");
    }
    static final class Prepared {
        private final DocumentReadLedger.PinnedRead<DocumentHistoricalReadPlan>.Use use;
        private final String account;
        private final Set<UUID> objects;
        private final List<Batch> batches;
        private final List<PublicationHistoricalReuse> selectors;
        private final List<DocumentHistoricalReadPlan.Entry> entries;
        private Prepared(DocumentReadLedger.PinnedRead<DocumentHistoricalReadPlan>.Use use,
                String account, Set<UUID> objects, List<Batch> batches, List<PublicationHistoricalReuse> selectors,
                List<DocumentHistoricalReadPlan.Entry> entries) {
            this.use = use; this.account = account; this.objects = Set.copyOf(objects); this.batches = List.copyOf(batches);
            this.selectors = List.copyOf(selectors);
            this.entries = List.copyOf(entries);
        }
        List<PublicationHistoricalReuse> selectors() { use.plan(); return selectors; }
        List<DocumentHistoricalReadPlan.Entry> entries() { use.plan(); return entries; }
    }

    /** Borrows an existing Use, which the caller retains through the final transaction. */
    static Prepared prepare(DocumentReadLedger.PinnedHistory history,
            DocumentReadLedger.PinnedRead<DocumentHistoricalReadPlan>.Use use,
            List<PublicationHistoricalReuse> selectors, RepositoryReadControl control) {
        selectors = List.copyOf(selectors);
        var entries = history.selectRetained(use, selectors, control);
        var plan = use.plan();
        var batches = new ArrayList<Batch>();
        for (int start = 0; start < selectors.size(); start += 256) {
            var rows = ListValue.newBuilder();
            int end = Math.min(start + 256, selectors.size());
            for (int i = start; i < end; i++) {
                control.check();
                var selector = selectors.get(i); var object = selector.getObject();
                var row = Struct.newBuilder()
                        .putFields("node", text(DocumentIds.nodeId(selector.getSource()).toString()))
                        .putFields("revision", text(selector.getRevisionId()))
                        .putFields("publication", text(Long.toString(plan.publicationRevision())))
                        .putFields("ordinal", text(Integer.toString(selector.getRevisionOrdinal())))
                        .putFields("part", text(Integer.toString(selector.getSourceSlot().getPartValue())))
                        .putFields("sub_key", text(selector.getSourceSlot().getSubKey()))
                        .putFields("object_id", text(object.getObjectId()))
                        .putFields("generation", text(object.getBackendGeneration()))
                        .putFields("realm", text(object.getStorageRealm()))
                        .putFields("namespace", text(object.getNamespace()))
                        .putFields("object_key", text(object.getObjectKey()))
                        .putFields("size", text(Long.toString(object.getSizeBytes())))
                        .putFields("sha256", text(object.getSha256()))
                        .putFields("content_type", text(object.getContentType()));
                if (object.hasProviderVersion()) row.putFields("version", text(object.getProviderVersion()));
                rows.addValues(Value.newBuilder().setStructValue(row));
            }
            try { batches.add(new Batch(JsonFormat.printer().omittingInsignificantWhitespace().print(rows), end - start)); }
            catch (com.google.protobuf.InvalidProtocolBufferException failure) {
                throw new IllegalArgumentException("Cannot encode historical reference claims", failure);
            }
        }
        control.check(); use.plan();
        return new Prepared(use, plan.address().getAccountId(),
                entries.stream().map(DocumentHistoricalReadPlan.Entry::objectId).collect(Collectors.toSet()), batches, selectors, entries);
    }

    /**
     * Caller holds operation/policy, complete current document authorization and drive locks,
     * followed by the complete origin/retention set. This adds no destination or policy grant.
     * Success applies only in this transaction, before reference publication and Use release.
     */
    static void requireBoundSources(EntityManager em, Prepared prepared,
            DocumentPublicationLocks.IndependentOrigins locks, RepositoryReadControl control) {
        try {
            control.check(); prepared.use.plan();
            locks.requireObjects(em, prepared.objects);
            for (var batch : prepared.batches) {
                control.check();
                long count = ((Number) em.createNativeQuery("""
                        SELECT count(*) FROM jsonb_to_recordset(CAST(:rows AS jsonb)) q(
                            node uuid,revision uuid,publication bigint,ordinal integer,part integer,sub_key text,
                            object_id uuid,generation text,realm text,namespace text,object_key text,
                            version text,size bigint,sha256 text,content_type text)
                        JOIN document_revision_publications h ON h.revision_id=q.revision AND h.node_id=q.node
                            AND h.publication_revision=q.publication AND h.native_binding=h.revision_id AND h.projection_sealed
                        JOIN document_revision_commits c ON c.revision_id=h.revision_id AND c.node_id=h.node_id
                            AND c.account_id=:account AND c.publication_revision=h.publication_revision AND c.creation_xid=h.projection_xid
                        JOIN repository_operation_success s ON s.account_id=c.account_id AND s.principal=c.principal
                            AND s.operation_id=c.operation_id AND s.owner_generation=c.owner_generation AND s.creation_xid=c.creation_xid
                        JOIN document_revision_parts p ON p.revision_id=h.revision_id AND p.revision_ordinal=q.ordinal
                            AND p.part=q.part AND p.sub_key=q.sub_key AND p.object_id=q.object_id
                        JOIN repository_physical_locations l ON l.object_id=p.object_id AND l.source_kind='DOCUMENT_PART'
                        JOIN document_part_attempt_objects o ON o.attempt_id=l.source_id AND o.ordinal=l.source_ordinal
                            AND o.physical_object_id=l.object_id AND o.verified AND o.part=q.part AND o.sub_key=q.sub_key
                        JOIN document_part_attempts a ON a.attempt_id=o.attempt_id AND a.account_id=:account AND a.state='VERIFIED'
                        JOIN repository_object_retention r ON r.object_id=l.object_id AND NOT r.retiring AND NOT r.reclaiming
                        JOIN repository_object_references ref ON ref.object_id=l.object_id AND ref.owner_kind='DOCUMENT_HISTORY'
                            AND ref.owner_id=h.revision_id AND ref.owner_revision=h.publication_revision
                        WHERE l.backend_generation=q.generation AND a.backend_generation=l.backend_generation
                            AND l.storage_realm=q.realm AND a.storage_realm=l.storage_realm AND o.storage_realm=l.storage_realm
                            AND l.storage_namespace=q.namespace AND o.storage_namespace=l.storage_namespace
                            AND l.object_key=q.object_key AND o.object_key=l.object_key
                            AND o.provider_version IS NOT DISTINCT FROM q.version
                            AND o.expected_size=q.size AND o.expected_sha256=q.sha256 AND o.content_type=q.content_type
                            AND NOT EXISTS(SELECT 1 FROM document_part_attempt_cleanup cleanup WHERE cleanup.attempt_id=a.attempt_id)
                        """).setParameter("rows", batch.json()).setParameter("account", prepared.account).getSingleResult()).longValue();
                if (count != batch.count()) throw new DocumentPartAttemptLedger.FenceException(
                        "Historical reference differs from retained physical binding");
            }
            control.check(); prepared.use.plan();
        } catch (RuntimeException | Error failure) {
            try { if (em.getTransaction().isActive()) em.getTransaction().setRollbackOnly(); }
            catch (RuntimeException marking) { failure.addSuppressed(marking); }
            throw failure;
        }
    }

    private static Value text(String value) { return Value.newBuilder().setStringValue(value).build(); }
}
