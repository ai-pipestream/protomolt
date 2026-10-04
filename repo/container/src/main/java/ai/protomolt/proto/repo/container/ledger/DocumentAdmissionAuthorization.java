package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.container.blob.DocumentIds;
import ai.protomolt.proto.repo.spi.DocumentAccessPolicy;
import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.v1.Access;
import ai.protomolt.proto.repo.v1.DocumentRevisionCondition;
import ai.protomolt.proto.repo.v1.DocumentSecurity;
import ai.protomolt.proto.repo.v1.NodeAddress;
import jakarta.persistence.EntityManager;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/** Current-policy admission only; publication must repeat authorization under its own revision locks. */
final class DocumentAdmissionAuthorization {
    private DocumentAdmissionAuthorization() {}

    record Prepared(Set<UUID> destinations, Map<UUID, DocumentRevisionCondition> sources) {
        Prepared {
            destinations = Set.copyOf(destinations);
            // Prepare stable diagnostic order before acquiring database locks.
            sources = java.util.Collections.unmodifiableMap(new java.util.TreeMap<>(sources));
        }
    }

    static Prepared prepare(DocumentUploadPlan.Prepared plan) {
        var nodes = new HashSet<UUID>();
        var sources = new HashMap<UUID, DocumentRevisionCondition>();
        for (var member : plan.members()) {
            nodes.add(member.nodeId());
            for (var source : member.intent().getSourcesList()) addSource(sources, source);
            for (var part : member.intent().getPartsList()) {
                if (part.hasReuse()) addSource(sources, part.getReuse().getSource());
            }
        }
        return new Prepared(nodes, sources);
    }

    private static void addSource(Map<UUID, DocumentRevisionCondition> sources, DocumentRevisionCondition condition) {
        var prior = sources.putIfAbsent(DocumentIds.nodeId(condition.getAddress()), condition);
        if (prior != null && !prior.equals(condition))
            throw new IllegalArgumentException("Conflicting source conditions across publication members");
    }

    /** Trusted caller comes from the host, never from the command's proposed ownership. */
    static void requireCaller(RepositoryCaller caller, RepositoryOperationLedger.Owner owner, String account) {
        if (caller == null || (!caller.processAuthority() && caller.accountIds().isEmpty()))
            throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED, "Repository account bindings are required");
        if (!caller.processAuthority() && !caller.accountIds().contains(account)) throw unavailable();
        if (!caller.principalName().equals(owner.key().principal()))
            throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED, "Operation principal differs from authenticated caller");
    }

    static void lockAndAuthorize(EntityManager em, RepositoryCaller caller,
            DocumentUploadPlan.Prepared plan, Prepared prepared) {
        // Lock every address first, but authorize before exposing revision mismatches.
        // Otherwise a source revision conflict can disclose a document the caller cannot read.
        var locked = DocumentRevisionLocks.lock(em, prepared.destinations(), prepared.sources().keySet());
        for (var source : prepared.sources().entrySet()) {
            var row = locked.get(source.getKey());
            requireIdentity(row, source.getValue().getAddress());
            requireAccess(caller, row, Access.ACCESS_READ);
            if (!DocumentStatus.AVAILABLE.equals(row.status) || row.pendingPurgeId != null) throw unavailable();
        }
        for (var member : plan.members()) {
            var intent = member.intent();
            var row = locked.get(member.nodeId());
            if (row == null) {
                // Scoped creation needs its own host-bound grant contract; do not infer it from ownership.
                if (!caller.processAuthority()) throw unavailable();
            } else {
                requireIdentity(row, intent.getDestination().getAddress());
                requireAccess(caller, row, Access.ACCESS_WRITE);
                if (!caller.processAuthority() && (!DocumentStatus.AVAILABLE.equals(row.status) || row.pendingPurgeId != null))
                    throw unavailable();
            }
            var proposed = intent.getOwnership().getSecurity();
            if (caller.processAuthority()) {
                DocumentAccessPolicy.allows(caller, intent.getOwnership().getAccountId(), proposed, List.of(), Access.ACCESS_WRITE);
            } else {
                if (!Objects.equals(security(row), proposed))
                    throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED,
                            "Changing document access policy requires process authority");
                String reason = intent.getSourceBlobDeleteReason().isBlank() ? null : intent.getSourceBlobDeleteReason();
                if (!member.placement().drive().name().equals(row.driveName)
                        || !intent.getOwnership().getDatasourceId().equals(row.datasourceId)
                        || intent.getDeleteSourceBlobsOnSettle() != row.deleteSourceBlobsOnSettle
                        || !Objects.equals(reason, row.sourceBlobDeleteReason))
                    throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED,
                            "Changing document storage, datasource or deletion policy requires process authority");
            }
        }
        // Complete authorization for the entire set before returning any revision
        // conflict, including a stale readable source paired with a denied destination.
        for (var source : prepared.sources().entrySet())
            DocumentLedger.requireRevision(locked.get(source.getKey()), source.getValue().getExpectedMutationRevision());
        for (var member : plan.members()) {
            var destination = member.intent().getDestination();
            DocumentLedger.requireRevision(locked.get(member.nodeId()), destination.hasExpectedMutationRevision()
                    ? destination.getExpectedMutationRevision() : null);
        }
    }

    private static void requireIdentity(DocumentRecord row, NodeAddress address) {
        if (row == null || !address.getAccountId().equals(row.accountId) || !address.getDocId().equals(row.docId)
                || !address.getGraphId().equals(row.graphId) || !address.getGraphAddressId().equals(row.graphAddressId))
            throw unavailable();
    }

    private static void requireAccess(RepositoryCaller caller, DocumentRecord row, Access access) {
        if (!caller.processAuthority() && !caller.accountIds().contains(row.accountId)) throw unavailable();
        if (!DocumentAccessPolicy.allows(caller, row.accountId, security(row), caller.processAuthority() ? List.of() : null, access))
            throw unavailable();
    }

    private static DocumentSecurity security(DocumentRecord row) {
        try { return row.readSecurity(); }
        catch (LedgerException failure) {
            throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION, "Stored document policy is malformed", failure);
        }
    }

    private static RepositoryException unavailable() {
        return new RepositoryException(RepositoryException.Code.NOT_FOUND, "Document is unavailable");
    }
}
