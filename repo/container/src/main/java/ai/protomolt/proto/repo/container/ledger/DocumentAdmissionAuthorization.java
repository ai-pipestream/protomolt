package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.container.blob.DocumentIds;
import ai.protomolt.proto.repo.spi.DocumentAccessPolicy;
import ai.protomolt.proto.repo.spi.DocumentPublicationCommand;
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

    record Prepared(Set<UUID> destinations, Map<UUID, DocumentRevisionCondition> sources,
            Map<UUID, NodeAddress> historicalSources) {
        Prepared {
            destinations = Set.copyOf(destinations);
            // Prepare stable diagnostic order before acquiring database locks.
            sources = java.util.Collections.unmodifiableMap(new java.util.TreeMap<>(sources));
            historicalSources = java.util.Collections.unmodifiableMap(new java.util.TreeMap<>(historicalSources));
        }
    }

    static Prepared prepare(DocumentUploadPlan.Prepared plan) {
        return prepare(plan, List.of());
    }

    static Prepared prepare(DocumentUploadPlan.Prepared plan, List<DocumentHistoricalReferenceAdmission.Prepared> historical) {
        var nodes = new HashSet<UUID>();
        var sources = new HashMap<UUID, DocumentRevisionCondition>();
        var expected = new HashSet<ai.protomolt.proto.repo.v1.PublicationHistoricalReuse>();
        for (var member : plan.members()) {
            nodes.add(member.nodeId());
            for (var source : member.intent().getSourcesList()) addSource(sources, source);
            for (var part : member.intent().getPartsList()) {
                if (part.hasReuse()) addSource(sources, part.getReuse().getSource());
                if (part.hasHistoricalReuse()) expected.add(part.getHistoricalReuse());
            }
        }
        var actual = new HashSet<ai.protomolt.proto.repo.v1.PublicationHistoricalReuse>();
        var addresses = new HashMap<UUID, NodeAddress>();
        if (historical.size() > DocumentPublicationCommand.MAX_PARTS)
            throw new IllegalArgumentException("Historical source preparations exceed bounds");
        int count = 0;
        long bytes = 0;
        for (var prepared : historical) for (var selector : prepared.selectors()) {
            if (++count > DocumentPublicationCommand.MAX_PARTS)
                throw new IllegalArgumentException("Historical source selectors exceed bounds");
            bytes += selector.getSerializedSize();
            if (bytes > DocumentPublicationCommand.MAX_COMMAND_BYTES)
                throw new IllegalArgumentException("Historical source selector bytes exceed command bound");
            var address = selector.getSource();
            if (!plan.command().intent().getAccountId().equals(address.getAccountId()))
                throw new IllegalArgumentException("Historical source differs from command account");
            var prior = addresses.putIfAbsent(DocumentIds.nodeId(address), address);
            if (prior != null && !prior.equals(address))
                throw new IllegalArgumentException("Conflicting historical source addresses");
            actual.add(selector);
        }
        if (!actual.equals(expected))
            throw new IllegalArgumentException("Historical source preparations differ from complete command");
        return new Prepared(nodes, sources, addresses);
    }

    private static void addSource(Map<UUID, DocumentRevisionCondition> sources, DocumentRevisionCondition condition) {
        var prior = sources.putIfAbsent(DocumentIds.nodeId(condition.getAddress()), condition);
        if (prior != null && !prior.equals(condition))
            throw new IllegalArgumentException("Conflicting source conditions across publication members");
    }

    /** Trusted caller comes from the host, never from the command's proposed ownership. */
    static void requireCaller(RepositoryCaller caller, RepositoryOperationLedger.Owner owner, String account) {
        requireCaller(caller, owner.key(), account);
    }

    static void requireCaller(RepositoryCaller caller, RepositoryOperationLedger.Key key, String account) {
        if (caller == null || (!caller.processAuthority() && caller.accountIds().isEmpty()))
            throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED, "Repository account bindings are required");
        if (!caller.processAuthority() && !caller.accountIds().contains(account)) throw unavailable();
        if (!caller.principalName().equals(key.principal()))
            throw new RepositoryException(RepositoryException.Code.PERMISSION_DENIED, "Operation principal differs from authenticated caller");
    }

    /** Replay checks current read policy, without reapplying old write/revision preconditions. */
    static void authorizeReplay(EntityManager em, RepositoryCaller caller, DocumentPublicationCommand command) {
        authorizeReplay(em, caller, command, false);
    }

    /** Pending observation requires the current read set, not matching revision conditions. */
    static void authorizePending(EntityManager em, RepositoryCaller caller, DocumentPublicationCommand command) {
        authorizeReplay(em, caller, command, true);
    }

    /** Rejected creation can have no target; only the explicit process creation authority covers it. */
    static void authorizeRejection(EntityManager em, RepositoryCaller caller, DocumentPublicationCommand command) {
        authorizeReplay(em, caller, command, true);
    }

    /** Authorize the complete read set before revealing whether any command condition is stale. */
    static boolean revisionPreconditionsMatch(EntityManager em, RepositoryCaller caller, DocumentPublicationCommand command) {
        var locked = authorizeReplay(em, caller, command, true);
        for (var member : command.intent().getMembersList()) {
            if (!conditionMatches(member.getDestination(), locked)) return false;
            for (var source : member.getSourcesList()) if (!conditionMatches(source, locked)) return false;
            for (var part : member.getPartsList())
                if (part.hasReuse() && !conditionMatches(part.getReuse().getSource(), locked)) return false;
        }
        return true;
    }

    private static boolean conditionMatches(DocumentRevisionCondition condition, Map<UUID, DocumentRevisionLocks.SourceView> locked) {
        var row = locked.get(DocumentIds.nodeId(condition.getAddress()));
        return condition.getIfAbsent() ? row == null : row != null && row.mutationRevision() == condition.getExpectedMutationRevision();
    }

    private static Map<UUID, DocumentRevisionLocks.SourceView> authorizeReplay(EntityManager em, RepositoryCaller caller, DocumentPublicationCommand command,
            boolean allowUncreatedTarget) {
        var nodes = command.intent().getMembersList().stream()
                .map(member -> DocumentIds.nodeId(member.getDestination().getAddress()))
                .collect(java.util.stream.Collectors.toSet());
        var sources = new HashMap<UUID, DocumentRevisionCondition>();
        if (allowUncreatedTarget) {
            for (var member : command.intent().getMembersList()) {
                member.getSourcesList().forEach(source -> addSource(sources, source));
                member.getPartsList().stream().filter(part -> part.hasReuse())
                        .forEach(part -> addSource(sources, part.getReuse().getSource()));
            }
            nodes.addAll(sources.keySet());
        }
        var historical = new java.util.TreeMap<UUID, NodeAddress>();
        for (var member : command.intent().getMembersList()) for (var part : member.getPartsList()) {
            if (!part.hasHistoricalReuse()) continue;
            var address = part.getHistoricalReuse().getSource();
            var previous = historical.putIfAbsent(DocumentIds.nodeId(address), address);
            if (previous != null && !previous.equals(address))
                throw new IllegalArgumentException("Conflicting historical source addresses");
        }
        nodes.addAll(historical.keySet());
        var locked = DocumentRevisionLocks.lockForObservation(em, nodes);
        boolean needsCreationGrant = false;
        for (var source : historical.entrySet()) {
            var row = locked.get(source.getKey());
            requireIdentity(row, source.getValue());
            requireSourceAccess(caller, row);
            if (!DocumentStatus.AVAILABLE.equals(row.status()) || row.pendingPurgeId() != null) throw unavailable();
        }
        for (var member : command.intent().getMembersList()) {
            var address = member.getDestination().getAddress();
            var row = locked.get(DocumentIds.nodeId(address));
            if (row == null && allowUncreatedTarget && member.getDestination().getIfAbsent()) {
                needsCreationGrant |= !caller.processAuthority();
                continue;
            }
            requireIdentity(row, address);
            if (!DocumentStatus.AVAILABLE.equals(row.status()) || row.pendingPurgeId() != null) throw unavailable();
            requireSourceAccess(caller, row);
        }
        for (var source : sources.entrySet()) {
            var row = locked.get(source.getKey());
            requireIdentity(row, source.getValue().getAddress());
            if (!DocumentStatus.AVAILABLE.equals(row.status()) || row.pendingPurgeId() != null) throw unavailable();
            requireSourceAccess(caller, row);
        }
        RepositoryCreationGrants.authorizeObservation(em, caller, command, needsCreationGrant, allowUncreatedTarget);
        return locked;
    }

    /** Current read access precedes any historical revision or schema lookup. */
    static void authorizeHistory(EntityManager em, RepositoryCaller caller, NodeAddress address) {
        Objects.requireNonNull(address);
        if (caller == null) throw new RepositoryException(RepositoryException.Code.UNAUTHENTICATED,
                "Authenticated repository caller is required");
        if (!caller.processAuthority() && !caller.accountIds().contains(address.getAccountId())) throw unavailable();
        var node = DocumentIds.nodeId(address);
        var source = DocumentRevisionLocks.lockForAdmission(em, Set.of(), Set.of(node)).sources().get(node);
        requireIdentity(source, address);
        requireSourceAccess(caller, source);
        if (!DocumentStatus.AVAILABLE.equals(source.status()) || source.pendingPurgeId() != null) throw unavailable();
    }

    static Map<UUID, DocumentRecord> lockAndAuthorize(EntityManager em, RepositoryCaller caller,
            DocumentUploadPlan.Prepared plan, Prepared prepared) {
        return lockAndAuthorize(em, caller, plan, prepared, null);
    }

    static Map<UUID, DocumentRecord> lockAndAuthorize(EntityManager em, RepositoryCaller caller,
            DocumentUploadPlan.Prepared plan, Prepared prepared, DocumentCreationAuthorization creation) {
        // Lock every address first, but authorize before exposing revision mismatches.
        // Otherwise a source revision conflict can disclose a document the caller cannot read.
        var readNodes = new HashSet<>(prepared.sources().keySet());
        readNodes.addAll(prepared.historicalSources().keySet());
        var admitted = DocumentRevisionLocks.lockForAdmission(em, prepared.destinations(), readNodes);
        var locked = admitted.documents();
        for (var source : prepared.historicalSources().entrySet()) {
            var row = admitted.sources().get(source.getKey());
            requireIdentity(row, source.getValue());
            requireSourceAccess(caller, row);
            if (!DocumentStatus.AVAILABLE.equals(row.status()) || row.pendingPurgeId() != null) throw unavailable();
        }
        for (var source : prepared.sources().entrySet()) {
            var row = admitted.sources().get(source.getKey());
            requireIdentity(row, source.getValue().getAddress());
            requireSourceAccess(caller,row);
            if (!DocumentStatus.AVAILABLE.equals(row.status()) || row.pendingPurgeId() != null) throw unavailable();
        }
        for (var member : plan.members()) {
            var intent = member.intent();
            var row = locked.get(member.nodeId());
            if (row == null) {
                if (!caller.processAuthority() && (creation == null || caller.credentialBinding().isEmpty()
                        || !intent.getDestination().getIfAbsent()))
                    throw unavailable();
            } else {
                requireIdentity(row, intent.getDestination().getAddress());
                requireAccess(caller, row, Access.ACCESS_WRITE);
                if (!caller.processAuthority() && (!DocumentStatus.AVAILABLE.equals(row.status) || row.pendingPurgeId != null))
                    throw unavailable();
            }
            var proposed = intent.getOwnership().getSecurity();
            if (caller.processAuthority()) {
                DocumentAccessPolicy.allows(caller, intent.getOwnership().getAccountId(), proposed, List.of(), Access.ACCESS_WRITE);
            } else if (row != null) {
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
        if (!caller.processAuthority() && plan.members().stream().anyMatch(member -> member.intent().getDestination().getIfAbsent())) {
            if (creation == null) throw unavailable();
            creation.require(em, caller, plan);
        } else if (!caller.processAuthority() && caller.credentialBinding().isPresent()) {
            // Existing targets need no creation grant, but a cached caller or
            // retained handle cannot preserve a revoked credential generation.
            RepositoryCredentialAuthorities.requireLive(em, caller);
        }
        // Complete authorization for the entire set before returning any revision
        // conflict, including a stale readable source paired with a denied destination.
        for (var source : prepared.sources().entrySet())
            if (admitted.sources().get(source.getKey()).mutationRevision()!=source.getValue().getExpectedMutationRevision())
                throw new DocumentLedger.RevisionConflictException();
        for (var member : plan.members()) {
            var destination = member.intent().getDestination();
            DocumentLedger.requireRevision(locked.get(member.nodeId()), destination.hasExpectedMutationRevision()
                    ? destination.getExpectedMutationRevision() : null);
        }
        return locked;
    }

    private static void requireIdentity(DocumentRecord row, NodeAddress address) {
        if (row == null || !address.getAccountId().equals(row.accountId) || !address.getDocId().equals(row.docId)
                || !address.getGraphId().equals(row.graphId) || !address.getGraphAddressId().equals(row.graphAddressId))
            throw unavailable();
    }

    private static void requireIdentity(DocumentRevisionLocks.SourceView row,NodeAddress address) {
        if (row==null || !address.getAccountId().equals(row.accountId()) || !address.getDocId().equals(row.docId())
                || !address.getGraphId().equals(row.graphId()) || !address.getGraphAddressId().equals(row.graphAddressId()))
            throw unavailable();
    }

    private static void requireSourceAccess(RepositoryCaller caller,DocumentRevisionLocks.SourceView row) {
        if (!caller.processAuthority() && !caller.accountIds().contains(row.accountId())) throw unavailable();
        final DocumentSecurity policy;
        try { policy=DocumentRecord.parseSecurity(row.security(),row.nodeId()); }
        catch (LedgerException failure) {
            throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION,"Stored document policy is malformed",failure);
        }
        if (!DocumentAccessPolicy.allows(caller,row.accountId(),policy,caller.processAuthority() ? List.of() : null,Access.ACCESS_READ))
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
