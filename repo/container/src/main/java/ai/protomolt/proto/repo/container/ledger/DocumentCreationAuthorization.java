package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.RepositoryCaller;
import ai.protomolt.proto.repo.spi.RepositoryException;
import com.google.protobuf.ByteString;
import jakarta.persistence.EntityManager;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/** Prepared outside SQL; selected placement is authoritative only after the in-transaction check. */
final class DocumentCreationAuthorization {
    private final DocumentUploadPlan.Prepared plan;
    private final DriveLedger drives;
    private final Map<UUID, DocumentUploadPlan.Placement> placements;
    private final ByteString digest;

    static DocumentCreationAuthorization prepare(DocumentUploadPlan.Prepared plan, DriveLedger drives, RepositoryCaller caller) {
        return !caller.processAuthority() && caller.credentialBinding().isPresent()
                && plan.members().stream().anyMatch(member -> member.intent().getDestination().getIfAbsent())
                ? new DocumentCreationAuthorization(plan, drives) : null;
    }

    DocumentCreationAuthorization(DocumentUploadPlan.Prepared plan, DriveLedger drives) {
        this.plan = Objects.requireNonNull(plan);
        this.drives = Objects.requireNonNull(drives);
        var selected = new java.util.TreeMap<UUID, DocumentUploadPlan.Placement>();
        for (var member : plan.members()) {
            var placement = member.placement();
            var prior = selected.putIfAbsent(placement.drive().id(), placement);
            if (prior != null && !prior.equals(placement))
                throw new IllegalArgumentException("Conflicting selected drive snapshots");
        }
        placements = java.util.Collections.unmodifiableMap(selected);
        digest = ByteString.copyFrom(DocumentPublicationPreparationCodec.placementDigest(placements));
    }

    void require(EntityManager em, RepositoryCaller caller, DocumentUploadPlan.Prepared actual) {
        if (actual != plan) throw new IllegalArgumentException("Creation authorization belongs to another prepared plan");
        // Preserve the host-selected backend gate; never construct a default DriveLedger here.
        for (var placement : placements.values()) {
            placement.drive().lock(em, drives);
            if (!ManagedBackendLedger.find(em, placement.generation()).filter(placement.profile()::equals).isPresent())
                throw new RepositoryException(RepositoryException.Code.CONFLICT, "Selected backend profile is unavailable or changed");
        }
        RepositoryCreationGrants.requireLive(em, caller, plan.command(), digest);
    }
}
