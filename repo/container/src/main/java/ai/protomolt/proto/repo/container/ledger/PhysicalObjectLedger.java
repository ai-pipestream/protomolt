package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.spi.PhysicalObjectLocation;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Original location lookup for admitted managed objects. Registration is atomic
 * with source admission through database guards. This catalog does not authorize
 * reads, certify bytes, acquire references, or decide cleanup eligibility.
 */
public final class PhysicalObjectLedger {
    private final Tx tx;

    public PhysicalObjectLedger(Tx tx) {
        this.tx = Objects.requireNonNull(tx, "tx");
    }

    public Optional<PhysicalObjectLocation> find(UUID objectId) {
        Objects.requireNonNull(objectId, "objectId");
        return tx.readOnly(em -> {
            var rows = em.createNativeQuery("""
                    SELECT object_id,backend_generation,storage_realm,storage_namespace,object_key
                    FROM repository_physical_locations WHERE object_id=:id
                    """).setParameter("id", objectId).getResultList();
            if (rows.isEmpty()) return Optional.empty();
            var row = (Object[]) rows.getFirst();
            return Optional.of(new PhysicalObjectLocation((UUID) row[0], (String) row[1],
                    (String) row[2], (String) row[3], (String) row[4]));
        });
    }
}
