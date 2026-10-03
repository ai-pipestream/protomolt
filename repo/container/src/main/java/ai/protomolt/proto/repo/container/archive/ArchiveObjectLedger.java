package ai.protomolt.proto.repo.container.archive;

import ai.protomolt.proto.repo.container.ledger.Tx;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Immutable original storage coordinates for archive objects. Internal API;
 * callers must authorize the entry and select a qualified backend before use.
 * Registration is not byte verification, publication, or permission to delete.
 * Provider I/O and credentials never enter this ledger.
 */
public final class ArchiveObjectLedger {
    private final Tx tx;

    public ArchiveObjectLedger(Tx tx) { this.tx = Objects.requireNonNull(tx, "tx"); }

    public record Location(UUID entryUuid, String accountId, String archive,
            String backendGeneration, String bucket, String objectKey) {
        public Location {
            Objects.requireNonNull(entryUuid, "entryUuid");
            requireText(accountId, "accountId");
            requireText(archive, "archive");
            requireText(backendGeneration, "backendGeneration");
            requireText(bucket, "bucket");
            requireText(objectKey, "objectKey");
        }
    }

    public record Binding(UUID objectId, Location location, String storageRealm) {}

    /**
     * Register a fresh physical object before PUT. Duplicate coordinates fail;
     * they never redirect an existing object. A backend profile must already
     * exist. Engine admission/lease integration is a separate lifecycle boundary.
     */
    public Binding register(Location location) {
        Objects.requireNonNull(location, "location");
        return tx.inTransaction(em -> { return register(em, location); });
    }

    static Binding register(jakarta.persistence.EntityManager em, Location location) {
        UUID id = UUID.randomUUID();
        List<?> realms = em.createNativeQuery("SELECT storage_realm FROM managed_backend_profiles WHERE generation=:generation")
                .setParameter("generation", location.backendGeneration()).getResultList();
        if (realms.isEmpty()) throw new IllegalArgumentException("Archive backend generation is not registered");
        String realm = (String) realms.getFirst();
        em.createNativeQuery("""
                INSERT INTO archive_object_bindings
                (object_id,entry_uuid,account_id,archive,backend_generation,storage_realm,bucket,object_key)
                VALUES (:id,:entry,:account,:archive,:backend,:realm,:bucket,:key)
                """).setParameter("id", id).setParameter("entry", location.entryUuid())
                .setParameter("account", location.accountId()).setParameter("archive", location.archive())
                .setParameter("backend", location.backendGeneration()).setParameter("realm", realm)
                .setParameter("bucket", location.bucket())
                .setParameter("key", location.objectKey()).executeUpdate();
        return new Binding(id, location, realm);
    }

    public Optional<Binding> find(UUID objectId) {
        Objects.requireNonNull(objectId, "objectId");
        return tx.readOnly(em -> { return find(em, objectId); });
    }

    static Optional<Binding> find(jakarta.persistence.EntityManager em, UUID objectId) {
        List<?> rows = em.createNativeQuery("""
                SELECT object_id,entry_uuid,account_id,archive,backend_generation,bucket,object_key,storage_realm
                FROM archive_object_bindings WHERE object_id=:id
                """).setParameter("id", objectId).getResultList();
        if (rows.isEmpty()) return Optional.empty();
        var row = (Object[]) rows.getFirst();
        return Optional.of(new Binding((UUID) row[0], new Location((UUID) row[1],
                (String) row[2], (String) row[3], (String) row[4], (String) row[5], (String) row[6]), (String) row[7]));
    }

    private static void requireText(String value, String field) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException(field + " must not be blank");
    }
}
