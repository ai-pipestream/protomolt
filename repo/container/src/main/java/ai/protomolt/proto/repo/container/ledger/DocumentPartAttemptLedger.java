package ai.protomolt.proto.repo.container.ledger;

import ai.protomolt.proto.repo.codec.RepositoryNamespaces;
import ai.protomolt.proto.repo.v1.DocumentPart;
import jakarta.persistence.EntityManager;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Internal document-part admission. No provider I/O, publication, or reclamation.
 * The trusted writer must measure/verify bytes before reporting them here.
 */
public final class DocumentPartAttemptLedger {
    private final Tx tx;
    public DocumentPartAttemptLedger(Tx tx) { this.tx = Objects.requireNonNull(tx); }

    /** Namespace is the exact physical bucket/container, never a drive alias. */
    public record Location(UUID nodeId, String accountId, String backendGeneration, String namespace) {
        public Location {
            Objects.requireNonNull(nodeId, "nodeId");
            text(accountId, 200, "accountId");
            if (accountId.contains("/")) throw new IllegalArgumentException("Account must be one key segment");
            text(backendGeneration, 128, "backendGeneration");
            text(namespace, 1024, "namespace");
        }
    }

    public record PlannedObject(DocumentPart part, String subKey, String objectKey,
            long size, String sha256, String contentType) {
        public PlannedObject {
            Objects.requireNonNull(part, "part");
            Objects.requireNonNull(subKey, "subKey");
            if (part == DocumentPart.UNRECOGNIZED || part == DocumentPart.DOCUMENT_PART_UNSPECIFIED
                    || subKey.length() > 1024 || (part == DocumentPart.DOCUMENT_PART_CHUNKS ? subKey.isBlank() : !subKey.isEmpty()))
                throw new IllegalArgumentException("Invalid document part slot");
            text(objectKey, 2048, "objectKey");
            if (!RepositoryNamespaces.isDocumentPart(objectKey) || RepositoryNamespaces.isArchive(objectKey)
                    || RepositoryNamespaces.isManagedRaw(objectKey))
                throw new IllegalArgumentException("Document part key must use the documents namespace");
            if (size < 0) throw new IllegalArgumentException("Negative part size");
            digest(sha256);
            text(contentType, 1024, "contentType");
        }
    }

    /**
     * The trusted writer mints a fresh UUID before planning keys; this is not an
     * adoption API for existing bytes. Sampled revision zero means the destination
     * was absent. Object order is retained for chunk reconstruction.
     */
    public record Plan(UUID attemptId, Location location, long sampledRevision, Map<UUID, Long> sources, List<PlannedObject> objects) {
        public Plan {
            Objects.requireNonNull(attemptId, "attemptId");
            Objects.requireNonNull(location, "location");
            sources = Map.copyOf(sources);
            objects = List.copyOf(objects);
            if (sampledRevision < 0 || objects.isEmpty() || objects.size() > 10000 || sources.size() > 10000)
                throw new IllegalArgumentException("Invalid document part plan size or revision");
            if (sources.values().stream().anyMatch(revision -> revision <= 0))
                throw new IllegalArgumentException("Source revisions must be positive");
            if (sources.containsKey(location.nodeId()) && sources.get(location.nodeId()) != sampledRevision)
                throw new IllegalArgumentException("Same-node source revision differs from destination");
            var keys = new HashSet<String>();
            var slots = new HashSet<Map.Entry<DocumentPart, String>>();
            int cores = 0;
            String scope = "/documents/" + location.accountId() + "/" + location.nodeId() + "/attempts/" + attemptId + "/";
            for (var object : objects) {
                if (!("/" + object.objectKey()).contains(scope) || object.objectKey().endsWith("/"))
                    throw new IllegalArgumentException("Object key is outside its document attempt");
                if (!keys.add(object.objectKey()) || !slots.add(Map.entry(object.part(), object.subKey())))
                    throw new IllegalArgumentException("Duplicate document part key or slot");
                if (object.part() == DocumentPart.DOCUMENT_PART_CORE) cores++;
            }
            if (cores != 1) throw new IllegalArgumentException("Document part plan requires exactly one CORE");
        }
    }

    public record Attempt(UUID id, Location location, String storageRealm, long sampledRevision,
            UUID token, Instant leaseUntil, String state, int plannedCount) {}

    public static final class FenceException extends RuntimeException {
        public FenceException(String message) { super(message); }
    }

    /** Reserve the complete immutable plan in one transaction before any PUT/COPY. */
    public Attempt begin(Plan plan, Duration lease) {
        Objects.requireNonNull(plan, "plan");
        requireLease(lease);
        return tx.inTransaction(em -> {
            var location = plan.location();
            List<?> realms = em.createNativeQuery("SELECT storage_realm FROM managed_backend_profiles WHERE generation=:generation")
                    .setParameter("generation", location.backendGeneration()).getResultList();
            if (realms.isEmpty()) throw new IllegalArgumentException("Original backend generation is not registered");
            String realm = (String) realms.getFirst();
            UUID id = plan.attemptId();
            em.createNativeQuery("""
                    INSERT INTO document_part_attempts(attempt_id,node_id,account_id,sampled_revision,backend_generation,
                        storage_realm,storage_namespace,planned_count,source_count,lease_token,lease_until,state)
                    VALUES (:id,:node,:account,:revision,:generation,:realm,:namespace,:count,:sources,:token,
                        clock_timestamp()+(:millis * interval '1 millisecond'),'PLANNING')
                    """).setParameter("id", id).setParameter("node", location.nodeId()).setParameter("account", location.accountId())
                    .setParameter("revision", plan.sampledRevision()).setParameter("generation", location.backendGeneration())
                    .setParameter("realm", realm).setParameter("namespace", location.namespace()).setParameter("count", plan.objects().size())
                    .setParameter("sources", plan.sources().size()).setParameter("token", UUID.randomUUID())
                    .setParameter("millis", lease.toMillis()).executeUpdate();
            for (int ordinal = 0; ordinal < plan.objects().size(); ordinal++) {
                var object = plan.objects().get(ordinal);
                em.createNativeQuery("""
                        INSERT INTO document_part_attempt_objects(attempt_id,ordinal,part,sub_key,storage_realm,storage_namespace,
                            object_key,expected_size,expected_sha256,content_type)
                        VALUES (:id,:ordinal,:part,:sub,:realm,:namespace,:key,:size,:sha,:type)
                        """).setParameter("id", id).setParameter("ordinal", ordinal).setParameter("part", object.part().getNumber()).setParameter("sub", object.subKey())
                        .setParameter("realm", realm).setParameter("namespace", location.namespace()).setParameter("key", object.objectKey())
                        .setParameter("size", object.size()).setParameter("sha", object.sha256()).setParameter("type", object.contentType()).executeUpdate();
            }
            for (var source : plan.sources().entrySet()) {
                em.createNativeQuery("INSERT INTO document_part_attempt_sources(attempt_id,source_node_id,revision) VALUES (:id,:node,:revision)")
                        .setParameter("id", id).setParameter("node", source.getKey()).setParameter("revision", source.getValue()).executeUpdate();
            }
            em.createNativeQuery("UPDATE document_part_attempts SET state='STAGING' WHERE attempt_id=:id").setParameter("id", id).executeUpdate();
            return read(em, id, true).orElseThrow();
        });
    }

    public Optional<Attempt> find(UUID id) {
        Objects.requireNonNull(id, "id");
        return tx.readOnly(em -> read(em, id, false));
    }

    public Attempt renew(UUID id, UUID token, Duration lease) {
        requireLease(lease);
        return tx.inTransaction(em -> {
            requireOwner(em, id, token);
            em.createNativeQuery("""
                    UPDATE document_part_attempts SET lease_until=GREATEST(lease_until,clock_timestamp()+(:millis * interval '1 millisecond'))
                    WHERE attempt_id=:id
                    """).setParameter("id", id).setParameter("millis", lease.toMillis()).executeUpdate();
            return read(em, id, true).orElseThrow();
        });
    }

    /** Record trusted measured bytes; VERIFIED means complete byte verification, not document validity. */
    public Attempt verify(UUID id, UUID token, String key, long size, String sha256, String version, String etag) {
        text(key, 2048, "key");
        digest(sha256);
        return tx.inTransaction(em -> {
            var attempt = requireOwner(em, id, token);
            List<?> rows = em.createNativeQuery("""
                    SELECT expected_size,expected_sha256,verified,provider_version,etag
                    FROM document_part_attempt_objects WHERE attempt_id=:id AND object_key=:key
                    """).setParameter("id", id).setParameter("key", key).getResultList();
            if (rows.isEmpty()) throw new FenceException("Object is not in the admitted plan");
            Object[] row = (Object[]) rows.getFirst();
            if (((Number) row[0]).longValue() != size || !sha256.equals(row[1]))
                throw new FenceException("Observed document part differs from admitted identity");
            if ((Boolean) row[2]) {
                if (!Objects.equals(version, row[3]) || !Objects.equals(etag, row[4]))
                    throw new FenceException("Verified document part provider identity differs");
                return attempt;
            }
            em.createNativeQuery("""
                    UPDATE document_part_attempt_objects SET verified=true,provider_version=:version,etag=:etag
                    WHERE attempt_id=:id AND object_key=:key
                    """).setParameter("version", version).setParameter("etag", etag).setParameter("id", id).setParameter("key", key).executeUpdate();
            em.createNativeQuery("""
                    UPDATE document_part_attempts SET state='VERIFIED' WHERE attempt_id=:id
                    AND NOT EXISTS (SELECT 1 FROM document_part_attempt_objects WHERE attempt_id=:id AND NOT verified)
                    """).setParameter("id", id).executeUpdate();
            return read(em, id, true).orElseThrow();
        });
    }

    private static Attempt requireOwner(EntityManager em, UUID id, UUID token) {
        Objects.requireNonNull(id, "id");
        var attempt = read(em, id, true).orElseThrow(() -> new FenceException("Document part attempt is missing"));
        Instant now = em.unwrap(org.hibernate.Session.class).createNativeQuery("SELECT clock_timestamp()", Instant.class).getSingleResult();
        if (!attempt.token().equals(token) || !attempt.leaseUntil().isAfter(now)
                || !(attempt.state().equals("STAGING") || attempt.state().equals("VERIFIED")))
            throw new FenceException("Document part attempt lease expired or belongs to another writer");
        return attempt;
    }

    private static Optional<Attempt> read(EntityManager em, UUID id, boolean lock) {
        List<?> rows = em.createNativeQuery("""
                SELECT attempt_id,node_id,account_id,backend_generation,storage_namespace,storage_realm,
                       sampled_revision,lease_token,EXTRACT(EPOCH FROM lease_until),state,planned_count
                FROM document_part_attempts WHERE attempt_id=:id
                """ + (lock ? " FOR UPDATE" : "")).setParameter("id", id).getResultList();
        if (rows.isEmpty()) return Optional.empty();
        Object[] r = (Object[]) rows.getFirst();
        var epoch = (java.math.BigDecimal) r[8];
        long seconds = epoch.longValue();
        Instant until = Instant.ofEpochSecond(seconds, epoch.subtract(java.math.BigDecimal.valueOf(seconds)).movePointRight(9).intValueExact());
        return Optional.of(new Attempt((UUID) r[0], new Location((UUID) r[1], (String) r[2], (String) r[3], (String) r[4]),
                (String) r[5], ((Number) r[6]).longValue(), (UUID) r[7], until, (String) r[9], ((Number) r[10]).intValue()));
    }

    private static void requireLease(Duration lease) {
        Objects.requireNonNull(lease, "lease");
        if (lease.compareTo(Duration.ofSeconds(1)) < 0 || lease.compareTo(Duration.ofDays(1)) > 0)
            throw new IllegalArgumentException("Lease must be between one second and one day");
    }
    private static void text(String value, int max, String name) {
        if (value == null || value.isBlank() || value.length() > max) throw new IllegalArgumentException("Invalid " + name);
    }
    private static void digest(String value) {
        if (value == null || !value.matches("[0-9a-f]{64}")) throw new IllegalArgumentException("Invalid SHA-256");
    }
}
