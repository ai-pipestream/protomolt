package ai.protomolt.proto.repo.container.ledger;

import java.net.URI;
import java.util.Objects;
import java.util.Optional;
import ai.protomolt.proto.repo.blob.spi.BackendIdentity;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import java.util.Map;
import java.util.HashMap;

/** Durable generation bindings. Provider credentials and clients belong to the host. */
public final class ManagedBackendLedger {
    /** Delegates endpoint resolution to the SDK; never synthesize an AWS hostname. */
    private static final String SDK_DEFAULT = "SDK_DEFAULT";
    private final Tx tx;
    public ManagedBackendLedger(Tx tx) { this.tx = Objects.requireNonNull(tx); }

    /** Identity persistence does not qualify a provider for managed streaming. */
    public record Profile(BackendIdentity identity, String storageRealm) {
        public Profile {
            Objects.requireNonNull(identity, "identity");
            requireIdentifier(storageRealm, "storage realm");
        }

        // Decodes only the immutable pre-V17 SQL shape. New identities come from providers.
        private static BackendIdentity legacyIdentity(String provider, String endpoint, String region, boolean pathStyle) {
            if (!"s3".equals(provider)) throw new IllegalArgumentException("Unsupported managed backing provider");
            requireIdentifier(region, "region");
            if (!SDK_DEFAULT.equals(endpoint)) {
                URI uri;
                try { uri = URI.create(Objects.requireNonNull(endpoint)); }
                catch (RuntimeException invalid) { throw new IllegalArgumentException("Invalid managed backend endpoint"); }
                if (!("http".equals(uri.getScheme()) || "https".equals(uri.getScheme()))
                        || uri.getHost() == null || uri.getUserInfo() != null || uri.getQuery() != null || uri.getFragment() != null)
                    throw new IllegalArgumentException("Managed backend endpoint must be an HTTP origin without credentials, query or fragment");
                if (uri.getRawPath() != null && !uri.getRawPath().isEmpty() && !"/".equals(uri.getRawPath()))
                    throw new IllegalArgumentException("Managed backend endpoint must not contain a path");
                String host = uri.getHost().toLowerCase(java.util.Locale.ROOT);
                int port = uri.getPort();
                if (("https".equals(uri.getScheme()) && port == 443) || ("http".equals(uri.getScheme()) && port == 80)) port = -1;
                endpoint = uri.getScheme() + "://" + host + (port == -1 ? "" : ":" + port);
            }
            return new BackendIdentity("s3", "s3/v1", Map.of("endpoint", endpoint, "region", region,
                    "path-style", Boolean.toString(pathStyle)));
        }
    }

    /** Atomic insert-or-check: concurrent disagreeing registrations cannot both succeed. */
    public void bind(String generation, Profile profile) {
        requireIdentifier(generation, "backend generation");
        Objects.requireNonNull(profile);
        tx.inTransaction(em -> {
            em.createNativeQuery("""
                    INSERT INTO managed_backend_profiles(generation,provider,identity_schema,identity_json,storage_realm)
                    VALUES (:id,:provider,:schema,CAST(:location AS jsonb),:realm) ON CONFLICT (generation) DO NOTHING
                    """).setParameter("id", generation).setParameter("provider", profile.identity().provider())
                    .setParameter("schema", profile.identity().schema()).setParameter("location", encode(profile.identity()))
                    .setParameter("realm", profile.storageRealm()).executeUpdate();
            var rows = em.createNativeQuery("SELECT provider,endpoint,region,path_style,storage_realm,identity_schema,CAST(identity_json AS text) FROM managed_backend_profiles WHERE generation=:id")
                    .setParameter("id", generation).getResultList();
            if (rows.size() != 1 || !profile.equals(decode((Object[]) rows.getFirst())))
                throw new IllegalStateException("Managed backend generation is already bound to another physical profile");
        });
    }

    public Optional<Profile> find(String generation) {
        requireIdentifier(generation, "backend generation");
        return tx.inTransaction(em -> { return find(em, generation); });
    }

    static Optional<Profile> find(jakarta.persistence.EntityManager em, String generation) {
        var rows = em.createNativeQuery("SELECT provider,endpoint,region,path_style,storage_realm,identity_schema,CAST(identity_json AS text) FROM managed_backend_profiles WHERE generation=:id")
                .setParameter("id", generation).getResultList();
        return rows.isEmpty() ? Optional.empty() : Optional.of(decode((Object[]) rows.getFirst()));
    }

    private static Profile decode(Object[] row) {
        if (row[5] == null)
            return new Profile(Profile.legacyIdentity((String) row[0], (String) row[1], (String) row[2], (Boolean) row[3]), (String) row[4]);
        var fields = Struct.newBuilder();
        try { JsonFormat.parser().merge((String) row[6], fields); }
        catch (com.google.protobuf.InvalidProtocolBufferException invalid) {
            throw new IllegalStateException("Invalid persisted backend identity", invalid);
        }
        var location = new HashMap<String, String>();
        fields.getFieldsMap().forEach((key, value) -> {
            if (value.getKindCase() != Value.KindCase.STRING_VALUE)
                throw new IllegalStateException("Persisted backend identity fields must be strings");
            location.put(key, value.getStringValue());
        });
        return new Profile(new BackendIdentity((String) row[0], (String) row[5], location), (String) row[4]);
    }

    private static String encode(BackendIdentity identity) {
        var fields = Struct.newBuilder();
        identity.location().forEach((key, value) -> fields.putFields(key, Value.newBuilder().setStringValue(value).build()));
        try { return JsonFormat.printer().print(fields); }
        catch (com.google.protobuf.InvalidProtocolBufferException invalid) {
            throw new IllegalArgumentException("Cannot encode backend identity", invalid);
        }
    }

    private static void requireIdentifier(String value, String field) {
        if (value == null || !value.matches("[A-Za-z0-9][A-Za-z0-9._:-]{0,127}"))
            throw new IllegalArgumentException("Invalid " + field);
    }
}
