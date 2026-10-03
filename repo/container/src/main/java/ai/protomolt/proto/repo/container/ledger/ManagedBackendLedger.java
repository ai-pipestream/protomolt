package ai.protomolt.proto.repo.container.ledger;

import java.net.URI;
import java.util.Objects;
import java.util.Optional;

/** Durable generation bindings. Provider credentials and clients belong to the host. */
public final class ManagedBackendLedger {
    /** Delegates endpoint resolution to the SDK; never synthesize an AWS hostname. */
    public static final String SDK_DEFAULT = "SDK_DEFAULT";
    private final Tx tx;
    public ManagedBackendLedger(Tx tx) { this.tx = Objects.requireNonNull(tx); }

    /** Only S3 backing stores are currently qualified for managed streaming. */
    public record Profile(String provider, String endpoint, String region, boolean pathStyle, String storageRealm) {
        public Profile {
            if (!"s3".equals(provider)) throw new IllegalArgumentException("Unsupported managed backing provider");
            requireIdentifier(region, "region");
            requireIdentifier(storageRealm, "storage realm");
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
        }
    }

    /** Atomic insert-or-check: concurrent disagreeing registrations cannot both succeed. */
    public void bind(String generation, Profile profile) {
        requireIdentifier(generation, "backend generation");
        Objects.requireNonNull(profile);
        tx.inTransaction(em -> {
            em.createNativeQuery("""
                    INSERT INTO managed_backend_profiles(generation,provider,endpoint,region,path_style,storage_realm)
                    VALUES (:id,:provider,:endpoint,:region,:style,:realm) ON CONFLICT (generation) DO NOTHING
                    """).setParameter("id", generation).setParameter("provider", profile.provider())
                    .setParameter("endpoint", profile.endpoint()).setParameter("region", profile.region())
                    .setParameter("style", profile.pathStyle()).setParameter("realm", profile.storageRealm()).executeUpdate();
            var rows = em.createNativeQuery("SELECT provider,endpoint,region,path_style,storage_realm FROM managed_backend_profiles WHERE generation=:id")
                    .setParameter("id", generation).getResultList();
            if (rows.size() != 1 || !profile.equals(decode((Object[]) rows.getFirst())))
                throw new IllegalStateException("Managed backend generation is already bound to another physical profile");
        });
    }

    public Optional<Profile> find(String generation) {
        requireIdentifier(generation, "backend generation");
        return tx.inTransaction(em -> {
            var rows = em.createNativeQuery("SELECT provider,endpoint,region,path_style,storage_realm FROM managed_backend_profiles WHERE generation=:id")
                    .setParameter("id", generation).getResultList();
            return rows.isEmpty() ? Optional.empty() : Optional.of(decode((Object[]) rows.getFirst()));
        });
    }

    private static Profile decode(Object[] row) {
        return new Profile((String) row[0], (String) row[1], (String) row[2], (Boolean) row[3], (String) row[4]);
    }

    private static void requireIdentifier(String value, String field) {
        if (value == null || !value.matches("[A-Za-z0-9][A-Za-z0-9._:-]{0,127}"))
            throw new IllegalArgumentException("Invalid " + field);
    }
}
