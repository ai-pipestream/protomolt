package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.blob.spi.BlobCapability;
import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.blob.spi.BlobStores;
import ai.protomolt.proto.repo.blob.spi.NamespaceProvisioner;
import ai.protomolt.proto.repo.blob.spi.ObjectReclaimer;
import ai.protomolt.proto.repo.container.ledger.DriveRecord;
import ai.protomolt.proto.repo.spi.RepositoryException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/** Process-owned provider handles, shared by complete resolved backend configuration. */
final class DriveBackingPool implements AutoCloseable {
    record Backing(String provider, String endpoint, BlobStore store,
            NamespaceProvisioner namespaces, ObjectReclaimer reclaimer, String keyPrefix, String writePolicy,
            String region, boolean pathStyle) {
        Backing(String provider, String endpoint, BlobStore store,
                NamespaceProvisioner namespaces, ObjectReclaimer reclaimer) {
            this(provider, endpoint, store, namespaces, reclaimer, "", "replace", "", false);
        }
        Backing(String provider, String endpoint, BlobStore store, NamespaceProvisioner namespaces,
                ObjectReclaimer reclaimer, String keyPrefix, String writePolicy) {
            this(provider, endpoint, store, namespaces, reclaimer, keyPrefix, writePolicy, "", false);
        }
        String namespace(String bucket, String prefix) {
            if ("s3".equals(provider)) return region.isEmpty() ? bucket + "/" + prefix
                    : "s3:" + encode(region) + ":" + (pathStyle ? "path:" : "virtual:") + bucket + "/" + prefix;
            // Encode each physical namespace dimension independently; separators in
            // user-provided prefixes or bucket names cannot alias another tuple.
            return "redis:" + writePolicy + ":" + encode(keyPrefix) + ":" + encode(bucket) + ":" + encode(prefix);
        }
        private static String encode(String value) {
            return java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(
                    value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        }
    }
    private record Key(String provider, Map<String, String> options) {}
    private final RepoServiceConfig config;
    private final BlobStores providers;
    private final Backing defaults;
    private final Function<String, String> environment;
    private final boolean bounded;
    private final OwnedResources owned = new OwnedResources();
    private final Map<Key, Backing> backings = new HashMap<>();
    private boolean closed;

    DriveBackingPool(RepoServiceConfig config, BlobStores providers, RepoStorageWiring.Wired defaults) {
        this(config, providers, defaults, false, System::getenv);
    }

    DriveBackingPool(RepoServiceConfig config, BlobStores providers, RepoStorageWiring.Wired defaults,
            Function<String, String> environment) {
        this(config, providers, defaults, false, environment);
    }

    DriveBackingPool(RepoServiceConfig config, BlobStores providers, RepoStorageWiring.Wired defaults,
            boolean bounded, Function<String, String> environment) {
        this.config = config;
        this.bounded = bounded;
        this.providers = providers;
        this.environment = environment;
        String provider = config.hasS3Backing() ? "s3" : config.redisCompatibleProvider();
        this.defaults = new Backing(provider, config.hasS3Backing()
                ? s3Endpoint(config.s3Endpoint(), config.s3Region(), config.s3Endpoint() != null) : endpoint(config.redisUri()), defaults.blobStore(),
                defaults.namespaces(), defaults.reclaimer(), "", bounded ? "create-only" : "replace",
                config.hasS3Backing() ? config.s3Region() : "", config.s3Endpoint() != null);
        if (config.hasS3Backing())
            backings.put(new Key("s3", Map.copyOf(RepoStorageWiring.s3Options(config))), this.defaults);
        else {
            var options = Map.of("uri", config.redisUri(),
                "ttl-seconds", Integer.toString(config.redisTtlSeconds()),
                "max-object-bytes", Long.toString(config.redisMaxObjectBytes()), "key-prefix", "",
                "write-policy", bounded ? "create-only" : "replace");
            backings.put(new Key(provider, options), this.defaults);
            if ("valkey".equals(provider)) backings.put(new Key("redis", options), this.defaults);
        }
    }

    synchronized Backing resolve(DriveRecord drive) {
        if (closed) throw new IllegalStateException("Drive backing pool is closed");
        var stored = drive.readProviderConfig();
        if (stored != null && stored.getConfigCase()
                == ai.protomolt.proto.repo.v1.DriveProviderConfig.ConfigCase.CONFIG_NOT_SET
                && stored.getOptionsMap().isEmpty() && stored.getUnknownFields().asMap().isEmpty()) stored = null;
        if (stored == null) {
            if (blank(drive.credentialsRef)) {
                new SelectedDriveBackend(config).accept(drive);
                return defaults;
            }
            // Credentials may change independently of endpoint selection. Validate
            // the inherited identity before resolving its explicit credential profile.
            var inherited = new DriveRecord();
            inherited.provider = drive.provider;
            inherited.region = drive.region;
            new SelectedDriveBackend(config).accept(inherited);
            var builder = ai.protomolt.proto.repo.v1.DriveProviderConfig.newBuilder();
            stored = config.hasS3Backing()
                    ? builder.setS3(ai.protomolt.proto.repo.v1.S3DriveConfig.newBuilder()
                            .setEndpointOverride(text(config.s3Endpoint()))
                            .setForcePathStyle(config.s3Endpoint() != null)).build()
                    : builder.setRedis(ai.protomolt.proto.repo.v1.RedisDriveConfig.newBuilder()
                            .setUri(config.redisUri()).setTtlSeconds(config.redisTtlSeconds())
                            .setMaxObjectBytes(config.redisMaxObjectBytes())).build();
        }
        String provider = drive.provider;
        var options = new HashMap<String, String>();
        switch (stored.getConfigCase()) {
            case S3 -> {
                if (!"s3".equals(provider)) throw invalid("S3 configuration requires s3 provider");
                options.putAll(RepoStorageWiring.s3Options(config));
                options.put("endpoint", stored.getS3().getEndpointOverride());
                options.put("path-style", Boolean.toString(stored.getS3().getForcePathStyle()));
                options.put("region", blank(drive.region) ? config.s3Region() : drive.region);
                if (!blank(drive.credentialsRef)) credentials(options, drive.credentialsRef);
                else if (!stored.getS3().getEndpointOverride().equals(text(config.s3Endpoint())))
                    throw invalid("A different S3 endpoint requires an explicit credentials_ref");
            }
            case REDIS -> {
                if (!Set.of("redis", "valkey").contains(provider))
                    throw invalid("Redis configuration requires redis or valkey provider");
                var redis = stored.getRedis();
                if (redis.getTtlSeconds() != 0)
                    throw invalid("Repository drives must retain bytes until explicit purge; expiring drives are not qualified");
                options.put("uri", redisCredentials(redis.getUri(), drive.credentialsRef));
                options.put("ttl-seconds", "0");
                options.put("max-object-bytes", Long.toString(redis.getMaxObjectBytes()));
                options.put("key-prefix", redis.getKeyPrefix());
                options.put("write-policy", bounded ? "create-only" : "replace");
            }
            default -> throw invalid("A drive must explicitly select its provider configuration");
        }
        // Only tuning options may augment the typed identity and credential selection.
        for (var entry : stored.getOptionsMap().entrySet()) {
            if (!Set.of("api-call-timeout-ms", "api-attempt-timeout-ms", "connection-timeout-ms",
                    "socket-timeout-ms", "conditional-writes").contains(entry.getKey()) || !"s3".equals(provider))
                throw invalid("Unsupported per-drive tuning option: " + entry.getKey());
            options.put(entry.getKey(), entry.getValue());
        }
        var key = new Key(provider, Map.copyOf(options));
        return backings.computeIfAbsent(key, selected -> open(selected));
    }

    /** Compatibility identity emitted by the former one-backend host; never rewrites it. */
    ai.protomolt.proto.repo.v2.StackRecord legacyIdentity(DriveRecord drive, Backing backing, String prefix) {
        if (backing != defaults) return null;
        return ai.protomolt.proto.repo.v2.StackRecord.newBuilder().setStackId("stack-" + drive.driveId)
                .setProvider(config.driveProvider()).setEndpoint(text(config.s3Endpoint()))
                .setNamespace(drive.bucket + "/" + prefix).build();
    }

    private Backing open(Key key) {
        var handle = providers.open(key.provider(), key.options());
        try {
            var required = config.managedStorage().retentionQualified() && !bounded
                    ? Set.of(BlobCapability.NON_EXPIRING_WRITES, BlobCapability.PHYSICAL_RECLAMATION,
                            BlobCapability.STREAMING_WRITE)
                    : Set.of(BlobCapability.NON_EXPIRING_WRITES, BlobCapability.PHYSICAL_RECLAMATION);
            if (!handle.capabilities().containsAll(required))
                throw invalid("Drive provider lacks required repository retention capabilities");
            var backing = new Backing(key.provider(), "s3".equals(key.provider())
                    ? s3Endpoint(key.options().get("endpoint"), key.options().get("region"),
                            Boolean.parseBoolean(key.options().get("path-style"))) : endpoint(key.options().get("uri")),
                    handle.store(), handle::ensureNamespace, handle.reclaimer(),
                    key.options().getOrDefault("key-prefix", ""), key.options().getOrDefault("write-policy", "replace"),
                    key.options().getOrDefault("region", ""), Boolean.parseBoolean(key.options().get("path-style")));
            owned.add(handle);
            return backing;
        } catch (RuntimeException | Error failure) {
            try { handle.close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    private void credentials(Map<String, String> options, String reference) {
        // The host profile is opt-in when sending credentials to another endpoint.
        if ("host:default".equals(reference)) return;
        options.remove("access-key");
        options.remove("secret-key");
        if (reference.startsWith("env:") && reference.length() > 4) {
            String prefix = reference.substring(4);
            String access = environment.apply(prefix + "_ACCESS_KEY_ID");
            String secret = environment.apply(prefix + "_SECRET_ACCESS_KEY");
            if (blank(access) || blank(secret)) throw invalid("Drive credential environment profile is incomplete");
            options.put("credentials-mode", "static");
            options.put("access-key", access);
            options.put("secret-key", secret);
        } else if (reference.startsWith("profile:") && reference.length() > 8) {
            options.put("credentials-mode", "profile");
            options.put("profile-name", reference.substring(8));
        } else throw invalid("Drive credentials_ref must use env:, profile: or host:default");
    }

    private String redisCredentials(String endpoint, String reference) {
        final URI uri;
        try { uri = URI.create(endpoint); }
        catch (IllegalArgumentException invalid) { throw invalid("Invalid Redis endpoint"); }
        if (uri.getUserInfo() != null)
            throw invalid("Per-drive Redis endpoints must not contain credentials; use credentials_ref");
        if (blank(reference)) return endpoint;
        if (!reference.startsWith("env:") || reference.length() == 4)
            throw invalid("Redis credentials_ref must use env:");
        String prefix = reference.substring(4);
        String password = environment.apply(prefix + "_PASSWORD");
        String username = environment.apply(prefix + "_USERNAME");
        if (blank(password)) throw invalid("Drive credential environment profile is incomplete");
        try {
            return new URI(uri.getScheme(), text(username) + ":" + password,
                    uri.getHost(), uri.getPort(), uri.getPath(), uri.getQuery(), uri.getFragment()).toString();
        } catch (java.net.URISyntaxException invalid) { throw invalid("Invalid Redis credential profile"); }
    }

    private static String s3Endpoint(String value, String region, boolean pathStyle) {
        if (blank(value)) return "";
        var uri = URI.create(value); // already validated by the opened S3 provider
        // S3BackendIdentity deliberately covers origins; the unmanaged provider
        // also supports explicit path prefixes, whose exact spelling stays identity.
        if (uri.getRawPath() != null && !uri.getRawPath().isEmpty() && !uri.getRawPath().equals("/")) return value;
        return ai.protomolt.proto.repo.blob.s3.S3BackendIdentity.of(value, region, pathStyle).location().get("endpoint");
    }

    private static String endpoint(String value) {
        var location = ai.protomolt.proto.repo.blob.redis.RedisBackendIdentity.of(value, "").location();
        try {
            return new URI(location.get("scheme"), null, location.get("host"),
                    Integer.parseInt(location.get("port")), "/" + location.get("database"), null, null).toString();
        } catch (java.net.URISyntaxException invalid) {
            // The identity has already removed credentials, so this cause is safe.
            throw new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION, "Invalid drive endpoint", invalid);
        }
    }

    void requireStorable(DriveRecord drive, Backing backing) {
        String prefix = ai.protomolt.proto.repo.codec.RepositoryNamespaces.under(
                drive.prefix == null ? "" : RepoV2Wiring.stripLeadingSlashes(drive.prefix), "documents");
        ai.protomolt.proto.repo.catalog.CatalogEntries.requireStorableStack(
                ai.protomolt.proto.repo.v2.StackRecord.newBuilder().setStackId("stack-" + drive.driveId)
                        .setProvider(backing.provider()).setEndpoint(backing.endpoint())
                        .setNamespace(backing.namespace(drive.bucket, prefix)).build());
    }

    private static String text(String value) { return value == null ? "" : value; }
    private static boolean blank(String value) { return value == null || value.isBlank(); }
    private static RepositoryException invalid(String message) {
        return new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION, message);
    }
    @Override public synchronized void close() {
        if (closed) return;
        closed = true;
        backings.clear();
        owned.close();
    }
}
