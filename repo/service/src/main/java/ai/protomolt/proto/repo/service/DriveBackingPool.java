package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.blob.cache.CachingBlobStore;
import ai.protomolt.proto.repo.blob.spi.BlobCapability;
import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.blob.spi.BlobStores;
import ai.protomolt.proto.repo.blob.spi.NamespaceProvisioner;
import ai.protomolt.proto.repo.blob.spi.ObjectReclaimer;
import ai.protomolt.proto.repo.blob.spi.OpenedBlobStore;
import ai.protomolt.proto.repo.container.ledger.DriveRecord;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.v1.DriveCacheConfig;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/** Process-owned provider handles, shared by complete resolved backend configuration. */
final class DriveBackingPool implements AutoCloseable {
    private static final String TEMPORARY_PREFIX = "protomolt:temporary:v1:";
    private static final String TEMPORARY_NAMESPACE = "redis-expiring:";

    record Backing(String provider, String endpoint, BlobStore store,
            NamespaceProvisioner namespaces, ObjectReclaimer reclaimer, String keyPrefix, String writePolicy,
            String region, boolean pathStyle, int ttlSeconds) {
        Backing(String provider, String endpoint, BlobStore store,
                NamespaceProvisioner namespaces, ObjectReclaimer reclaimer) {
            this(provider, endpoint, store, namespaces, reclaimer, "", "replace", "", false);
        }
        Backing(String provider, String endpoint, BlobStore store, NamespaceProvisioner namespaces,
                ObjectReclaimer reclaimer, String keyPrefix, String writePolicy) {
            this(provider, endpoint, store, namespaces, reclaimer, keyPrefix, writePolicy, "", false);
        }
        Backing(String provider, String endpoint, BlobStore store, NamespaceProvisioner namespaces,
                ObjectReclaimer reclaimer, String keyPrefix, String writePolicy, String region, boolean pathStyle) {
            this(provider, endpoint, store, namespaces, reclaimer, keyPrefix, writePolicy, region, pathStyle, 0);
        }
        boolean temporary() {
            return ttlSeconds > 0;
        }
        String namespace(String bucket, String prefix) {
            if ("s3".equals(provider)) return region.isEmpty() ? bucket + "/" + prefix
                    : "s3:" + encode(region) + ":" + (pathStyle ? "path:" : "virtual:") + bucket + "/" + prefix;
            String retention = temporary() ? TEMPORARY_NAMESPACE + ttlSeconds + ":" : "redis:";
            return retention + writePolicy + ":" + encode(keyPrefix) + ":" + encode(bucket) + ":" + encode(prefix);
        }
    }
    private record StoreKey(String provider, Map<String, String> options) {}
    private record CacheSpec(String provider, Map<String, String> options, int ttlSeconds, long maxCacheableBytes) {}
    private record Key(String provider, Map<String, String> options, CacheSpec cache) {}
    private final RepoServiceConfig config;
    private final BlobStores providers;
    private final Backing defaults;
    private final Function<String, String> environment;
    private final boolean bounded;
    private final OwnedResources owned = new OwnedResources();
    private final Map<StoreKey, OpenedBlobStore> authoritativeHandles = new HashMap<>();
    private final Map<StoreKey, OpenedBlobStore> cacheHandles = new HashMap<>();
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
        if (config.hasS3Backing()) {
            Map<String, String> hostS3Options = Map.copyOf(RepoStorageWiring.s3Options(config));
            CacheSpec hostCache = config.hasRedisCache()
                    ? new CacheSpec(config.redisCompatibleProvider(), Map.of(
                            "uri", config.redisUri(),
                            "ttl-seconds", Integer.toString(config.redisTtlSeconds()),
                            "max-object-bytes", Long.toString(config.redisMaxObjectBytes()),
                            "key-prefix", "",
                            "write-policy", "replace"),
                            config.redisTtlSeconds(),
                            config.redisMaxObjectBytes())
                    : null;
            backings.put(new Key("s3", hostS3Options, hostCache), this.defaults);
            if (!config.hasRedisCache()) {
                authoritativeHandles.put(new StoreKey("s3", hostS3Options), new OpenedBlobStore(
                        defaults.blobStore(),
                        () -> {},
                        Set.of(BlobCapability.NON_EXPIRING_WRITES,
                                BlobCapability.PHYSICAL_RECLAMATION,
                                BlobCapability.STREAMING_WRITE),
                        defaults.namespaces(),
                        defaults.reclaimer()));
            }
        } else {
            var options = Map.of("uri", config.redisUri(),
                "ttl-seconds", Integer.toString(config.redisTtlSeconds()),
                "max-object-bytes", Long.toString(config.redisMaxObjectBytes()), "key-prefix", "",
                "write-policy", bounded ? "create-only" : "replace");
            backings.put(new Key(provider, options, null), this.defaults);
            if ("valkey".equals(provider)) backings.put(new Key("redis", options, null), this.defaults);
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
        CacheSpec cacheSpec = null;
        switch (stored.getConfigCase()) {
            case S3 -> {
                if (!"s3".equals(provider)) throw invalid("S3 configuration requires s3 provider");
                var s3 = stored.getS3();
                options.putAll(RepoStorageWiring.s3Options(config));
                options.put("endpoint", s3.getEndpointOverride());
                options.put("path-style", Boolean.toString(s3.getForcePathStyle()));
                options.put("region", blank(drive.region) ? config.s3Region() : drive.region);
                if (!blank(drive.credentialsRef)) credentials(options, drive.credentialsRef);
                else if (!s3.getEndpointOverride().equals(text(config.s3Endpoint())))
                    throw invalid("A different S3 endpoint requires an explicit credentials_ref");
                if (s3.hasReadThroughCache()) {
                    cacheSpec = resolveCacheSpec(s3.getReadThroughCache());
                }
            }
            case REDIS -> {
                if (!Set.of("redis", "valkey").contains(provider))
                    throw invalid("Redis configuration requires redis or valkey provider");
                var redis = stored.getRedis();
                int ttl = redis.getTtlSeconds();
                if (ttl < 0)
                    throw invalid("Redis ttl_seconds must not be negative");
                boolean temporary = ttl > 0;
                if (!temporary && redis.getKeyPrefix().startsWith(TEMPORARY_PREFIX))
                    throw invalid("Redis key_prefix uses the reserved temporary-drive prefix");
                if (temporary && config.managedStorage().retentionQualified() && !bounded)
                    throw invalid("Retention-qualified managed storage refuses expiring drives; "
                            + "temporary drives are not qualified for retained ingestion");
                if (temporary && bounded)
                    throw invalid("Bounded create-only storage requires zero TTL; temporary drives are not supported");
                options.put("uri", redisCredentials(redis.getUri(), drive.credentialsRef));
                options.put("ttl-seconds", Integer.toString(ttl));
                options.put("max-object-bytes", Long.toString(redis.getMaxObjectBytes()));
                options.put("key-prefix", temporary ? temporaryKeyPrefix(redis.getKeyPrefix(), ttl) : redis.getKeyPrefix());
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
        var key = new Key(provider, Map.copyOf(options), cacheSpec);
        return backings.computeIfAbsent(key, selected -> open(selected));
    }

    private CacheSpec resolveCacheSpec(DriveCacheConfig cache) {
        String cacheProvider = blank(cache.getProvider()) ? "valkey" : cache.getProvider();
        if (!Set.of("valkey", "redis").contains(cacheProvider))
            throw invalid("Drive read_through_cache provider must be valkey or redis");
        if (blank(cache.getUri()))
            throw invalid("Drive read_through_cache requires a non-blank uri");
        int ttl = cache.getTtlSeconds();
        if (ttl <= 0)
            throw invalid("Drive read_through_cache ttl_seconds must be positive");
        long maxBytes = cache.getMaxCacheableBytes();
        if (maxBytes <= 0)
            throw invalid("Drive read_through_cache max_cacheable_bytes must be positive");
        String keyPrefix = cache.getKeyPrefix();
        if (keyPrefix.startsWith(TEMPORARY_PREFIX))
            throw invalid("Drive read_through_cache key_prefix uses the reserved temporary-drive prefix");
        String resolvedUri = redisCredentials(cache.getUri(), cache.getCredentialsRef());
        var cacheOptions = Map.of(
                "uri", resolvedUri,
                "ttl-seconds", Integer.toString(ttl),
                "max-object-bytes", Long.toString(maxBytes),
                "key-prefix", keyPrefix,
                "write-policy", "replace");
        return new CacheSpec(cacheProvider, cacheOptions, ttl, maxBytes);
    }

    /** Compatibility identity emitted by the former one-backend host; never rewrites it. */
    ai.protomolt.proto.repo.v2.StackRecord legacyIdentity(DriveRecord drive, Backing backing, String prefix) {
        if (backing != defaults) return null;
        return ai.protomolt.proto.repo.v2.StackRecord.newBuilder().setStackId("stack-" + drive.driveId)
                .setProvider(config.driveProvider()).setEndpoint(text(config.s3Endpoint()))
                .setNamespace(drive.bucket + "/" + prefix).build();
    }

    private Backing open(Key key) {
        int ttlSeconds = "s3".equals(key.provider()) ? 0
                : Integer.parseInt(key.options().getOrDefault("ttl-seconds", "0"));
        StoreKey authKey = new StoreKey(key.provider(), key.options());
        boolean newAuthHandle = !authoritativeHandles.containsKey(authKey);
        OpenedBlobStore handle = newAuthHandle
                ? openAuthoritativeHandle(authKey, ttlSeconds)
                : authoritativeHandles.get(authKey);
        try {
            String endpoint = "s3".equals(key.provider())
                    ? s3Endpoint(key.options().get("endpoint"), key.options().get("region"),
                            Boolean.parseBoolean(key.options().get("path-style")))
                    : endpoint(key.options().get("uri"));
            String region = key.options().getOrDefault("region", "");
            boolean pathStyle = Boolean.parseBoolean(key.options().get("path-style"));
            BlobStore store = handle.store();
            ObjectReclaimer reclaimer = handle.reclaimer();
            if (key.cache() != null) {
                StoreKey cacheKey = new StoreKey(key.cache().provider(), key.cache().options());
                OpenedBlobStore cacheHandle = cacheHandles.get(cacheKey);
                if (cacheHandle == null) {
                    cacheHandle = openCacheHandle(cacheKey);
                    cacheHandles.put(cacheKey, cacheHandle);
                    owned.add(cacheHandle);
                }
                var cachingStore = new CachingBlobStore(
                        handle.store(),
                        cacheHandle.store(),
                        key.cache().ttlSeconds(),
                        key.cache().maxCacheableBytes(),
                        s3CacheNamespace(endpoint, region, pathStyle));
                store = cachingStore;
                reclaimer = cachingStore.reclaimer(handle.reclaimer());
            }
            var backing = new Backing(key.provider(), endpoint,
                    store, handle::ensureNamespace, reclaimer,
                    key.options().getOrDefault("key-prefix", ""), key.options().getOrDefault("write-policy", "replace"),
                    region, pathStyle, ttlSeconds);
            if (newAuthHandle) {
                authoritativeHandles.put(authKey, handle);
                owned.add(handle);
            }
            return backing;
        } catch (RuntimeException | Error failure) {
            if (newAuthHandle) {
                try { handle.close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
            }
            throw failure;
        }
    }

    private OpenedBlobStore openAuthoritativeHandle(StoreKey key, int ttlSeconds) {
        var handle = providers.open(key.provider(), key.options());
        try {
            // A positive per-drive TTL selects a temporary drive: its writes expire, so
            // OBJECT_EXPIRY replaces NON_EXPIRING_WRITES in the qualification gate. The
            // host default and zero-TTL drives keep the durable retention gate.
            var required = ttlSeconds > 0
                    ? Set.of(BlobCapability.OBJECT_EXPIRY, BlobCapability.PHYSICAL_RECLAMATION)
                    : config.managedStorage().retentionQualified() && !bounded
                            ? Set.of(BlobCapability.NON_EXPIRING_WRITES, BlobCapability.PHYSICAL_RECLAMATION,
                                    BlobCapability.STREAMING_WRITE)
                            : Set.of(BlobCapability.NON_EXPIRING_WRITES, BlobCapability.PHYSICAL_RECLAMATION);
            if (!handle.capabilities().containsAll(required))
                throw invalid("Drive provider lacks required repository retention capabilities");
            return handle;
        } catch (RuntimeException | Error failure) {
            try { handle.close(); } catch (Exception cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    private OpenedBlobStore openCacheHandle(StoreKey key) {
        final OpenedBlobStore handle;
        try {
            handle = providers.open(key.provider(), key.options());
        } catch (IllegalArgumentException invalidConfig) {
            throw invalid("Invalid drive read_through_cache configuration: " + invalidConfig.getMessage());
        }
        try {
            if (!handle.capabilities().containsAll(
                    Set.of(BlobCapability.OBJECT_EXPIRY, BlobCapability.PHYSICAL_RECLAMATION)))
                throw invalid("Drive read_through_cache provider lacks required expiry and reclamation capabilities");
            return handle;
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

    static String s3Endpoint(String value, String region, boolean pathStyle) {
        if (blank(value)) return "";
        var uri = URI.create(value); // already validated by the opened S3 provider
        // S3BackendIdentity deliberately covers origins; the unmanaged provider
        // also supports explicit path prefixes, whose exact spelling stays identity.
        if (uri.getRawPath() != null && !uri.getRawPath().isEmpty() && !uri.getRawPath().equals("/")) return value;
        return ai.protomolt.proto.repo.blob.s3.S3BackendIdentity.of(value, region, pathStyle).location().get("endpoint");
    }

    /** Canonical physical backing identity isolating cache entries across S3 backends. */
    static String s3CacheNamespace(String endpoint, String region, boolean pathStyle) {
        return "s3:" + encode(text(endpoint)) + ":" + encode(text(region)) + ":" + (pathStyle ? "path" : "virtual");
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

    /** Encodes temporary identity under a prefix rejected for durable drive configurations. */
    static String temporaryKeyPrefix(String configuredPrefix, int ttlSeconds) {
        if (ttlSeconds <= 0) throw new IllegalArgumentException("Temporary drives need a positive TTL");
        return TEMPORARY_PREFIX + ttlSeconds + ":" + encode(configuredPrefix);
    }

    static boolean temporaryNamespace(String namespace) {
        return namespace.startsWith(TEMPORARY_NAMESPACE);
    }

    private static String encode(String value) {
        return java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(
                value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
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
        authoritativeHandles.clear();
        cacheHandles.clear();
        owned.close();
    }
}
