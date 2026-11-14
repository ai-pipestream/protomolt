package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.blob.cache.CachingBlobStore;
import ai.protomolt.proto.repo.blob.redis.RedisBackendIdentity;
import ai.protomolt.proto.repo.blob.s3.S3BackendIdentity;
import ai.protomolt.proto.repo.blob.spi.BlobCapability;
import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.blob.spi.BlobStores;
import ai.protomolt.proto.repo.blob.spi.NamespaceProvisioner;
import ai.protomolt.proto.repo.blob.spi.ObjectReclaimer;
import ai.protomolt.proto.repo.blob.spi.OpenedBlobStore;
import ai.protomolt.proto.repo.catalog.CatalogEntries;
import ai.protomolt.proto.repo.codec.RepositoryNamespaces;
import ai.protomolt.proto.repo.container.ledger.DriveRecord;
import ai.protomolt.proto.repo.spi.RepositoryException;
import ai.protomolt.proto.repo.v1.DriveCacheConfig;
import ai.protomolt.proto.repo.v1.DriveProviderConfig;
import ai.protomolt.proto.repo.v1.RedisDriveConfig;
import ai.protomolt.proto.repo.v1.S3DriveConfig;
import ai.protomolt.proto.repo.v2.StackRecord;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Process-owned provider handles, shared by explicit canonical backend identity without
 * process-wide serialization during slow provider initialization.
 */
final class DriveBackingPool implements AutoCloseable {
    private static final String PROVIDER_S3 = "s3";
    private static final String PROVIDER_REDIS = "redis";
    private static final String PROVIDER_VALKEY = "valkey";
    private static final Set<String> REDIS_COMPATIBLE_PROVIDERS = Set.of(PROVIDER_REDIS, PROVIDER_VALKEY);

    private static final String OPT_URI = "uri";
    private static final String OPT_TTL_SECONDS = "ttl-seconds";
    private static final String OPT_MAX_OBJECT_BYTES = "max-object-bytes";
    private static final String OPT_KEY_PREFIX = "key-prefix";
    private static final String OPT_WRITE_POLICY = "write-policy";
    private static final String OPT_ENDPOINT = "endpoint";
    private static final String OPT_REGION = "region";
    private static final String OPT_PATH_STYLE = "path-style";
    private static final String OPT_CONDITIONAL_WRITES = "conditional-writes";
    private static final String OPT_CREDENTIALS_MODE = "credentials-mode";
    private static final String OPT_ACCESS_KEY = "access-key";
    private static final String OPT_SECRET_KEY = "secret-key";
    private static final String OPT_PROFILE_NAME = "profile-name";

    private static final String WRITE_POLICY_REPLACE = "replace";
    private static final String WRITE_POLICY_CREATE_ONLY = "create-only";
    private static final String HOST_DEFAULT_CREDENTIALS = "host:default";
    private static final String ENV_CREDENTIALS_PREFIX = "env:";
    private static final String PROFILE_CREDENTIALS_PREFIX = "profile:";
    private static final String DOCUMENTS_SUBPATH = "documents";
    private static final int MAX_STORABLE_NAMESPACE_CHARS = 1024;

    private static final Set<String> S3_TUNING_OPTIONS = Set.of(
            "api-call-timeout-ms",
            "api-attempt-timeout-ms",
            "connection-timeout-ms",
            "socket-timeout-ms",
            OPT_CONDITIONAL_WRITES);

    private static final String TEMPORARY_PREFIX = "protomolt:temporary:v1:";
    private static final String TEMPORARY_NAMESPACE = "redis-expiring:";

    /**
     * Explicit canonical identity of an underlying {@link BlobStore} provider handle.
     *
     * <p>Compatible provider aliases ({@code valkey} and {@code redis}) and equivalent
     * normalized endpoint/URI spellings produce identical {@link StoreIdentity} values so
     * they share one physical client handle, while differing credentials, limits, write
     * policies, or timeouts remain distinct.
     */
    record StoreIdentity(String canonicalProvider, Map<String, String> canonicalOptions) {
        StoreIdentity {
            Objects.requireNonNull(canonicalProvider, "canonicalProvider");
            canonicalOptions = Map.copyOf(canonicalOptions);
        }
    }

    /** Canonical identity of a read-through cache configuration fronting an S3 store. */
    record CacheIdentity(StoreIdentity store, int ttlSeconds, long maxCacheableBytes) {
        CacheIdentity {
            Objects.requireNonNull(store, "store");
        }
    }

    /**
     * Complete canonical store configuration of a resolved {@link Backing}, combining its
     * authoritative store identity and optional read-through cache identity.
     */
    record BackingIdentity(StoreIdentity authoritative, CacheIdentity cache) {
        BackingIdentity {
            Objects.requireNonNull(authoritative, "authoritative");
        }
    }

    record Backing(String provider, String endpoint, BlobStore store,
            NamespaceProvisioner namespaces, ObjectReclaimer reclaimer, String keyPrefix, String writePolicy,
            String region, boolean pathStyle, int ttlSeconds, BackingIdentity identity) {
        Backing(String provider, String endpoint, BlobStore store,
                NamespaceProvisioner namespaces, ObjectReclaimer reclaimer) {
            this(provider, endpoint, store, namespaces, reclaimer, "", WRITE_POLICY_REPLACE, "", false);
        }

        Backing(String provider, String endpoint, BlobStore store, NamespaceProvisioner namespaces,
                ObjectReclaimer reclaimer, String keyPrefix, String writePolicy) {
            this(provider, endpoint, store, namespaces, reclaimer, keyPrefix, writePolicy, "", false);
        }

        Backing(String provider, String endpoint, BlobStore store, NamespaceProvisioner namespaces,
                ObjectReclaimer reclaimer, String keyPrefix, String writePolicy, String region, boolean pathStyle) {
            this(provider, endpoint, store, namespaces, reclaimer, keyPrefix, writePolicy, region, pathStyle, 0);
        }

        Backing(String provider, String endpoint, BlobStore store, NamespaceProvisioner namespaces,
                ObjectReclaimer reclaimer, String keyPrefix, String writePolicy,
                String region, boolean pathStyle, int ttlSeconds) {
            this(provider, endpoint, store, namespaces, reclaimer, keyPrefix, writePolicy,
                    region, pathStyle, ttlSeconds, null);
        }

        boolean temporary() {
            return ttlSeconds > 0;
        }

        String namespace(String bucket, String prefix) {
            return formatNamespace(provider, region, pathStyle, ttlSeconds, writePolicy, keyPrefix, bucket, prefix);
        }
    }

    private record BackingKey(String provider, BackingIdentity identity) {}

    private record PhysicalNamespace(String canonicalProvider, String endpoint, String namespace) {}

    private record CacheOpenSpec(
            String provider,
            Map<String, String> rawOptions,
            CacheIdentity identity) {}

    private record OpenSpec(
            String provider,
            Map<String, String> rawOptions,
            CacheOpenSpec cache,
            BackingIdentity identity,
            String endpoint,
            String keyPrefix,
            String writePolicy,
            String region,
            boolean pathStyle,
            int ttlSeconds) {}

    private final RepoServiceConfig config;
    private final BlobStores providers;
    private final Backing defaults;
    private final Function<String, String> environment;
    private final boolean bounded;
    private final OwnedResources owned = new OwnedResources();
    private final SingleFlightHandles authoritativeHandles = new SingleFlightHandles();
    private final SingleFlightHandles cacheHandles = new SingleFlightHandles();
    private final ConcurrentHashMap<BackingKey, Backing> backings = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<PhysicalNamespace, BackingIdentity> namespaceIdentities =
            new ConcurrentHashMap<>();
    private final ReentrantReadWriteLock lifecycleLock = new ReentrantReadWriteLock();
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
        String provider = config.hasS3Backing() ? PROVIDER_S3 : config.redisCompatibleProvider();
        String writePolicy = bounded ? WRITE_POLICY_CREATE_ONLY : WRITE_POLICY_REPLACE;
        if (config.hasS3Backing()) {
            boolean pathStyle = config.s3Endpoint() != null;
            String hostEndpoint = s3Endpoint(config.s3Endpoint(), config.s3Region(), pathStyle);
            Map<String, String> hostS3Options = Map.copyOf(RepoStorageWiring.s3Options(config));
            StoreIdentity hostAuthIdentity = s3StoreIdentity(hostS3Options);
            CacheIdentity hostCacheIdentity = null;
            if (config.hasRedisCache()) {
                Map<String, String> hostCacheOptions = Map.of(
                        OPT_URI, config.redisUri(),
                        OPT_TTL_SECONDS, Integer.toString(config.redisTtlSeconds()),
                        OPT_MAX_OBJECT_BYTES, Long.toString(config.redisMaxObjectBytes()),
                        OPT_KEY_PREFIX, "",
                        OPT_WRITE_POLICY, WRITE_POLICY_REPLACE);
                hostCacheIdentity = new CacheIdentity(
                        redisStoreIdentity(hostCacheOptions, "Invalid host cache endpoint"),
                        config.redisTtlSeconds(),
                        config.redisMaxObjectBytes());
            }
            BackingIdentity hostBackingIdentity = new BackingIdentity(hostAuthIdentity, hostCacheIdentity);
            this.defaults = new Backing(
                    PROVIDER_S3,
                    hostEndpoint,
                    defaults.blobStore(),
                    defaults.namespaces(),
                    defaults.reclaimer(),
                    "",
                    writePolicy,
                    config.s3Region(),
                    pathStyle,
                    0,
                    hostBackingIdentity);
            backings.put(new BackingKey(PROVIDER_S3, hostBackingIdentity), this.defaults);
            if (!config.hasRedisCache()) {
                authoritativeHandles.preseed(hostAuthIdentity, new OpenedBlobStore(
                        defaults.blobStore(),
                        () -> {},
                        Set.of(BlobCapability.NON_EXPIRING_WRITES,
                                BlobCapability.PHYSICAL_RECLAMATION,
                                BlobCapability.STREAMING_WRITE),
                        defaults.namespaces(),
                        defaults.reclaimer()));
            }
        } else {
            String hostEndpoint = endpoint(config.redisUri());
            Map<String, String> hostRedisOptions = Map.of(
                    OPT_URI, config.redisUri(),
                    OPT_TTL_SECONDS, Integer.toString(config.redisTtlSeconds()),
                    OPT_MAX_OBJECT_BYTES, Long.toString(config.redisMaxObjectBytes()),
                    OPT_KEY_PREFIX, "",
                    OPT_WRITE_POLICY, writePolicy);
            StoreIdentity hostAuthIdentity = redisStoreIdentity(hostRedisOptions, "Invalid drive endpoint");
            BackingIdentity hostBackingIdentity = new BackingIdentity(hostAuthIdentity, null);
            this.defaults = new Backing(
                    provider,
                    hostEndpoint,
                    defaults.blobStore(),
                    defaults.namespaces(),
                    defaults.reclaimer(),
                    "",
                    writePolicy,
                    "",
                    false,
                    config.redisTtlSeconds(),
                    hostBackingIdentity);
            backings.put(new BackingKey(provider, hostBackingIdentity), this.defaults);
            authoritativeHandles.preseed(hostAuthIdentity, new OpenedBlobStore(
                    defaults.blobStore(),
                    () -> {},
                    hostCapabilities(config.redisTtlSeconds(), config.managedStorage().retentionQualified() && !bounded),
                    defaults.namespaces(),
                    defaults.reclaimer()));
        }
    }

    Backing resolve(DriveRecord drive) {
        lifecycleLock.readLock().lock();
        try {
            if (closed) {
                throw new IllegalStateException("Drive backing pool is closed");
            }
            return resolveOpen(drive);
        } finally {
            lifecycleLock.readLock().unlock();
        }
    }

    private Backing resolveOpen(DriveRecord drive) {
        var stored = normalizedProviderConfig(drive.readProviderConfig());
        if (stored == null) {
            if (blank(drive.credentialsRef)) {
                new SelectedDriveBackend(config).accept(drive);
                String requestedProvider = blank(drive.provider) ? defaults.provider() : drive.provider;
                PhysicalNamespace ns = physicalNamespace(
                        drive,
                        requestedProvider,
                        defaults.endpoint(),
                        defaults.keyPrefix(),
                        defaults.writePolicy(),
                        defaults.region(),
                        defaults.pathStyle(),
                        defaults.ttlSeconds());
                checkNamespaceCompatible(ns, defaults.identity());
                Backing resolved = requestedProvider.equals(defaults.provider())
                        ? defaults
                        : backings.computeIfAbsent(
                                new BackingKey(requestedProvider, defaults.identity()),
                                ignored -> new Backing(
                                        requestedProvider,
                                        defaults.endpoint(),
                                        defaults.store(),
                                        defaults.namespaces(),
                                        defaults.reclaimer(),
                                        defaults.keyPrefix(),
                                        defaults.writePolicy(),
                                        defaults.region(),
                                        defaults.pathStyle(),
                                        defaults.ttlSeconds(),
                                        defaults.identity()));
                bindNamespace(ns, defaults.identity());
                return resolved;
            }
            // Credentials may change independently of endpoint selection. Validate
            // the inherited identity before resolving its explicit credential profile.
            var inherited = new DriveRecord();
            inherited.provider = drive.provider;
            inherited.region = drive.region;
            new SelectedDriveBackend(config).accept(inherited);
            var builder = DriveProviderConfig.newBuilder();
            stored = config.hasS3Backing()
                    ? builder.setS3(S3DriveConfig.newBuilder()
                            .setEndpointOverride(text(config.s3Endpoint()))
                            .setForcePathStyle(config.s3Endpoint() != null)).build()
                    : builder.setRedis(RedisDriveConfig.newBuilder()
                            .setUri(config.redisUri())
                            .setTtlSeconds(config.redisTtlSeconds())
                            .setMaxObjectBytes(config.redisMaxObjectBytes())).build();
        }
        OpenSpec spec = buildOpenSpec(drive, stored);
        PhysicalNamespace ns = physicalNamespace(
                drive,
                spec.provider(),
                spec.endpoint(),
                spec.keyPrefix(),
                spec.writePolicy(),
                spec.region(),
                spec.pathStyle(),
                spec.ttlSeconds());
        checkNamespaceCompatible(ns, spec.identity());
        BackingKey backingKey = new BackingKey(spec.provider(), spec.identity());
        Backing resolved = backings.get(backingKey);
        if (resolved == null) {
            Backing opened = open(spec);
            Backing existing = backings.putIfAbsent(backingKey, opened);
            resolved = existing != null ? existing : opened;
        }
        bindNamespace(ns, spec.identity());
        return resolved;
    }

    private OpenSpec buildOpenSpec(DriveRecord drive, DriveProviderConfig stored) {
        String provider = drive.provider;
        var options = new HashMap<String, String>();
        CacheOpenSpec cacheSpec = null;
        int ttlSeconds = 0;
        switch (stored.getConfigCase()) {
            case S3 -> {
                if (!PROVIDER_S3.equals(provider)) {
                    throw invalid("S3 configuration requires s3 provider");
                }
                var s3 = stored.getS3();
                options.putAll(RepoStorageWiring.s3Options(config));
                options.put(OPT_ENDPOINT, s3.getEndpointOverride());
                options.put(OPT_PATH_STYLE, Boolean.toString(s3.getForcePathStyle()));
                options.put(OPT_REGION, blank(drive.region) ? config.s3Region() : drive.region);
                if (!blank(drive.credentialsRef)) {
                    credentials(options, drive.credentialsRef);
                } else if (!s3.getEndpointOverride().equals(text(config.s3Endpoint()))) {
                    throw invalid("A different S3 endpoint requires an explicit credentials_ref");
                }
                if (s3.hasReadThroughCache()) {
                    cacheSpec = resolveCacheSpec(s3.getReadThroughCache());
                }
            }
            case REDIS -> {
                if (!REDIS_COMPATIBLE_PROVIDERS.contains(provider)) {
                    throw invalid("Redis configuration requires redis or valkey provider");
                }
                var redis = stored.getRedis();
                ttlSeconds = redis.getTtlSeconds();
                if (ttlSeconds < 0) {
                    throw invalid("Redis ttl_seconds must not be negative");
                }
                boolean temporary = ttlSeconds > 0;
                if (!temporary && redis.getKeyPrefix().startsWith(TEMPORARY_PREFIX)) {
                    throw invalid("Redis key_prefix uses the reserved temporary-drive prefix");
                }
                if (temporary && config.managedStorage().retentionQualified() && !bounded) {
                    throw invalid("Retention-qualified managed storage refuses expiring drives; "
                            + "temporary drives are not qualified for retained ingestion");
                }
                if (temporary && bounded) {
                    throw invalid("Bounded create-only storage requires zero TTL; temporary drives are not supported");
                }
                options.put(OPT_URI, redisCredentials(redis.getUri(), drive.credentialsRef));
                options.put(OPT_TTL_SECONDS, Integer.toString(ttlSeconds));
                options.put(OPT_MAX_OBJECT_BYTES, Long.toString(redis.getMaxObjectBytes()));
                options.put(OPT_KEY_PREFIX,
                        temporary ? temporaryKeyPrefix(redis.getKeyPrefix(), ttlSeconds) : redis.getKeyPrefix());
                options.put(OPT_WRITE_POLICY, bounded ? WRITE_POLICY_CREATE_ONLY : WRITE_POLICY_REPLACE);
            }
            default -> throw invalid("A drive must explicitly select its provider configuration");
        }
        // Only tuning options may augment the typed identity and credential selection.
        for (var entry : stored.getOptionsMap().entrySet()) {
            if (!S3_TUNING_OPTIONS.contains(entry.getKey()) || !PROVIDER_S3.equals(provider)) {
                throw invalid("Unsupported per-drive tuning option: " + entry.getKey());
            }
            options.put(entry.getKey(), entry.getValue());
        }
        Map<String, String> rawOptions = Map.copyOf(options);
        String region = PROVIDER_S3.equals(provider) ? rawOptions.getOrDefault(OPT_REGION, "") : "";
        boolean pathStyle = PROVIDER_S3.equals(provider) && Boolean.parseBoolean(rawOptions.get(OPT_PATH_STYLE));
        String resolvedEndpoint = PROVIDER_S3.equals(provider)
                ? s3Endpoint(rawOptions.get(OPT_ENDPOINT), region, pathStyle)
                : endpoint(rawOptions.get(OPT_URI));
        StoreIdentity authIdentity = PROVIDER_S3.equals(provider)
                ? s3StoreIdentity(rawOptions, resolvedEndpoint, region, pathStyle)
                : redisStoreIdentity(rawOptions, "Invalid Redis endpoint");
        BackingIdentity backingIdentity = new BackingIdentity(
                authIdentity,
                cacheSpec == null ? null : cacheSpec.identity());
        return new OpenSpec(
                provider,
                rawOptions,
                cacheSpec,
                backingIdentity,
                resolvedEndpoint,
                rawOptions.getOrDefault(OPT_KEY_PREFIX, ""),
                rawOptions.getOrDefault(OPT_WRITE_POLICY, WRITE_POLICY_REPLACE),
                region,
                pathStyle,
                ttlSeconds);
    }

    private CacheOpenSpec resolveCacheSpec(DriveCacheConfig cache) {
        String cacheProvider = blank(cache.getProvider()) ? PROVIDER_VALKEY : cache.getProvider();
        if (!REDIS_COMPATIBLE_PROVIDERS.contains(cacheProvider)) {
            throw invalid("Drive read_through_cache provider must be valkey or redis");
        }
        if (blank(cache.getUri())) {
            throw invalid("Drive read_through_cache requires a non-blank uri");
        }
        int ttl = cache.getTtlSeconds();
        if (ttl <= 0) {
            throw invalid("Drive read_through_cache ttl_seconds must be positive");
        }
        long maxBytes = cache.getMaxCacheableBytes();
        if (maxBytes <= 0) {
            throw invalid("Drive read_through_cache max_cacheable_bytes must be positive");
        }
        String keyPrefix = cache.getKeyPrefix();
        if (keyPrefix.startsWith(TEMPORARY_PREFIX)) {
            throw invalid("Drive read_through_cache key_prefix uses the reserved temporary-drive prefix");
        }
        String resolvedUri = redisCredentials(cache.getUri(), cache.getCredentialsRef());
        var cacheOptions = Map.of(
                OPT_URI, resolvedUri,
                OPT_TTL_SECONDS, Integer.toString(ttl),
                OPT_MAX_OBJECT_BYTES, Long.toString(maxBytes),
                OPT_KEY_PREFIX, keyPrefix,
                OPT_WRITE_POLICY, WRITE_POLICY_REPLACE);
        StoreIdentity cacheStoreIdentity = redisStoreIdentity(
                cacheOptions, "Invalid drive read_through_cache configuration");
        return new CacheOpenSpec(
                cacheProvider,
                cacheOptions,
                new CacheIdentity(cacheStoreIdentity, ttl, maxBytes));
    }

    /** Compatibility identity emitted by the former one-backend host; never rewrites it. */
    StackRecord legacyIdentity(DriveRecord drive, Backing backing, String prefix) {
        if (backing != defaults && !isInheritedDefault(drive, backing)) {
            return null;
        }
        return StackRecord.newBuilder()
                .setStackId("stack-" + drive.driveId)
                .setProvider(config.driveProvider())
                .setEndpoint(text(config.s3Endpoint()))
                .setNamespace(drive.bucket + "/" + prefix)
                .build();
    }

    private boolean isInheritedDefault(DriveRecord drive, Backing backing) {
        return normalizedProviderConfig(drive.readProviderConfig()) == null
                && blank(drive.credentialsRef)
                && defaults.identity().equals(backing.identity());
    }

    private Backing open(OpenSpec spec) {
        SingleFlightHandles.HandleLease authLease = authoritativeHandles.acquire(
                spec.identity().authoritative(),
                () -> openAuthoritativeHandle(spec.provider(), spec.rawOptions(), spec.ttlSeconds()));
        try {
            OpenedBlobStore handle = authLease.handle();
            BlobStore store = handle.store();
            ObjectReclaimer reclaimer = handle.reclaimer();
            if (spec.cache() != null) {
                SingleFlightHandles.HandleLease cacheLease = cacheHandles.acquire(
                        spec.cache().identity().store(),
                        () -> openCacheHandle(spec.cache().provider(), spec.cache().rawOptions()));
                cacheLease.commit(owned);
                OpenedBlobStore cacheHandle = cacheLease.handle();
                var cachingStore = new CachingBlobStore(
                        handle.store(),
                        cacheHandle.store(),
                        spec.cache().identity().ttlSeconds(),
                        spec.cache().identity().maxCacheableBytes(),
                        s3CacheNamespace(spec.endpoint(), spec.region(), spec.pathStyle()));
                store = cachingStore;
                reclaimer = cachingStore.reclaimer(handle.reclaimer());
            }
            var backing = new Backing(
                    spec.provider(),
                    spec.endpoint(),
                    store,
                    handle::ensureNamespace,
                    reclaimer,
                    spec.keyPrefix(),
                    spec.writePolicy(),
                    spec.region(),
                    spec.pathStyle(),
                    spec.ttlSeconds(),
                    spec.identity());
            authLease.commit(owned);
            return backing;
        } catch (RuntimeException | Error failure) {
            authLease.abort(failure);
            throw failure;
        }
    }

    private OpenedBlobStore openAuthoritativeHandle(
            String provider, Map<String, String> options, int ttlSeconds) {
        var handle = providers.open(provider, options);
        try {
            // A positive per-drive TTL selects a temporary drive: its writes expire, so
            // OBJECT_EXPIRY replaces NON_EXPIRING_WRITES in the qualification gate. The
            // host default and zero-TTL drives keep the durable retention gate.
            var required = hostCapabilities(ttlSeconds, config.managedStorage().retentionQualified() && !bounded);
            if (!handle.capabilities().containsAll(required)) {
                throw invalid("Drive provider lacks required repository retention capabilities");
            }
            return handle;
        } catch (RuntimeException | Error failure) {
            try {
                handle.close();
            } catch (Exception cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    private static Set<BlobCapability> hostCapabilities(int ttlSeconds, boolean managedStreaming) {
        if (ttlSeconds > 0) {
            return Set.of(BlobCapability.OBJECT_EXPIRY, BlobCapability.PHYSICAL_RECLAMATION);
        }
        if (managedStreaming) {
            return Set.of(
                    BlobCapability.NON_EXPIRING_WRITES,
                    BlobCapability.PHYSICAL_RECLAMATION,
                    BlobCapability.STREAMING_WRITE);
        }
        return Set.of(BlobCapability.NON_EXPIRING_WRITES, BlobCapability.PHYSICAL_RECLAMATION);
    }

    private OpenedBlobStore openCacheHandle(String provider, Map<String, String> options) {
        final OpenedBlobStore handle;
        try {
            handle = providers.open(provider, options);
        } catch (IllegalArgumentException invalidConfig) {
            throw invalid("Invalid drive read_through_cache configuration: " + invalidConfig.getMessage());
        }
        try {
            if (!handle.capabilities().containsAll(
                    Set.of(BlobCapability.OBJECT_EXPIRY, BlobCapability.PHYSICAL_RECLAMATION))) {
                throw invalid("Drive read_through_cache provider lacks required expiry and reclamation capabilities");
            }
            return handle;
        } catch (RuntimeException | Error failure) {
            try {
                handle.close();
            } catch (Exception cleanup) {
                failure.addSuppressed(cleanup);
            }
            throw failure;
        }
    }

    private void checkNamespaceCompatible(PhysicalNamespace namespace, BackingIdentity identity) {
        if (namespace == null || identity == null) {
            return;
        }
        BackingIdentity established = namespaceIdentities.get(namespace);
        if (established != null && !established.equals(identity)) {
            throw invalid("Drive configuration conflicts with existing store for physical namespace");
        }
    }

    private void bindNamespace(PhysicalNamespace namespace, BackingIdentity identity) {
        if (namespace == null || identity == null
                || namespace.namespace().length() > MAX_STORABLE_NAMESPACE_CHARS) {
            return;
        }
        BackingIdentity established = namespaceIdentities.putIfAbsent(namespace, identity);
        if (established != null && !established.equals(identity)) {
            throw invalid("Drive configuration conflicts with existing store for physical namespace");
        }
    }

    private static PhysicalNamespace physicalNamespace(
            DriveRecord drive,
            String provider,
            String endpoint,
            String keyPrefix,
            String writePolicy,
            String region,
            boolean pathStyle,
            int ttlSeconds) {
        if (blank(drive.bucket)) {
            return null;
        }
        String prefix = RepositoryNamespaces.under(
                drive.prefix == null ? "" : RepoV2Wiring.stripLeadingSlashes(drive.prefix),
                DOCUMENTS_SUBPATH);
        String canonicalProvider = PROVIDER_S3.equals(provider) ? PROVIDER_S3 : PROVIDER_REDIS;
        String encodedNamespace = formatNamespace(
                provider, region, pathStyle, ttlSeconds, writePolicy, keyPrefix, drive.bucket, prefix);
        return new PhysicalNamespace(canonicalProvider, endpoint, encodedNamespace);
    }

    private static String formatNamespace(
            String provider,
            String region,
            boolean pathStyle,
            int ttlSeconds,
            String writePolicy,
            String keyPrefix,
            String bucket,
            String prefix) {
        if (PROVIDER_S3.equals(provider)) {
            return region.isEmpty()
                    ? bucket + "/" + prefix
                    : "s3:" + encode(region) + ":" + (pathStyle ? "path:" : "virtual:") + bucket + "/" + prefix;
        }
        String retention = ttlSeconds > 0 ? TEMPORARY_NAMESPACE + ttlSeconds + ":" : "redis:";
        return retention + writePolicy + ":" + encode(keyPrefix) + ":" + encode(bucket) + ":" + encode(prefix);
    }

    private static StoreIdentity s3StoreIdentity(Map<String, String> options) {
        String region = options.getOrDefault(OPT_REGION, "");
        boolean pathStyle = Boolean.parseBoolean(options.get(OPT_PATH_STYLE));
        String canonicalEndpoint = s3Endpoint(options.get(OPT_ENDPOINT), region, pathStyle);
        return s3StoreIdentity(options, canonicalEndpoint, region, pathStyle);
    }

    private static StoreIdentity s3StoreIdentity(
            Map<String, String> options, String canonicalEndpoint, String region, boolean pathStyle) {
        var normalized = new HashMap<>(options);
        normalized.put(OPT_ENDPOINT, canonicalEndpoint);
        normalized.put(OPT_REGION, region);
        normalized.put(OPT_PATH_STYLE, Boolean.toString(pathStyle));
        normalized.put(
                OPT_CONDITIONAL_WRITES,
                Boolean.toString(Boolean.parseBoolean(options.get(OPT_CONDITIONAL_WRITES))));
        return new StoreIdentity(PROVIDER_S3, normalized);
    }

    private static StoreIdentity redisStoreIdentity(Map<String, String> options, String errorMessage) {
        var normalized = new HashMap<>(options);
        normalized.put(OPT_URI, canonicalRedisUri(options.get(OPT_URI), errorMessage));
        return new StoreIdentity(PROVIDER_REDIS, normalized);
    }

    private static String canonicalRedisUri(String value, String errorMessage) {
        final URI parsed;
        final Map<String, String> location;
        try {
            parsed = URI.create(value);
            location = RedisBackendIdentity.of(value, "").location();
        } catch (RuntimeException invalidUri) {
            throw invalid(errorMessage);
        }
        try {
            return new URI(
                    location.get("scheme"),
                    parsed.getUserInfo(),
                    location.get("host"),
                    Integer.parseInt(location.get("port")),
                    "/" + location.get("database"),
                    null,
                    null).toString();
        } catch (URISyntaxException invalidUri) {
            throw invalid(errorMessage);
        }
    }

    private static DriveProviderConfig normalizedProviderConfig(DriveProviderConfig stored) {
        if (stored != null
                && stored.getConfigCase() == DriveProviderConfig.ConfigCase.CONFIG_NOT_SET
                && stored.getOptionsMap().isEmpty()
                && stored.getUnknownFields().asMap().isEmpty()) {
            return null;
        }
        return stored;
    }

    private void credentials(Map<String, String> options, String reference) {
        // The host profile is opt-in when sending credentials to another endpoint.
        if (HOST_DEFAULT_CREDENTIALS.equals(reference)) {
            return;
        }
        options.remove(OPT_ACCESS_KEY);
        options.remove(OPT_SECRET_KEY);
        if (reference.startsWith(ENV_CREDENTIALS_PREFIX) && reference.length() > ENV_CREDENTIALS_PREFIX.length()) {
            String prefix = reference.substring(ENV_CREDENTIALS_PREFIX.length());
            String access = environment.apply(prefix + "_ACCESS_KEY_ID");
            String secret = environment.apply(prefix + "_SECRET_ACCESS_KEY");
            if (blank(access) || blank(secret)) {
                throw invalid("Drive credential environment profile is incomplete");
            }
            options.put(OPT_CREDENTIALS_MODE, "static");
            options.put(OPT_ACCESS_KEY, access);
            options.put(OPT_SECRET_KEY, secret);
        } else if (reference.startsWith(PROFILE_CREDENTIALS_PREFIX)
                && reference.length() > PROFILE_CREDENTIALS_PREFIX.length()) {
            options.put(OPT_CREDENTIALS_MODE, "profile");
            options.put(OPT_PROFILE_NAME, reference.substring(PROFILE_CREDENTIALS_PREFIX.length()));
        } else {
            throw invalid("Drive credentials_ref must use env:, profile: or host:default");
        }
    }

    private String redisCredentials(String endpoint, String reference) {
        final URI uri;
        try {
            uri = URI.create(endpoint);
        } catch (IllegalArgumentException invalidUri) {
            throw invalid("Invalid Redis endpoint");
        }
        if (uri.getUserInfo() != null) {
            throw invalid("Per-drive Redis endpoints must not contain credentials; use credentials_ref");
        }
        if (blank(reference)) {
            return endpoint;
        }
        if (!reference.startsWith(ENV_CREDENTIALS_PREFIX) || reference.length() == ENV_CREDENTIALS_PREFIX.length()) {
            throw invalid("Redis credentials_ref must use env:");
        }
        String prefix = reference.substring(ENV_CREDENTIALS_PREFIX.length());
        String password = environment.apply(prefix + "_PASSWORD");
        String username = environment.apply(prefix + "_USERNAME");
        if (blank(password)) {
            throw invalid("Drive credential environment profile is incomplete");
        }
        try {
            return new URI(
                    uri.getScheme(),
                    text(username) + ":" + password,
                    uri.getHost(),
                    uri.getPort(),
                    uri.getPath(),
                    uri.getQuery(),
                    uri.getFragment()).toString();
        } catch (URISyntaxException invalidProfile) {
            throw invalid("Invalid Redis credential profile");
        }
    }

    static String s3Endpoint(String value, String region, boolean pathStyle) {
        if (blank(value)) {
            return "";
        }
        final URI uri;
        try {
            uri = URI.create(value);
        } catch (IllegalArgumentException invalidUri) {
            throw invalid("Invalid S3 endpoint");
        }
        // S3BackendIdentity deliberately covers origins; the unmanaged provider
        // also supports explicit path prefixes, whose exact spelling stays identity.
        if (uri.getRawPath() != null && !uri.getRawPath().isEmpty() && !uri.getRawPath().equals("/")) {
            return value;
        }
        try {
            return S3BackendIdentity.of(value, region, pathStyle).location().get(OPT_ENDPOINT);
        } catch (IllegalArgumentException invalidIdentity) {
            throw invalid("Invalid S3 drive configuration: " + invalidIdentity.getMessage());
        }
    }

    /** Canonical physical backing identity isolating cache entries across S3 backends. */
    static String s3CacheNamespace(String endpoint, String region, boolean pathStyle) {
        return "s3:" + encode(text(endpoint)) + ":" + encode(text(region)) + ":" + (pathStyle ? "path" : "virtual");
    }

    private static String endpoint(String value) {
        final Map<String, String> location;
        try {
            location = RedisBackendIdentity.of(value, "").location();
        } catch (IllegalArgumentException invalidUri) {
            throw invalid("Invalid drive endpoint");
        }
        try {
            return new URI(
                    location.get("scheme"),
                    null,
                    location.get("host"),
                    Integer.parseInt(location.get("port")),
                    "/" + location.get("database"),
                    null,
                    null).toString();
        } catch (URISyntaxException invalidUri) {
            // The identity has already removed credentials, so this cause is safe.
            throw new RepositoryException(
                    RepositoryException.Code.FAILED_PRECONDITION, "Invalid drive endpoint", invalidUri);
        }
    }

    void requireStorable(DriveRecord drive, Backing backing) {
        String prefix = RepositoryNamespaces.under(
                drive.prefix == null ? "" : RepoV2Wiring.stripLeadingSlashes(drive.prefix),
                DOCUMENTS_SUBPATH);
        CatalogEntries.requireStorableStack(
                StackRecord.newBuilder()
                        .setStackId("stack-" + drive.driveId)
                        .setProvider(backing.provider())
                        .setEndpoint(backing.endpoint())
                        .setNamespace(backing.namespace(drive.bucket, prefix))
                        .build());
    }

    /** Encodes temporary identity under a prefix rejected for durable drive configurations. */
    static String temporaryKeyPrefix(String configuredPrefix, int ttlSeconds) {
        if (ttlSeconds <= 0) {
            throw new IllegalArgumentException("Temporary drives need a positive TTL");
        }
        return TEMPORARY_PREFIX + ttlSeconds + ":" + encode(configuredPrefix);
    }

    static boolean temporaryNamespace(String namespace) {
        return namespace.startsWith(TEMPORARY_NAMESPACE);
    }

    private static String encode(String value) {
        return Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String text(String value) {
        return value == null ? "" : value;
    }

    private static boolean blank(String value) {
        return value == null || value.isBlank();
    }

    private static RepositoryException invalid(String message) {
        return new RepositoryException(RepositoryException.Code.FAILED_PRECONDITION, message);
    }

    @Override
    public void close() {
        lifecycleLock.writeLock().lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            backings.clear();
            authoritativeHandles.clear();
            cacheHandles.clear();
            namespaceIdentities.clear();
            owned.close();
        } finally {
            lifecycleLock.writeLock().unlock();
        }
    }

    /**
     * Per-identity single-flight pool for {@link OpenedBlobStore} handles.
     *
     * <p>Provider initialization executes outside map bucket locks and outside per-slot
     * monitors so slow opens on one backend never block unrelated resolves or pin virtual
     * threads, while concurrent opens on the same canonical {@link StoreIdentity} share
     * one initialization flight.
     */
    private static final class SingleFlightHandles {
        private final ConcurrentHashMap<StoreIdentity, HandleSlot> slots = new ConcurrentHashMap<>();

        void preseed(StoreIdentity identity, OpenedBlobStore handle) {
            slots.put(identity, HandleSlot.preseeded(handle));
        }

        HandleLease acquire(StoreIdentity identity, Supplier<OpenedBlobStore> opener) {
            while (true) {
                HandleSlot existing = slots.get(identity);
                if (existing != null) {
                    HandleLease joined = existing.tryJoin(slots, identity);
                    if (joined != null) {
                        return joined;
                    }
                    continue;
                }
                HandleSlot created = new HandleSlot();
                HandleSlot raced = slots.putIfAbsent(identity, created);
                if (raced != null) {
                    HandleLease joined = raced.tryJoin(slots, identity);
                    if (joined != null) {
                        return joined;
                    }
                    continue;
                }
                try {
                    OpenedBlobStore opened = opener.get();
                    created.complete(opened);
                    return new HandleLease(slots, identity, created, opened, true);
                } catch (RuntimeException | Error failure) {
                    created.markAborted();
                    slots.remove(identity, created);
                    created.fail(failure);
                    throw failure;
                }
            }
        }

        void clear() {
            slots.clear();
        }

        private static final class HandleSlot {
            private final CompletableFuture<OpenedBlobStore> future;
            private OpenedBlobStore resolved;
            private int uncommittedAcquirers;
            private boolean committed;
            private boolean aborted;

            HandleSlot() {
                this.future = new CompletableFuture<>();
                this.uncommittedAcquirers = 1;
            }

            private HandleSlot(OpenedBlobStore handle) {
                this.future = CompletableFuture.completedFuture(handle);
                this.resolved = handle;
                this.committed = true;
            }

            static HandleSlot preseeded(OpenedBlobStore handle) {
                return new HandleSlot(handle);
            }

            HandleLease tryJoin(ConcurrentHashMap<StoreIdentity, HandleSlot> map, StoreIdentity identity) {
                boolean counted;
                synchronized (this) {
                    if (aborted) {
                        return null;
                    }
                    if (committed) {
                        return new HandleLease(map, identity, this, resolved, false);
                    }
                    uncommittedAcquirers++;
                    counted = true;
                }
                OpenedBlobStore opened = await();
                return new HandleLease(map, identity, this, opened, counted);
            }

            void complete(OpenedBlobStore handle) {
                synchronized (this) {
                    this.resolved = handle;
                }
                future.complete(handle);
            }

            void fail(Throwable failure) {
                future.completeExceptionally(failure);
            }

            synchronized void markAborted() {
                this.aborted = true;
            }

            void commit(OwnedResources owned, OpenedBlobStore handle) {
                boolean register = false;
                synchronized (this) {
                    if (!committed) {
                        committed = true;
                        uncommittedAcquirers = 0;
                        register = true;
                    }
                }
                if (register) {
                    owned.add(handle);
                }
            }

            void abort(
                    ConcurrentHashMap<StoreIdentity, HandleSlot> map,
                    StoreIdentity identity,
                    OpenedBlobStore handle,
                    Throwable cause) {
                boolean closeNow = false;
                synchronized (this) {
                    if (committed) {
                        return;
                    }
                    uncommittedAcquirers--;
                    if (uncommittedAcquirers <= 0) {
                        aborted = true;
                        map.remove(identity, this);
                        closeNow = true;
                    }
                }
                if (closeNow) {
                    try {
                        handle.close();
                    } catch (Exception cleanup) {
                        cause.addSuppressed(cleanup);
                    }
                }
            }

            private OpenedBlobStore await() {
                try {
                    return future.join();
                } catch (CompletionException wrapped) {
                    Throwable cause = wrapped.getCause();
                    if (cause instanceof RuntimeException runtime) {
                        throw runtime;
                    }
                    if (cause instanceof Error error) {
                        throw error;
                    }
                    throw wrapped;
                }
            }
        }

        static final class HandleLease {
            private final ConcurrentHashMap<StoreIdentity, HandleSlot> map;
            private final StoreIdentity identity;
            private final HandleSlot slot;
            private final OpenedBlobStore handle;
            private boolean pendingDecision;

            HandleLease(
                    ConcurrentHashMap<StoreIdentity, HandleSlot> map,
                    StoreIdentity identity,
                    HandleSlot slot,
                    OpenedBlobStore handle,
                    boolean pendingDecision) {
                this.map = map;
                this.identity = identity;
                this.slot = slot;
                this.handle = handle;
                this.pendingDecision = pendingDecision;
            }

            OpenedBlobStore handle() {
                return handle;
            }

            void commit(OwnedResources owned) {
                pendingDecision = false;
                slot.commit(owned, handle);
            }

            void abort(Throwable cause) {
                if (!pendingDecision) {
                    return;
                }
                pendingDecision = false;
                slot.abort(map, identity, handle, cause);
            }
        }
    }
}
