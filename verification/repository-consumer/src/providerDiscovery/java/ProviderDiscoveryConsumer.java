import ai.protomolt.proto.repo.blob.spi.BackendIdentity;
import ai.protomolt.proto.repo.blob.spi.BlobCapability;
import ai.protomolt.proto.repo.blob.spi.BlobStores;
import ai.protomolt.proto.repo.blob.spi.OpenedBlobStore;
import ai.protomolt.proto.repo.blob.s3.S3BlobStore;
import ai.protomolt.proto.repo.blob.s3.S3BlobStoreProvider;
import ai.protomolt.proto.repo.blob.redis.RedisBlobStore;
import ai.protomolt.proto.repo.blob.redis.RedisBlobStoreProvider;
import java.nio.charset.StandardCharsets;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Exercises ServiceLoader provider discovery from the packaged published JARs.
 * Every provider class must load from the staged repository's JAR files, the
 * packaged META-INF/services entries must name exactly the two trusted
 * factories, and selection/identity/capability behavior must hold without any
 * build-tree class on the classpath.
 */
public final class ProviderDiscoveryConsumer {
    private static final String SERVICE = "META-INF/services/ai.protomolt.proto.repo.blob.spi.BlobStoreProvider";

    public static void main(String[] args) throws Exception {
        if (args.length != 1) throw new IllegalArgumentException("usage: ProviderDiscoveryConsumer <staged-repository-dir>");
        String repository = new java.io.File(args[0]).getCanonicalPath();

        var providers = BlobStores.discover();
        require(providers.providerIds().equals(Set.of("s3", "redis")),
                "discovered provider IDs must be exactly {s3, redis}: " + providers.providerIds());

        assertPackagedJar(S3BlobStoreProvider.class, repository, "protomolt-repo-blob-s3-");
        assertPackagedJar(RedisBlobStoreProvider.class, repository, "protomolt-repo-blob-redis-");
        assertPackagedServices();

        BackendIdentity s3Identity = providers.managedIdentity("s3", Map.of(
                "endpoint", "http://127.0.0.1:1", "region", "us-east-1", "path-style", "true"));
        require(s3Identity.provider().equals("s3") && s3Identity.schema().equals("s3/v1"),
                "unexpected S3 backend identity: " + s3Identity);
        require("http://127.0.0.1:1".equals(s3Identity.location().get("endpoint")),
                "S3 identity must retain the explicit endpoint: " + s3Identity.location());
        requireSecretFree(s3Identity);

        BackendIdentity redisIdentity = providers.managedIdentity("redis", Map.of(
                "uri", "redis://127.0.0.1:1", "ttl-seconds", "0", "max-object-bytes", "0", "key-prefix", ""));
        require(redisIdentity.provider().equals("redis") && redisIdentity.schema().equals("redis/v2"),
                "unexpected Redis backend identity: " + redisIdentity);
        requireSecretFree(redisIdentity);

        expectIllegalArgument(() -> providers.managedIdentity("absent", Map.of()), "not installed");
        expectIllegalArgument(() -> providers.managedIdentity("s3", Map.of()), null);

        Map<String, String> s3Options = Map.of(
                "endpoint", "http://127.0.0.1:1", "region", "us-east-1",
                "access-key", "consumer", "secret-key", "consumer-secret",
                "path-style", "true", "conditional-writes", "true");
        try (OpenedBlobStore opened = providers.open("s3", s3Options)) {
            require(opened.store() instanceof S3BlobStore, "S3 handle must expose the S3 adapter");
            require(opened.capabilities().equals(EnumSet.of(
                    BlobCapability.BOUNDED_READ, BlobCapability.LIST, BlobCapability.SERVER_SIDE_COPY,
                    BlobCapability.STREAMING_WRITE, BlobCapability.NON_EXPIRING_WRITES,
                    BlobCapability.PHYSICAL_RECLAMATION, BlobCapability.AUTHORITATIVE_CONDITIONAL_READ,
                    BlobCapability.ATOMIC_CONDITIONAL_WRITE)),
                    "unexpected S3 capabilities: " + opened.capabilities());
        }
        Map<String, String> s3Unconditional = new java.util.HashMap<>(s3Options);
        s3Unconditional.put("conditional-writes", "false");
        try (OpenedBlobStore opened = providers.open("s3", s3Unconditional)) {
            require(!opened.capabilities().contains(BlobCapability.ATOMIC_CONDITIONAL_WRITE),
                    "unqualified endpoint must not claim conditional writes");
        }
        expectUnsupported(() -> providers.open("s3", s3Unconditional,
                Set.of(BlobCapability.ATOMIC_CONDITIONAL_WRITE)).close());
        expectUnsupported(() -> providers.open("s3", s3Options,
                Set.of(BlobCapability.OBJECT_EXPIRY)).close());

        Map<String, String> redisOptions = Map.of(
                "uri", "redis://127.0.0.1:1", "ttl-seconds", "0", "max-object-bytes", "0", "key-prefix", "");
        var redis = providers.open("redis", redisOptions);
        require(redis.store() instanceof RedisBlobStore, "Redis handle must expose the Redis adapter");
        require(redis.capabilities().equals(EnumSet.of(
                BlobCapability.LIST, BlobCapability.OBJECT_EXPIRY, BlobCapability.BOUNDED_READ,
                BlobCapability.AUTHORITATIVE_CONDITIONAL_READ, BlobCapability.PHYSICAL_RECLAMATION,
                BlobCapability.ATOMIC_CONDITIONAL_WRITE, BlobCapability.NON_EXPIRING_WRITES)),
                "unexpected Redis capabilities: " + redis.capabilities());
        redis.close();
        redis.close();
        try {
            redis.store();
            throw new AssertionError("closed handle must refuse store access");
        } catch (IllegalStateException expected) {
            // closed handles fail explicitly
        }
        expectUnsupported(() -> providers.open("redis", redisOptions,
                Set.of(BlobCapability.STREAMING_WRITE)).close());
        expectIllegalArgument(() -> providers.open("absent", Map.of()), "not installed");

        System.out.println("PROVIDER-DISCOVERY-CONSUMER OK: packaged discovery, identity, capabilities, selection");
    }

    private static void assertPackagedJar(Class<?> type, String repository, String artifactPrefix) throws Exception {
        String location = type.getProtectionDomain().getCodeSource().getLocation().getPath();
        require(location.endsWith(".jar"), type.getName() + " must load from a packaged jar: " + location);
        require(location.contains("/" + artifactPrefix), type.getName() + " loaded from unexpected artifact: " + location);
        require(new java.io.File(location).getCanonicalPath().startsWith(repository + "/"),
                type.getName() + " must load from the staged repository " + repository + ": " + location);
    }

    private static void assertPackagedServices() throws Exception {
        var named = new TreeSet<String>();
        var resources = Thread.currentThread().getContextClassLoader().getResources(SERVICE);
        while (resources.hasMoreElements()) {
            try (var input = resources.nextElement().openStream()) {
                for (String line : new String(input.readAllBytes(), StandardCharsets.UTF_8).split("\\R")) {
                    String trimmed = line.trim();
                    if (!trimmed.isEmpty() && !trimmed.startsWith("#")) named.add(trimmed);
                }
            }
        }
        require(named.equals(Set.of(
                "ai.protomolt.proto.repo.blob.s3.S3BlobStoreProvider",
                "ai.protomolt.proto.repo.blob.redis.RedisBlobStoreProvider")),
                "packaged service registrations must name exactly the two trusted factories: " + named);
    }

    private static void requireSecretFree(BackendIdentity identity) {
        for (var field : identity.location().entrySet()) {
            require(!field.getKey().contains("secret") && !field.getKey().contains("access")
                    && !field.getKey().contains("password") && !field.getValue().contains("consumer-secret"),
                    "backend identity must be nonsecret: " + identity.location());
        }
    }

    private static void expectUnsupported(ThrowingRunnable call) throws Exception {
        try {
            call.run();
            throw new AssertionError("unsupported capability requirement must be rejected");
        } catch (UnsupportedOperationException expected) {
            // explicit refusal, never a fallback
        }
    }

    private static void expectIllegalArgument(ThrowingRunnable call, String messagePart) throws Exception {
        try {
            call.run();
            throw new AssertionError("invalid selection must be rejected");
        } catch (IllegalArgumentException expected) {
            if (messagePart != null) require(expected.getMessage() != null && expected.getMessage().contains(messagePart),
                    "unexpected refusal message: " + expected.getMessage());
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }

    @FunctionalInterface private interface ThrowingRunnable { void run() throws Exception; }
}
