import ai.protomolt.proto.repo.blob.spi.BlobCapability;
import ai.protomolt.proto.repo.blob.spi.BlobStore;
import ai.protomolt.proto.repo.blob.spi.BlobStores;
import ai.protomolt.proto.repo.blob.spi.OpenedBlobStore;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Non-S3 published consumer: resolves only the byte SPI and the Redis provider
 * from the staged repository, proves the AWS and Azure SDKs are absent from the
 * published classpath, then starts a real Redis container (via the docker CLI,
 * an orchestration tool rather than a classpath dependency) and executes real
 * conditional operations through ServiceLoader discovery and explicit selection.
 */
public final class RedisPublishedConsumer {
    public static void main(String[] args) throws Exception {
        requireAbsent("software.amazon.awssdk.services.s3.S3Client");
        requireAbsent("software.amazon.awssdk.auth.credentials.AwsBasicCredentials");
        requireAbsent("com.azure.storage.blob.BlobServiceClient");
        Class.forName("redis.clients.jedis.Jedis");

        var providers = BlobStores.discover();
        require(providers.providerIds().equals(Set.of("redis")),
                "the Redis-only classpath must discover exactly redis: " + providers.providerIds());
        try {
            providers.open("s3", Map.of());
            throw new AssertionError("an uninstalled provider must not open");
        } catch (IllegalArgumentException expected) {
            require(expected.getMessage().contains("not installed"), "unexpected refusal: " + expected.getMessage());
        }

        String container = docker("run", "--rm", "-d", "-p", "127.0.0.1::6379", "redis:7-alpine").trim();
        try {
            String published = docker("port", container, "6379").trim();
            require(published.startsWith("127.0.0.1:"), "unexpected redis port mapping: " + published);
            String uri = "redis://" + published;
            Map<String, String> options = Map.of(
                    "uri", uri, "ttl-seconds", "0", "max-object-bytes", "0",
                    "key-prefix", "published-consumer-" + UUID.randomUUID(), "write-policy", "replace");
            // Readiness is checked outside the code under test, so no provider
            // failure is ever caught and retried.
            awaitRedisReady(container);
            OpenedBlobStore opened = providers.open("redis", options, Set.of(
                    BlobCapability.ATOMIC_CONDITIONAL_WRITE, BlobCapability.AUTHORITATIVE_CONDITIONAL_READ,
                    BlobCapability.BOUNDED_READ, BlobCapability.LIST, BlobCapability.PHYSICAL_RECLAMATION,
                    BlobCapability.NON_EXPIRING_WRITES));
            try {
                var store = opened.store();
                var spec = new BlobStore.PutSpec("namespace", "key", "application/octet-stream", Map.of(), null);

                var created = store.conditionalPut(spec, new byte[]{1, 2, 3}, BlobStore.WriteCondition.absent());
                var authoritative = store.getForUpdate("namespace", "key");
                require(java.util.Arrays.equals(authoritative.data(), new byte[]{1, 2, 3}), "authoritative read bytes differ");
                require(authoritative.eTag().equals(created.eTag()), "authoritative ETag must match the created object");

                var replaced = store.conditionalPut(spec, new byte[]{4, 5}, BlobStore.WriteCondition.matching(authoritative.eTag()));
                require(!replaced.eTag().equals(created.eTag()), "replacement must carry a new ETag");
                try {
                    store.conditionalPut(spec, new byte[]{9}, BlobStore.WriteCondition.matching(authoritative.eTag()));
                    throw new AssertionError("a stale precondition must conflict");
                } catch (BlobStore.BlobConflictException expected) {
                    // the losing write never becomes successful
                }
                require(java.util.Arrays.equals(store.getForUpdate("namespace", "key").data(), new byte[]{4, 5}),
                        "conflicting write must not mutate stored bytes");
                try {
                    store.getBounded("namespace", "key", null, 1);
                    throw new AssertionError("bounded read must refuse an oversized object");
                } catch (BlobStore.BlobReadLimitException expected) {
                    // bounded reads never return a truncated prefix
                }

                require(store.delete("namespace", "key"), "delete must report the removed object");
                try {
                    store.get("namespace", "key");
                    throw new AssertionError("deleted object must not be readable");
                } catch (BlobStore.BlobNotFoundException expected) {
                    // deletion is authoritative
                }
            } finally {
                opened.close();
                opened.close();
            }
            try {
                opened.store();
                throw new AssertionError("closed handle must refuse store access");
            } catch (IllegalStateException expected) {
                // owned pool released exactly once
            }
        } finally {
            docker("rm", "-f", container);
        }
        System.out.println("REDIS-PUBLISHED-CONSUMER OK: real conditional operations with AWS/Azure SDK absent");
    }

    private static void awaitRedisReady(String container) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        String last = "";
        while (System.nanoTime() < deadline) {
            var process = new ProcessBuilder("docker", "exec", container, "redis-cli", "ping")
                    .redirectErrorStream(true).start();
            last = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
            if (!process.waitFor(10, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                throw new AssertionError("redis-cli ping did not exit within 10 seconds");
            }
            if (process.exitValue() == 0 && last.equals("PONG")) return;
            Thread.sleep(250);
        }
        throw new AssertionError("redis container did not answer PONG within 30 seconds; last reply: " + last);
    }

    private static void requireAbsent(String className) {
        try {
            Class.forName(className);
            throw new AssertionError(className + " must be absent from the Redis-only published classpath");
        } catch (ClassNotFoundException expected) {
            // provider isolation: no unrelated SDK is inherited
        }
    }

    private static String docker(String... arguments) throws Exception {
        var process = new ProcessBuilder(concat("docker", arguments)).redirectErrorStream(true).start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        if (!process.waitFor(60, TimeUnit.SECONDS) || process.exitValue() != 0) {
            throw new AssertionError("docker " + String.join(" ", arguments) + " failed: " + output);
        }
        return output;
    }

    private static String[] concat(String first, String[] rest) {
        var all = new String[rest.length + 1];
        all[0] = first;
        System.arraycopy(rest, 0, all, 1, rest.length);
        return all;
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }
}
