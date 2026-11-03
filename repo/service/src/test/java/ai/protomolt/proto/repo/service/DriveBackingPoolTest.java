package ai.protomolt.proto.repo.service;

import ai.protomolt.proto.repo.blob.spi.*;
import ai.protomolt.proto.repo.container.ledger.DriveRecord;
import ai.protomolt.proto.repo.v1.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class DriveBackingPoolTest {
    private static final BlobStore STORE = (BlobStore) java.lang.reflect.Proxy.newProxyInstance(
            BlobStore.class.getClassLoader(), new Class<?>[]{BlobStore.class},
            (proxy, method, args) -> { throw new AssertionError("No byte IO expected"); });
    private static final RepoServiceConfig CONFIG = RepoServiceConfig.fromEnvironment(Map.of());
    private static final RepoStorageWiring.Wired DEFAULTS = new RepoStorageWiring.Wired(
            STORE, name -> {}, null, (bucket, key) -> true);

    @Test void sharesCompleteConfigurationAndClosesOnceInReverseOrder() {
        var closed = new ArrayList<String>();
        var opened = new AtomicInteger();
        var providers = BlobStores.of(List.of(provider(opened, closed, Set.of(
                BlobCapability.NON_EXPIRING_WRITES, BlobCapability.PHYSICAL_RECLAMATION))));
        var pool = new DriveBackingPool(CONFIG, providers, DEFAULTS);
        var first = pool.resolve(s3("http://first:9000"));
        assertThat(pool.resolve(s3("http://first:9000"))).isSameAs(first);
        assertThat(pool.resolve(s3("http://second:9000"))).isNotSameAs(first);
        assertThat(opened).hasValue(2);
        pool.close();
        pool.close();
        assertThat(closed).containsExactly("http://second:9000", "http://first:9000");
        assertThatThrownBy(() -> pool.resolve(s3("http://first:9000")))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("closed");
    }

    @Test void omittedConfigurationBorrowsHostLifetimeWithoutOpeningAnotherProvider() {
        var record = new DriveRecord(); record.provider = "s3";
        try (var pool = new DriveBackingPool(CONFIG, BlobStores.of(List.of()), DEFAULTS)) {
            assertThat(pool.resolve(record).store()).isSameAs(STORE);
            assertThat(pool.resolve(record).provider()).isEqualTo("s3");
            assertThat(pool.resolve(record).endpoint()).isEmpty();
        }
    }

    @Test void failedCapabilityQualificationClosesAndDoesNotPoisonPool() {
        var opened = new AtomicInteger(); var closed = new ArrayList<String>();
        var providers = BlobStores.of(List.of(provider(opened, closed, Set.of())));
        try (var pool = new DriveBackingPool(CONFIG, providers, DEFAULTS)) {
            for (int attempt = 0; attempt < 2; attempt++)
                assertThatThrownBy(() -> pool.resolve(s3("http://first:9000")))
                        .hasMessageContaining("retention capabilities");
        }
        assertThat(opened).hasValue(2);
        assertThat(closed).containsExactly("http://first:9000", "http://first:9000");
    }

    @Test void resolvesCredentialsExplicitlyAndNeverFallsBackWhenMissing() {
        var seen = new ArrayList<Map<String, String>>();
        var provider = new BlobStoreProvider() {
            public String id() { return "s3"; }
            public OpenedBlobStore open(Map<String, String> options) {
                seen.add(options);
                return handle(() -> {});
            }
        };
        var env = Map.of("OTHER_ACCESS_KEY_ID", "access", "OTHER_SECRET_ACCESS_KEY", "secret");
        try (var pool = new DriveBackingPool(CONFIG, BlobStores.of(List.of(provider)), DEFAULTS, env::get)) {
            var record = s3("http://other:9000"); record.credentialsRef = "env:OTHER";
            pool.resolve(record);
            assertThat(seen.getFirst()).containsEntry("access-key", "access").containsEntry("secret-key", "secret");
            record.credentialsRef = "env:MISSING";
            assertThatThrownBy(() -> pool.resolve(record)).hasMessageContaining("incomplete");
            record.credentialsRef = "unknown:OTHER";
            assertThatThrownBy(() -> pool.resolve(record)).hasMessageContaining("env:, profile: or host:default");
            assertThat(seen).hasSize(1);
            record.credentialsRef = "profile:archive";
            pool.resolve(record);
            assertThat(seen.getLast()).containsEntry("credentials-mode", "profile")
                    .containsEntry("profile-name", "archive").doesNotContainKeys("access-key", "secret-key");
        }
    }

    @Test void aNewEndpointNeverImplicitlyReceivesHostCredentials() {
        try (var pool = new DriveBackingPool(CONFIG, BlobStores.of(List.of()), DEFAULTS)) {
            var record = s3("http://another:9000"); record.credentialsRef = null;
            assertThatThrownBy(() -> pool.resolve(record)).hasMessageContaining("explicit credentials_ref");
        }
    }

    @Test void managedStreamingKeepsItsCapabilityGateAndClosesRejectedProvider() {
        var opened = new AtomicInteger(); var closed = new ArrayList<String>();
        var config = CONFIG.withManagedStorage(new ManagedStoragePolicy("generation", "realm", true));
        var providers = BlobStores.of(List.of(provider(opened, closed,
                Set.of(BlobCapability.NON_EXPIRING_WRITES, BlobCapability.PHYSICAL_RECLAMATION))));
        try (var pool = new DriveBackingPool(config, providers, DEFAULTS)) {
            assertThatThrownBy(() -> pool.resolve(s3("http://other:9000"))).hasMessageContaining("retention capabilities");
        }
        assertThat(closed).containsExactly("http://other:9000");
    }

    @Test void secretsCannotBeStoredInRedisProviderConfiguration() {
        try (var pool = new DriveBackingPool(CONFIG, BlobStores.of(List.of()), DEFAULTS)) {
            var drive = new DriveRecord(); drive.provider = "valkey";
            drive.writeProviderConfig(DriveProviderConfig.newBuilder().setRedis(RedisDriveConfig.newBuilder()
                    .setUri("redis://user:password-not-public@host:6379")).build());
            assertThatThrownBy(() -> pool.resolve(drive)).hasMessageContaining("must not contain credentials")
                    .hasMessageNotContaining("password-not-public");
        }
    }

    @Test void refusesExpiryAndIdentityOverridesBeforeProviderOpen() {
        try (var pool = new DriveBackingPool(CONFIG, BlobStores.of(List.of()), DEFAULTS)) {
            var redis = new DriveRecord(); redis.provider = "valkey";
            redis.writeProviderConfig(DriveProviderConfig.newBuilder().setRedis(RedisDriveConfig.newBuilder()
                    .setUri("redis://host:6379").setTtlSeconds(5)).build());
            assertThatThrownBy(() -> pool.resolve(redis)).hasMessageContaining("until explicit purge");
            var s3 = s3("http://host:9000");
            s3.writeProviderConfig(s3.readProviderConfig().toBuilder().putOptions("endpoint", "http://other:9000").build());
            assertThatThrownBy(() -> pool.resolve(s3)).hasMessageContaining("Unsupported per-drive tuning option");
        }
    }

    @Test void redisNamespacesIncludePrefixWithoutCollisionsAndEndpointsExcludeCredentials() {
        var provider = new BlobStoreProvider() {
            public String id() { return "valkey"; }
            public OpenedBlobStore open(Map<String, String> options) { return handle(() -> {}); }
        };
        try (var pool = new DriveBackingPool(CONFIG, BlobStores.of(List.of(provider)), DEFAULTS, Map.of("KV_USERNAME", "user", "KV_PASSWORD", "secret")::get)) {
            var record = new DriveRecord(); record.provider = "valkey"; record.credentialsRef = "env:KV";
            var redis = RedisDriveConfig.newBuilder().setUri("redis://host:6379/2");
            record.writeProviderConfig(DriveProviderConfig.newBuilder().setRedis(redis).build());
            var a = pool.resolve(record);
            record.writeProviderConfig(DriveProviderConfig.newBuilder().setRedis(redis.setKeyPrefix("p:")).build());
            var b = pool.resolve(record);
            assertThat(a.endpoint()).isEqualTo("redis://host:6379/2");
            assertThat(a.namespace("bucket", "documents")).isNotEqualTo(b.namespace("bucket", "documents"));
        }
    }

    @Test void redisWritePoliciesAreDistinctPhysicalNamespaces() {
        var replace = new DriveBackingPool.Backing("valkey", "redis://host:6379", STORE,
                name -> {}, (bucket, key) -> true, "prefix", "replace");
        var createOnly = new DriveBackingPool.Backing("valkey", "redis://host:6379", STORE,
                name -> {}, (bucket, key) -> true, "prefix", "create-only");
        assertThat(replace.namespace("bucket", "documents"))
                .isNotEqualTo(createOnly.namespace("bucket", "documents"));
    }

    @Test void s3RegionsAndAddressingModesCannotAliasOneStackNamespace() {
        var a = new DriveBackingPool.Backing("s3", "", STORE, name -> {}, (bucket, key) -> true,
                "", "replace", "us-east-1", false);
        var b = new DriveBackingPool.Backing("s3", "", STORE, name -> {}, (bucket, key) -> true,
                "", "replace", "cn-north-1", false);
        var path = new DriveBackingPool.Backing("s3", "", STORE, name -> {}, (bucket, key) -> true,
                "", "replace", "us-east-1", true);
        assertThat(a.namespace("bucket", "documents")).isNotEqualTo(b.namespace("bucket", "documents"))
                .isNotEqualTo(path.namespace("bucket", "documents"));
    }

    private static DriveRecord s3(String endpoint) {
        var record = new DriveRecord(); record.provider = "s3"; record.region = "us-east-1";
        record.credentialsRef = "host:default";
        record.writeProviderConfig(DriveProviderConfig.newBuilder().setS3(S3DriveConfig.newBuilder()
                .setEndpointOverride(endpoint).setForcePathStyle(true)).build());
        return record;
    }
    private static OpenedBlobStore handle(AutoCloseable close) {
        return new OpenedBlobStore(STORE, close, Set.of(BlobCapability.NON_EXPIRING_WRITES,
                BlobCapability.PHYSICAL_RECLAMATION), name -> {}, (bucket, key) -> true);
    }
    private static BlobStoreProvider provider(AtomicInteger opened, List<String> closed, Set<BlobCapability> capabilities) {
        return new BlobStoreProvider() {
            public String id() { return "s3"; }
            public OpenedBlobStore open(Map<String, String> options) {
                opened.incrementAndGet();
                return new OpenedBlobStore(STORE, () -> closed.add(options.get("endpoint")), capabilities,
                        name -> {}, (bucket, key) -> true);
            }
        };
    }
}
