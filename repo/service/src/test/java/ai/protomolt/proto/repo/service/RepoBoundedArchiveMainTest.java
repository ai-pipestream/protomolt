package ai.protomolt.proto.repo.service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class RepoBoundedArchiveMainTest {
    static Map<String, String> environment() {
        var env = new HashMap<String, String>();
        env.put("PROTOMOLT_API_TOKEN", "test-token");
        env.put("DOCUMENT_PLATFORM_ARCHIVE_ACCOUNT", "account");
        env.put("DOCUMENT_PLATFORM_ARCHIVE_DRIVE", "storage");
        env.put(RepoServiceConfig.ENV_BLOB_STORE, "redis");
        env.put(RepoServiceConfig.ENV_REDIS_MAX_OBJECT_BYTES, "1048576");
        env.put(RepoServiceConfig.ENV_REDIS_TTL_SECONDS, "0");
        env.put(ManagedStoragePolicy.ENV_GENERATION, "test-redis");
        env.put(ManagedStoragePolicy.ENV_REALM, "test-realm");
        env.put(ManagedStoragePolicy.ENV_RETENTION, "true");
        return env;
    }

    @Test void parsesExplicitProviderAndBoundedDefaultsWithoutOpeningResources() {
        var parsed = RepoBoundedArchiveMain.Settings.parse(environment());
        assertThat(parsed.config.blobStore()).isEqualTo("redis");
        assertThat(parsed.limits.maxObjectBytes()).isEqualTo(1048576);
        assertThat(parsed.limits.maxResponseBytes()).isEqualTo(parsed.limits.maxRequestBytes());
        assertThat(parsed.account).isEqualTo("account");
    }

    @Test void refusesMissingIdentityAndMalformedOrUnqualifiedSettings() {
        for (String name : new String[]{"PROTOMOLT_API_TOKEN", "DOCUMENT_PLATFORM_ARCHIVE_ACCOUNT", "DOCUMENT_PLATFORM_ARCHIVE_DRIVE"}) {
            var env = environment(); env.remove(name);
            assertThatThrownBy(() -> RepoBoundedArchiveMain.Settings.parse(env)).hasMessageContaining(name);
        }
        for (String name : new String[]{"DOCUMENT_PLATFORM_ARCHIVE_MAX_OBJECT_BYTES", "DOCUMENT_PLATFORM_ARCHIVE_MAX_REQUEST_BYTES",
                "DOCUMENT_PLATFORM_ARCHIVE_MAX_RENDITIONS", "DOCUMENT_PLATFORM_ARCHIVE_PAYLOAD_BUDGET_BYTES",
                "DOCUMENT_PLATFORM_ARCHIVE_MAX_CONCURRENT_REQUESTS", "DOCUMENT_PLATFORM_ARCHIVE_MAX_RESPONSE_BYTES",
                "DOCUMENT_PLATFORM_ARCHIVE_MAX_MANIFEST_BYTES"}) {
            var env = environment(); env.put(name, "bad-limit");
            assertThatThrownBy(() -> RepoBoundedArchiveMain.Settings.parse(env)).hasMessageContaining(name);
        }
        for (var bad : Map.of(RepoServiceConfig.ENV_BLOB_STORE, "s3", RepoServiceConfig.ENV_REDIS_TTL_SECONDS, "1",
                ManagedStoragePolicy.ENV_RETENTION, "false", RepoServiceConfig.ENV_LIFECYCLE_ENABLED, "false").entrySet()) {
            var env = environment(); env.put(bad.getKey(), bad.getValue());
            assertThatThrownBy(() -> RepoBoundedArchiveMain.Settings.parse(env)).isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test void retriesOnlyDirectDrainTimeouts() {
        var calls = new AtomicInteger();
        RepoBoundedArchiveMain.drain(() -> {
            if (calls.incrementAndGet() < 3) throw new RepositoryDrainTimeoutException(
                    RepositoryDrainTimeoutException.Phase.ARCHIVE_PUT, "held");
        });
        assertThat(calls.get()).isEqualTo(3);
        var failure = new IllegalStateException("provider failure");
        failure.addSuppressed(new RepositoryDrainTimeoutException(RepositoryDrainTimeoutException.Phase.ARCHIVE_PUT, "held"));
        calls.set(0);
        assertThatThrownBy(() -> RepoBoundedArchiveMain.drain(() -> { calls.incrementAndGet(); throw failure; }))
                .isSameAs(failure);
        assertThat(calls.get()).isEqualTo(1);
    }

    @Test void exceptionalServingExitDrainsBeforeRestoringInterruption() {
        var calls = new AtomicInteger();
        var failure = new InterruptedException("await terminated");
        Thread.currentThread().interrupt();
        try {
            RepoBoundedArchiveMain.cleanupAfterServing(() -> {
                assertThat(Thread.currentThread().isInterrupted()).isFalse();
                if (calls.incrementAndGet() == 1) throw new RepositoryDrainTimeoutException(
                        RepositoryDrainTimeoutException.Phase.ARCHIVE_RPC, "held");
            }, failure);
            assertThat(calls.get()).isEqualTo(2);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            assertThat(failure.getSuppressed()).isEmpty();
        } finally { Thread.interrupted(); }
    }

    @Test void restoresInterruptConsumedByAwaitTermination() {
        assertThat(Thread.currentThread().isInterrupted()).isFalse();
        try {
            RepoBoundedArchiveMain.cleanupAfterServing(() -> {}, new InterruptedException("await terminated"));
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally { Thread.interrupted(); }
    }
}
