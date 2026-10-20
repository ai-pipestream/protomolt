package ai.protomolt.proto.serve;

import ai.protomolt.proto.config.DistributedConfig;
import ai.protomolt.proto.config.TrustSnapshotMounts;
import ai.protomolt.proto.config.registry.RegistryConfigSource;
import ai.protomolt.proto.receipt.TrustSnapshot;
import ai.protomolt.proto.workflow.TrustPin;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The config lane of the serve wiring: the trust snapshot the verifying verbs fall back to.
 * Without a config lane this is the operator's pinned file, resolved once, exactly as before.
 * With one, the lane's snapshot takes precedence when a document has applied and the file
 * remains the floor, so a node keeps verifying across a lane outage instead of losing custody.
 * Either way a request's own {@code trust} still wins.
 */
final class ConfigLaneMount {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigLaneMount.class);

    private ConfigLaneMount() {
    }

    /**
     * Resolves the trust source for the configured lane. The boot pull runs before the catalog
     * is assembled; with a positive refresh interval a single platform thread keeps pulling on
     * the delay schedule, which is the backpressure - serialising refreshes is what keeps two
     * config pulls from overlapping.
     */
    static Supplier<TrustSnapshot> trustSource(ProtoMoltServe.ConfigLaneOptions lane,
                                               String apiToken) {
        TrustPin pinned = TrustPin.fromEnvironment();
        TrustSnapshot file = pinned == null ? null : pinned.snapshot();
        if (lane == null) {
            return () -> file;
        }
        DistributedConfig config = DistributedConfig.over(
                new RegistryConfigSource(lane.registryUrl(), apiToken));
        TrustSnapshotMounts mounts = TrustSnapshotMounts.follow(config);
        refreshQuietly(config, "the boot pull");
        if (lane.refreshSeconds() > 0) {
            ScheduledExecutorService refresher = Executors.newSingleThreadScheduledExecutor(
                    Thread.ofPlatform().name("serve-config-refresh").daemon().factory());
            refresher.scheduleWithFixedDelay(
                    () -> refreshQuietly(config, "a config refresh"),
                    lane.refreshSeconds(), lane.refreshSeconds(), TimeUnit.SECONDS);
        }
        return () -> mounts.current()
                .map(TrustSnapshotMounts.Mounted::snapshot)
                .orElse(file);
    }

    /**
     * A lane that cannot be reached must not take the server down or drop the custody it
     * already has: the refresh logs and the previous snapshot stays live.
     */
    private static void refreshQuietly(DistributedConfig config, String what) {
        try {
            config.refresh();
        } catch (RuntimeException e) {
            LOG.warn("{} of the config lane failed; the previous trust snapshot stays"
                    + " live", what, e);
        }
    }
}
