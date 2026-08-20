package ai.pipestream.proto.config;

import ai.pipestream.proto.receipt.TrustSnapshot;
import ai.pipestream.proto.receipt.TrustSnapshots;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Follows the trust snapshot off the config lane: one subject
 * ({@value #SUBJECT}), applied by the host's {@link DistributedConfig}
 * refresh. Verify-then-swap upstream means only a document passing the
 * snapshot's own declared rules reaches a mount; a snapshot duplicating
 * an issuer or key is refused here (the listener throws, the refresh logs
 * it, and the previous snapshot stays live). The mount is the platform's
 * custody of the same document a relying party pins as a file —
 * verification itself always takes the snapshot as an explicit input.
 */
public final class TrustSnapshotMounts {

    /** The snapshot's config subject; one snapshot per lane. */
    public static final String SUBJECT = "trust-snapshot";

    /**
     * One mounted snapshot.
     *
     * @param snapshot the trust snapshot
     * @param version the config source's version, the mount's evidence
     */
    public record Mounted(TrustSnapshot snapshot, String version) {
        public Mounted {
            Objects.requireNonNull(snapshot, "snapshot");
            Objects.requireNonNull(version, "version");
        }
    }

    private final AtomicReference<Mounted> mounted = new AtomicReference<>();

    private TrustSnapshotMounts() {
    }

    /**
     * Subscribes the snapshot subject on {@code config} and returns the
     * holder the subscription feeds.
     *
     * @param config the consumer to subscribe on; the host drives its cadence
     * @return the holder
     */
    public static TrustSnapshotMounts follow(DistributedConfig config) {
        Objects.requireNonNull(config, "config");
        TrustSnapshotMounts mounts = new TrustSnapshotMounts();
        config.subscribe(SUBJECT, TrustSnapshot.getDefaultInstance())
                .onChange((snapshot, version) -> mounts.mounted.set(
                        new Mounted(TrustSnapshots.requireWellFormed(snapshot), version)));
        return mounts;
    }

    /** The mounted snapshot, empty until a document applies. */
    public Optional<Mounted> current() {
        return Optional.ofNullable(mounted.get());
    }
}
