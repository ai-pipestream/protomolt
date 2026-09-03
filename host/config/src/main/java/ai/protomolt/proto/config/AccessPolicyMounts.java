package ai.protomolt.proto.config;

import ai.protomolt.proto.authz.AccessPolicies;
import ai.protomolt.proto.authz.AccessPolicy;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Follows the access policy off the config lane: one subject ({@value #SUBJECT}), applied
 * by the host's {@link DistributedConfig} refresh. A document duplicating a principal or a
 * digest, or naming a scope outside the vocabulary, is refused here (the listener throws,
 * the refresh logs it, and the previous policy stays live), so a running fleet re-scopes
 * without a restart and a bad edit never takes authority away mid-flight.
 */
public final class AccessPolicyMounts {

    /** The policy's config subject; one policy per lane. */
    public static final String SUBJECT = "access-policy";

    /**
     * One mounted policy.
     *
     * @param policy the access policy
     * @param version the config source's version, the mount's evidence
     */
    public record Mounted(AccessPolicy policy, String version) {
        public Mounted {
            Objects.requireNonNull(policy, "policy");
            Objects.requireNonNull(version, "version");
        }
    }

    private final AtomicReference<Mounted> mounted = new AtomicReference<>();

    private AccessPolicyMounts() {
    }

    /**
     * Subscribes the policy subject on {@code config} and returns the holder the
     * subscription feeds.
     *
     * @param config the consumer to subscribe on; the host drives its cadence
     * @return the holder
     */
    public static AccessPolicyMounts follow(DistributedConfig config) {
        Objects.requireNonNull(config, "config");
        AccessPolicyMounts mounts = new AccessPolicyMounts();
        config.subscribe(SUBJECT, AccessPolicy.getDefaultInstance())
                .onChange((policy, version) -> mounts.mounted.set(
                        new Mounted(AccessPolicies.requireWellFormed(policy), version)));
        return mounts;
    }

    /** The mounted policy, empty until a document applies. */
    public Optional<Mounted> current() {
        return Optional.ofNullable(mounted.get());
    }
}
