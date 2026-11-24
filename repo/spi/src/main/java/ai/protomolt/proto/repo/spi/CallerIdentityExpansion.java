package ai.protomolt.proto.repo.spi;

import ai.protomolt.proto.repo.v1.IdentityLink;
import ai.protomolt.proto.repo.v1.Principal;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Expands a caller's ACL identities along identity links: every identity
 * reachable from the caller's own, matched case-insensitively on identity
 * type and identity, the way access rules match. An expansion that reaches more than
 * {@link #DEFAULT_MAX_IDENTITIES} identities (or the configured limit) is refused.
 */
public final class CallerIdentityExpansion {

    /** The most identities one expansion may reach by default: {@value}. */
    public static final int DEFAULT_MAX_IDENTITIES = 10_000;

    private static final String PUBLIC_IDENTITY = "public";

    private final Map<AclIdentityKey, List<Principal>> next;
    private final int maxIdentities;

    /**
     * Creates an expansion over {@code links}.
     *
     * @param links the identity links; null, blank, malformed, or unknown-field principals are rejected
     */
    public CallerIdentityExpansion(Collection<IdentityLink> links) {
        this(links, DEFAULT_MAX_IDENTITIES);
    }

    /**
     * Creates an expansion over {@code links} that reaches at most {@code maxIdentities} identities.
     *
     * @param links the identity links; null, blank, malformed, or unknown-field principals are rejected
     * @param maxIdentities the most identities one expansion may reach, at least 1
     */
    public CallerIdentityExpansion(Collection<IdentityLink> links, int maxIdentities) {
        Objects.requireNonNull(links, "links");
        if (maxIdentities < 1) {
            throw new IllegalArgumentException("maxIdentities must be >= 1");
        }
        this.maxIdentities = maxIdentities;
        Map<AclIdentityKey, List<Principal>> byFrom = new HashMap<>();
        for (IdentityLink link : links) {
            Objects.requireNonNull(link, "link");
            if (!link.getUnknownFields().asMap().isEmpty()) {
                throw new IllegalArgumentException("Identity link contains unknown fields");
            }
            requireValidLinkPrincipal(link.getFrom());
            requireValidLinkPrincipal(link.getTo());
            byFrom.computeIfAbsent(key(link.getFrom()), k -> new ArrayList<>()).add(link.getTo());
        }
        this.next = byFrom;
    }

    /**
     * The caller's identities plus every identity linked from them, transitively.
     *
     * @param identities the caller's own identities
     * @return the expanded identities, the caller's own first
     * @throws IllegalStateException when the expansion reaches more identities than the limit
     */
    public Set<Principal> expand(Set<Principal> identities) {
        Objects.requireNonNull(identities, "identities");
        for (Principal identity : identities) {
            requireValidCallerPrincipal(identity);
        }
        Map<AclIdentityKey, Principal> seen = new LinkedHashMap<>();
        Deque<Principal> pending = new ArrayDeque<>(identities);
        while (!pending.isEmpty()) {
            Principal identity = pending.removeFirst();
            AclIdentityKey identityKey = key(identity);
            if (seen.putIfAbsent(identityKey, identity) == null) {
                if (seen.size() > maxIdentities) {
                    throw new IllegalStateException("Caller identity expansion exceeds " + maxIdentities + " identities");
                }
                pending.addAll(next.getOrDefault(identityKey, List.of()));
            }
        }
        return new LinkedHashSet<>(seen.values());
    }

    /**
     * The caller with its identities expanded.
     *
     * @param caller the caller
     * @return the same caller with the expanded identities, in no particular order; process
     *     authority unchanged
     * @throws IllegalStateException when the expansion reaches more identities than the limit
     */
    public RepositoryCaller expand(RepositoryCaller caller) {
        Objects.requireNonNull(caller, "caller");
        if (caller.processAuthority()) {
            return caller;
        }
        return new RepositoryCaller(caller.principalName(), false, caller.accountIds(),
                expand(caller.identities()), caller.credentialBinding());
    }

    private static AclIdentityKey key(Principal identity) {
        return new AclIdentityKey(identity.getIdentityType(), identity.getIdentity());
    }

    private static void requireValidLinkPrincipal(Principal identity) {
        requireValidCallerPrincipal(identity);
        if (identity.getIdentityType().equalsIgnoreCase(PUBLIC_IDENTITY)
                && !identity.getIdentity().equalsIgnoreCase(PUBLIC_IDENTITY)) {
            throw new IllegalArgumentException("Public identity link must use identity 'public'");
        }
    }

    private static void requireValidCallerPrincipal(Principal identity) {
        if (identity == null || identity.getIdentity().isBlank() || identity.getIdentityType().isBlank()
                || !identity.getUnknownFields().asMap().isEmpty()) {
            throw new IllegalArgumentException("Identity link needs an identity and an identity type");
        }
    }
}
