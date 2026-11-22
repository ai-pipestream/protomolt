package ai.protomolt.proto.repo.spi;

import ai.protomolt.proto.repo.v1.IdentityLink;
import ai.protomolt.proto.repo.v1.Principal;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Expands a caller's ACL identities along identity links: every identity
 * reachable from the caller's own, matched case-insensitively on identity
 * type and identity, the way access rules match.
 */
public final class CallerIdentityExpansion {

    private final Map<String, List<Principal>> next;

    /**
     * Creates an expansion over {@code links}.
     *
     * @param links the identity links; blank identities are rejected
     */
    public CallerIdentityExpansion(Collection<IdentityLink> links) {
        Objects.requireNonNull(links, "links");
        Map<String, List<Principal>> byFrom = new HashMap<>();
        for (IdentityLink link : links) {
            requireValid(link.getFrom());
            requireValid(link.getTo());
            byFrom.computeIfAbsent(key(link.getFrom()), k -> new java.util.ArrayList<>()).add(link.getTo());
        }
        this.next = byFrom;
    }

    /**
     * The caller's identities plus every identity linked from them, transitively.
     *
     * @param identities the caller's own identities
     * @return the expanded identities, the caller's own first
     */
    public Set<Principal> expand(Set<Principal> identities) {
        Objects.requireNonNull(identities, "identities");
        Map<String, Principal> seen = new LinkedHashMap<>();
        Deque<Principal> pending = new ArrayDeque<>(identities);
        while (!pending.isEmpty()) {
            Principal identity = pending.removeFirst();
            if (seen.putIfAbsent(key(identity), identity) == null) {
                pending.addAll(next.getOrDefault(key(identity), List.of()));
            }
        }
        return new LinkedHashSet<>(seen.values());
    }

    /**
     * The caller with its identities expanded.
     *
     * @param caller the caller
     * @return the same caller with the expanded identities; process authority unchanged
     */
    public RepositoryCaller expand(RepositoryCaller caller) {
        Objects.requireNonNull(caller, "caller");
        if (caller.processAuthority()) {
            return caller;
        }
        return new RepositoryCaller(caller.principalName(), false, caller.accountIds(),
                expand(caller.identities()), caller.credentialBinding());
    }

    private static String key(Principal identity) {
        return identity.getIdentityType().toLowerCase(Locale.ROOT) + "\u0000"
                + identity.getIdentity().toLowerCase(Locale.ROOT);
    }

    private static void requireValid(Principal identity) {
        if (identity.getIdentity().isBlank() || identity.getIdentityType().isBlank()) {
            throw new IllegalArgumentException("Identity link needs an identity and an identity type");
        }
    }
}
