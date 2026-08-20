package ai.pipestream.proto.authz;

import ai.pipestream.proto.actions.Caller;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * The shipped {@link CallerResolver}: a presented credential's SHA-256 digest looked up in
 * a well-formed {@link AccessPolicy}. Several digests on one principal resolve to the same
 * caller, which is what makes rotation-with-grace a property of the model. The policy holds
 * digests only; the credential itself exists nowhere but in the request.
 */
public final class AccessPolicyCallers implements CallerResolver {

    private final Map<String, Caller> byDigest;

    public AccessPolicyCallers(AccessPolicy policy) {
        AccessPolicies.requireWellFormed(policy);
        Map<String, Caller> resolved = new HashMap<>();
        for (Principal principal : policy.getPrincipalsList()) {
            Caller caller = Caller.scoped(principal.getName(),
                    Set.copyOf(principal.getScopesList()));
            for (String digest : principal.getCredentialSha256List()) {
                resolved.put(digest, caller);
            }
        }
        this.byDigest = Map.copyOf(resolved);
    }

    @Override
    public Optional<Caller> resolve(String credential) {
        Objects.requireNonNull(credential, "credential");
        return Optional.ofNullable(byDigest.get(sha256Hex(credential)));
    }

    /** The lowercase-hex SHA-256 of a credential's UTF-8 bytes — what a policy stores. */
    public static String sha256Hex(String credential) {
        Objects.requireNonNull(credential, "credential");
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(credential.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is a required JDK algorithm", e);
        }
    }
}
