package ai.pipestream.proto.authz;

import ai.pipestream.proto.actions.Scopes;
import ai.pipestream.proto.validate.ProtoValidator;
import ai.pipestream.proto.validate.ValidationResult;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Access-policy preconditions shared by everything that accepts one: the document's own
 * declared rules must hold, principal names and credential digests must be unique across
 * the document, and every scope must be in the closed vocabulary. A policy failing here is
 * the holder's error and refuses loudly naming the defect; a typo in a scope is a refusal,
 * never a silently dead grant.
 */
public final class AccessPolicies {

    private AccessPolicies() {
    }

    /** Refuses an invalid or ambiguous policy; returns it for chaining. */
    public static AccessPolicy requireWellFormed(AccessPolicy policy) {
        if (policy == null) {
            throw new IllegalArgumentException("access policy must not be null");
        }
        ValidationResult rules = ProtoValidator.create().validate(policy);
        if (!rules.valid()) {
            throw new IllegalArgumentException("access policy is invalid: "
                    + rules.violations().stream()
                            .map(violation -> violation.path() + ": " + violation.message())
                            .collect(Collectors.joining("; ")));
        }
        Set<String> names = new HashSet<>();
        Set<String> digests = new HashSet<>();
        for (Principal principal : policy.getPrincipalsList()) {
            if (!names.add(principal.getName())) {
                throw new IllegalArgumentException(
                        "access policy duplicates principal '" + principal.getName() + "'");
            }
            for (String digest : principal.getCredentialSha256List()) {
                if (!digests.add(digest)) {
                    throw new IllegalArgumentException("access policy duplicates credential "
                            + "digest '" + digest.substring(0, 12) + "…' under principal '"
                            + principal.getName() + "'");
                }
            }
            for (String scope : principal.getScopesList()) {
                if (!Scopes.VOCABULARY.contains(scope)) {
                    throw new IllegalArgumentException("principal '" + principal.getName()
                            + "' names unknown scope '" + scope + "'; the vocabulary is "
                            + String.join(", ", Scopes.VOCABULARY));
                }
            }
        }
        return policy;
    }

    /**
     * Loads and verifies a policy file. The extension decides the format — {@code .json}
     * parses as canonical proto3 JSON refusing unknown fields, {@code .binpb} or {@code .pb}
     * parses as the binary message — and anything else is refused naming the accepted forms.
     */
    public static AccessPolicy load(Path file) throws IOException {
        String name = file.getFileName().toString();
        if (name.endsWith(".json")) {
            AccessPolicy.Builder builder = AccessPolicy.newBuilder();
            try {
                JsonFormat.parser().merge(Files.readString(file), builder);
            } catch (InvalidProtocolBufferException e) {
                throw new IllegalArgumentException(
                        "access policy file '" + name + "' does not parse: " + e.getMessage(), e);
            }
            return requireWellFormed(builder.build());
        }
        if (name.endsWith(".binpb") || name.endsWith(".pb")) {
            try {
                return requireWellFormed(AccessPolicy.parseFrom(Files.readAllBytes(file)));
            } catch (InvalidProtocolBufferException e) {
                throw new IllegalArgumentException(
                        "access policy file '" + name + "' does not parse: " + e.getMessage(), e);
            }
        }
        throw new IllegalArgumentException("access policy file '" + name
                + "' must end in .json, .binpb, or .pb");
    }
}
