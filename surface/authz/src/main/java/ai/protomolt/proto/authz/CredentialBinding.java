package ai.protomolt.proto.authz;

import java.util.Objects;
import java.util.UUID;

/** Opaque identity provisioned by a credential authority, never the presented secret. */
public record CredentialBinding(String issuer, UUID credentialId, long generation) {
    public CredentialBinding {
        Objects.requireNonNull(issuer, "issuer");
        Objects.requireNonNull(credentialId, "credentialId");
        if (issuer.isBlank() || issuer.length() > 128 || issuer.codePoints().anyMatch(Character::isISOControl))
            throw new IllegalArgumentException("Credential issuer must be nonblank, at most 128 characters and contain no controls");
        if (generation <= 0) throw new IllegalArgumentException("Credential generation must be positive");
    }
}
