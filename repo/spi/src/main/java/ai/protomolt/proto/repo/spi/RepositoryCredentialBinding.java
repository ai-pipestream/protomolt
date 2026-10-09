package ai.protomolt.proto.repo.spi;

import java.util.Objects;
import java.util.UUID;

/** Trusted host credential identity, independent of authentication and storage implementations. */
public record RepositoryCredentialBinding(String issuer, UUID credentialId, long generation) {
    public RepositoryCredentialBinding {
        Objects.requireNonNull(issuer, "issuer");
        Objects.requireNonNull(credentialId, "credentialId");
        if (issuer.isBlank() || issuer.length() > 128 || issuer.codePoints().anyMatch(Character::isISOControl))
            throw new IllegalArgumentException("Credential issuer must be nonblank, at most 128 characters and contain no controls");
        if (generation <= 0) throw new IllegalArgumentException("Credential generation must be positive");
    }
}
