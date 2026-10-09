package ai.protomolt.proto.repo.spi;

import java.util.Objects;
import java.util.Optional;

/**
 * Measured byte identity at a reserved location, recorded by trusted admission.
 * Construction validates the value's shape; it does not verify stored bytes,
 * publish a revision, acquire retention, or authorize access.
 *
 * <p>A present provider version selects an exact immutable version qualified by
 * the adapter. Provider-specific sentinel values must be rejected by that
 * adapter. An absent version requires an explicitly qualified never-reused key;
 * it must not stand for missing legacy information or an unchecked provider.
 *
 * <p>The digest and size describe verified stored bytes. A checksum match alone
 * does not establish object identity or permission to reuse those bytes. Content
 * type describes their storage representation, not a protobuf Any type or schema
 * identity. Admission must confirm stored content type rather than copying an
 * unverified request value. Verification evidence and schema admission are
 * separate records.
 */
public record PhysicalObjectIdentity(PhysicalObjectLocation location,
        Optional<String> providerVersion, long sizeBytes, String sha256, String contentType) {
    public PhysicalObjectIdentity {
        Objects.requireNonNull(location, "location");
        Objects.requireNonNull(providerVersion, "providerVersion");
        if (providerVersion.isPresent() && providerVersion.get().isBlank())
            throw new IllegalArgumentException("providerVersion must not be blank");
        if (sizeBytes < 0)
            throw new IllegalArgumentException("sizeBytes must not be negative");
        if (sha256 == null || sha256.length() != 64
                || sha256.chars().anyMatch(c -> !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f')))
            throw new IllegalArgumentException("sha256 must be 64 lowercase hexadecimal characters");
        if (contentType == null || contentType.isBlank())
            throw new IllegalArgumentException("contentType must not be blank");
    }
}
