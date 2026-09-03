package ai.protomolt.proto.delegation;

import javax.crypto.SecretKey;

/** Resolves local encryption keys from opaque references stored with repository state. */
@FunctionalInterface
public interface RepositoryStateKeyResolver {

    /**
     * Resolves one 256-bit AES key. Implementations must not include the reference or
     * key material in exceptions.
     */
    SecretKey resolve(String keyReference);
}
