package ai.pipestream.proto.inference.spi;

import java.util.regex.Pattern;

/**
 * Resolves a model catalog entry's opaque credential reference
 * ({@code ModelEntry.credential_ref}, e.g. {@code "env:OPENAI_TOKEN"}) to the
 * credential material the transport presents to the model's endpoint.
 *
 * <p>The reference is a pointer, never a secret: the catalog stores
 * {@code env:OPENAI_TOKEN}, and the resolver turns it into the token at
 * request time. Implementations fail fast with a
 * {@link CredentialResolutionException} on a malformed reference, an
 * unsupported scheme, or a reference that does not resolve; messages name the
 * failure class only and never carry the reference or the resolved value.</p>
 *
 * <p>The production implementation is {@link EnvCredentialResolver} (the
 * {@code env:} scheme). Tests inject a fake directly — the interface is
 * functional, so {@code ref -> "test-token"} is a complete resolver.</p>
 *
 * <p>Implementations must be thread-safe; one resolver serves every request
 * of every model on the provider that holds it.</p>
 */
@FunctionalInterface
public interface CredentialResolver {

    /**
     * The well-formed reference shape: {@code <scheme>:<name>}, a lowercase
     * scheme and a name of word characters, dots, dashes, and slashes. Every
     * scheme validates against this one shape so a malformed reference fails
     * the same way everywhere.
     */
    Pattern REFERENCE_FORMAT = Pattern.compile("^[a-z][a-z0-9-]*:[A-Za-z0-9._/-]+$");

    /**
     * Resolves one reference to its credential material.
     *
     * @param credentialRef the opaque reference from the catalog entry
     *     (e.g. {@code "env:OPENAI_TOKEN"})
     * @return the resolved credential, never null or empty
     * @throws CredentialResolutionException on a malformed reference, an
     *     unsupported scheme, or an unresolvable reference; the message never
     *     carries the reference or any resolved material
     */
    String resolve(String credentialRef);

    /**
     * Fails fast when a reference is not well formed. Hosts call this at
     * configuration time (e.g. launcher spec parsing) so a malformed
     * reference fails startup rather than the first request.
     *
     * @param credentialRef the reference to check
     * @throws CredentialResolutionException when the reference is null or does
     *     not match {@link #REFERENCE_FORMAT}; the message never carries the
     *     reference
     */
    static void checkFormat(String credentialRef) {
        if (credentialRef == null || !REFERENCE_FORMAT.matcher(credentialRef).matches()) {
            throw new CredentialResolutionException("malformed credential reference "
                    + "(want '<scheme>:<name>', e.g. env:OPENAI_TOKEN)");
        }
    }
}
