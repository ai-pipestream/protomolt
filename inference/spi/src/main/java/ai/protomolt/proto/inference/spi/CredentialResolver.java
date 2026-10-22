package ai.protomolt.proto.inference.spi;

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

    /** Mirrors {@code ModelEntry.credential_ref}'s validated maximum. */
    int MAX_REFERENCE_LENGTH = 256;

    /** Prevents an environment lookup from becoming an unbounded HTTP header. */
    int MAX_CREDENTIAL_LENGTH = 16 * 1024;

    /**
     * The well-formed reference shape: {@code <scheme>:<name>}, a lowercase
     * scheme and a name of word characters, dots, dashes, and slashes. Every
     * scheme validates against this one shape so a malformed reference fails
     * the same way everywhere.
     */
    private static boolean isWellFormedReference(String reference) {
        int colon = reference.indexOf(':');
        if (colon < 1 || colon == reference.length() - 1) {
            return false;
        }
        char schemeStart = reference.charAt(0);
        if (schemeStart < 'a' || schemeStart > 'z') {
            return false;
        }
        for (int i = 1; i < colon; i++) {
            char c = reference.charAt(i);
            if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-')) {
                return false;
            }
        }
        for (int i = colon + 1; i < reference.length(); i++) {
            if (!isReferenceNameChar(reference.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private static boolean isReferenceNameChar(char c) {
        return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
                || c == '.' || c == '_' || c == '/' || c == '-';
    }

    /**
     * RFC 6750 bearer-token characters, including optional base64 padding (only as a
     * trailing run).
     */
    private static boolean isBearerTokenMaterial(String credential) {
        int length = credential.length();
        int i = 0;
        while (i < length && isBearerChar(credential.charAt(i))) {
            i++;
        }
        if (i == 0) {
            return false;
        }
        while (i < length && credential.charAt(i) == '=') {
            i++;
        }
        return i == length;
    }

    private static boolean isBearerChar(char c) {
        return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
                || c == '-' || c == '.' || c == '_' || c == '~' || c == '+' || c == '/';
    }

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
     *     not have the {@code <scheme>:<name>} shape; the message never carries the
     *     reference
     */
    static void checkFormat(String credentialRef) {
        if (credentialRef == null || credentialRef.length() > MAX_REFERENCE_LENGTH
                || !isWellFormedReference(credentialRef)) {
            throw new CredentialResolutionException("malformed credential reference "
                    + "(want '<scheme>:<name>', e.g. env:OPENAI_TOKEN)");
        }
    }

    /**
     * Resolves and validates material before a transport puts it in a bearer
     * header. This guard applies to injected resolvers as well as the built-in
     * environment resolver, and deliberately discards arbitrary resolver
     * exception messages because they may contain credential material.
     *
     * @param resolver the configured resolver
     * @param credentialRef the already opaque catalog reference
     * @return bounded, header-safe bearer material
     * @throws CredentialResolutionException without the reference or material
     */
    static String resolveBearer(CredentialResolver resolver, String credentialRef) {
        checkFormat(credentialRef);
        final String credential;
        try {
            credential = resolver.resolve(credentialRef);
        } catch (RuntimeException e) {
            // Resolver implementations are plugins. Do not trust their
            // exception text or cause chain to exclude credential material.
            throw new CredentialResolutionException("credential resolution failed");
        }
        if (credential == null || credential.isBlank()) {
            throw new CredentialResolutionException(
                    "credential resolver returned no credential material");
        }
        if (credential.length() > MAX_CREDENTIAL_LENGTH) {
            throw new CredentialResolutionException(
                    "resolved credential exceeds the bearer header size limit");
        }
        if (!isBearerTokenMaterial(credential)) {
            throw new CredentialResolutionException(
                    "resolved credential is not valid bearer-token material");
        }
        return credential;
    }
}
