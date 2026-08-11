package ai.pipestream.proto.inference.spi;

import java.util.Objects;
import java.util.function.Function;

/**
 * The production {@link CredentialResolver}: the {@code env:} scheme, where
 * the reference names the environment variable holding the credential
 * ({@code env:OPENAI_TOKEN} resolves to {@code System.getenv("OPENAI_TOKEN")}).
 *
 * <p>Resolution fails fast with a {@link CredentialResolutionException} on a
 * malformed reference, a non-{@code env} scheme, or an unset or empty
 * variable. Failure messages never name the variable or carry the resolved
 * value.</p>
 *
 * <p>Instances are stateless and thread-safe.</p>
 */
public final class EnvCredentialResolver implements CredentialResolver {

    /** The scheme this resolver supports: {@value}. */
    public static final String SCHEME = "env";

    private final Function<String, String> environment;

    /** Creates the resolver reading the process environment. */
    public EnvCredentialResolver() {
        this(System::getenv);
    }

    /**
     * Creates the resolver reading variables from an explicit lookup, for
     * tests that must not touch the process environment.
     *
     * @param environment the variable lookup; null return means unset
     */
    public EnvCredentialResolver(Function<String, String> environment) {
        this.environment = Objects.requireNonNull(environment, "environment");
    }

    @Override
    public String resolve(String credentialRef) {
        CredentialResolver.checkFormat(credentialRef);
        int colon = credentialRef.indexOf(':');
        if (!SCHEME.equals(credentialRef.substring(0, colon))) {
            throw new CredentialResolutionException("unsupported credential reference "
                    + "scheme (supported: " + SCHEME + ")");
        }
        String value = environment.apply(credentialRef.substring(colon + 1));
        if (value == null || value.isEmpty()) {
            throw new CredentialResolutionException("credential reference does not "
                    + "resolve: the referenced environment variable is unset or empty");
        }
        return value;
    }
}
