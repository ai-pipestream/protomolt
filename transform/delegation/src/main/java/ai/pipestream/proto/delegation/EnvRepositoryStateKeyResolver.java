package ai.pipestream.proto.delegation;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.util.Base64;
import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Pattern;

/** Resolves {@code env:VARIABLE_NAME} references containing base64-encoded AES-256 keys. */
public final class EnvRepositoryStateKeyResolver implements RepositoryStateKeyResolver {

    private static final Pattern VARIABLE = Pattern.compile("[A-Za-z_][A-Za-z0-9_]{0,127}");

    /** Looks up environment variables without exposing the environment API to callers. */
    private final Function<String, String> environment;

    /** Creates a resolver backed by the process environment. */
    public EnvRepositoryStateKeyResolver() {
        this(System::getenv);
    }

    /** Creates a resolver with an injectable environment lookup for tests and embedding. */
    public EnvRepositoryStateKeyResolver(Function<String, String> environment) {
        this.environment = Objects.requireNonNull(environment, "environment");
    }

    @Override
    public SecretKey resolve(String keyReference) {
        if (keyReference == null || !keyReference.startsWith("env:")
                || !VARIABLE.matcher(keyReference.substring(4)).matches()) {
            throw failure("repository state key reference is malformed or uses an unsupported scheme");
        }
        String encoded = environment.apply(keyReference.substring(4));
        if (encoded == null || encoded.isBlank()) {
            throw failure("repository state encryption key is unavailable");
        }
        byte[] key;
        try {
            key = Base64.getDecoder().decode(encoded);
        } catch (IllegalArgumentException e) {
            throw failure("repository state encryption key is not valid base64");
        }
        if (key.length != 32) {
            throw failure("repository state encryption key must contain exactly 32 bytes");
        }
        return new SecretKeySpec(key, "AES");
    }

    private static IllegalStateException failure(String message) {
        return new IllegalStateException(message);
    }
}
