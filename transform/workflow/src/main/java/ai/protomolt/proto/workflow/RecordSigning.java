package ai.protomolt.proto.workflow;

import ai.protomolt.proto.receipt.RecordKeys;
import ai.protomolt.proto.receipt.RecordSigner;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The operator-supplied record-signing identity: who issues and with which
 * key. Resolved from the environment; the three variables are
 * all-or-nothing, and a partial configuration refuses at startup naming
 * the missing pieces. The key file holds the raw 32-byte Ed25519 seed and
 * its content never appears in config documents, the registry, logs, or
 * errors.
 *
 * @param issuer the issuer name signed into every record
 * @param signer the signer holding the private key
 */
public record RecordSigning(String issuer, RecordSigner signer) {

    /** Path of the file holding the raw 32-byte Ed25519 seed. */
    public static final String ENV_KEY_FILE = "PROTOMOLT_RECEIPT_KEY_FILE";
    /** The key id the trust snapshot knows the key by. */
    public static final String ENV_KEY_ID = "PROTOMOLT_RECEIPT_KEY_ID";
    /** The issuer name records carry. */
    public static final String ENV_ISSUER = "PROTOMOLT_RECEIPT_ISSUER";

    public RecordSigning {
        Objects.requireNonNull(issuer, "issuer");
        Objects.requireNonNull(signer, "signer");
    }

    /** Resolves from the process environment; null when none of it is set. */
    public static RecordSigning fromEnvironment() {
        return fromEnvironment(System.getenv());
    }

    static RecordSigning fromEnvironment(Map<String, String> env) {
        String keyFile = value(env, ENV_KEY_FILE);
        String keyId = value(env, ENV_KEY_ID);
        String issuer = value(env, ENV_ISSUER);
        if (keyFile == null && keyId == null && issuer == null) {
            return null;
        }
        List<String> missing = new ArrayList<>();
        if (keyFile == null) {
            missing.add(ENV_KEY_FILE);
        }
        if (keyId == null) {
            missing.add(ENV_KEY_ID);
        }
        if (issuer == null) {
            missing.add(ENV_ISSUER);
        }
        if (!missing.isEmpty()) {
            throw new IllegalArgumentException(
                    "record signing is partially configured; missing " + missing);
        }
        byte[] seed;
        try {
            seed = Files.readAllBytes(Path.of(keyFile));
        } catch (IOException e) {
            throw new UncheckedIOException("cannot read the record signing key file", e);
        }
        return new RecordSigning(issuer, new RecordSigner(keyId, RecordKeys.privateKey(seed)));
    }

    private static String value(Map<String, String> env, String name) {
        String value = env.get(name);
        return value == null || value.isBlank() ? null : value;
    }
}
