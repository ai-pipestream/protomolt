package ai.protomolt.proto.parse.service;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Struct;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * The {@code ParserResult.config_fingerprint} function: a lowercase-hex
 * SHA-256 over the routing-rule identity AND the parser configuration, so a
 * rule edit or a config change each cleanly invalidate a stored result.
 *
 * <p>The hash input is {@code ruleId + ':' + canonical(config)}, where
 * {@code canonical} is protobuf's deterministic serialization (map entries in
 * sorted order) — two Structs with equal content always fingerprint equal.
 */
public final class ConfigFingerprints {

    private ConfigFingerprints() {
    }

    /**
     * Fingerprints one planned parse's configuration.
     *
     * @param ruleId the routing rule that selected the parser; empty for an
     *        explicit parser override
     * @param parserConfig the parser configuration handed over verbatim
     * @return 64 lowercase hex characters of SHA-256
     */
    public static String fingerprint(String ruleId, Struct parserConfig) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update((ruleId == null ? "" : ruleId).getBytes(StandardCharsets.UTF_8));
            digest.update((byte) ':');
            digest.update(canonicalBytes(parserConfig == null ? Struct.getDefaultInstance() : parserConfig));
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    private static byte[] canonicalBytes(Struct config) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(config.getSerializedSize());
        CodedOutputStream out = CodedOutputStream.newInstance(bytes);
        out.useDeterministicSerialization();
        try {
            config.writeTo(out);
            out.flush();
        } catch (IOException e) {
            throw new IllegalStateException("in-memory serialization failed", e);
        }
        return bytes.toByteArray();
    }
}
