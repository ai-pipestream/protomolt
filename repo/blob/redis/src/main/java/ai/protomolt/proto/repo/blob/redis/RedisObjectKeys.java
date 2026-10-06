package ai.protomolt.proto.repo.blob.redis;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;

/** Versioned, injective UTF-8 tuple encoding; no legacy key lookup. */
final class RedisObjectKeys {
    private final String prefix;
    RedisObjectKeys(String configuredPrefix) { this(configuredPrefix, RedisWritePolicy.REPLACE); }
    RedisObjectKeys(String configuredPrefix, RedisWritePolicy policy) {
        prefix = "protomolt:redis:" + (policy == RedisWritePolicy.CREATE_ONLY ? "v3-create-only" : "v2")
                + ":" + encode(configuredPrefix) + ":";
    }
    String namespace(String namespace) { requireAddress(namespace); return prefix + encode(namespace) + ":"; }
    String object(String namespace, String key) { requireAddress(key); return namespace(namespace) + encode(key); }
    String decode(String namespacePrefix, String physical) {
        if (!physical.startsWith(namespacePrefix)) throw new IllegalArgumentException("Redis object is outside namespace");
        String encoded = physical.substring(namespacePrefix.length());
        try {
            byte[] bytes = Base64.getUrlDecoder().decode(encoded);
            String value = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes)).toString();
            requireAddress(value);
            if (!encode(value).equals(encoded)) throw new IllegalArgumentException("Noncanonical Redis object key");
            return value;
        } catch (CharacterCodingException invalid) { throw new IllegalArgumentException("Invalid Redis object key encoding", invalid); }
    }
    private static void requireAddress(String value) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException("Redis namespace and object key must not be blank");
    }
    static String encode(String value) {
        Objects.requireNonNull(value);
        try {
            var encoded = StandardCharsets.UTF_8.newEncoder().onMalformedInput(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(value));
            byte[] bytes = new byte[encoded.remaining()]; encoded.get(bytes);
            return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
        } catch (CharacterCodingException invalid) { throw new IllegalArgumentException("Malformed UTF-16 in Redis identity", invalid); }
    }
}
