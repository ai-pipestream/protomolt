package ai.protomolt.proto.repo.blob.redis;

import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Bounded UTF-8 metadata encoding before any server-side allocation or mutation. */
final class RedisWriteMetadata {
    private static final int MAX_ENTRIES = 256;
    private static final int MAX_BYTES = 64 * 1024;
    private int remaining = MAX_BYTES;
    final byte[] contentType;
    final List<byte[]> attributes;

    RedisWriteMetadata(String type, Map<String, String> metadata) {
        contentType = encode(type == null ? "" : type);
        var encoded = new ArrayList<byte[]>();
        if (metadata != null) {
            if (metadata.size() > MAX_ENTRIES) throw new IllegalArgumentException("Redis metadata exceeds 256 entries");
            int count = 0;
            for (var entry : metadata.entrySet()) {
                if (++count > MAX_ENTRIES) throw new IllegalArgumentException("Redis metadata exceeds 256 entries");
                encoded.add(encode(entry.getKey()));
                encoded.add(encode(entry.getValue()));
            }
        }
        attributes = List.copyOf(encoded);
    }

    private byte[] encode(String value) {
        Objects.requireNonNull(value, "Redis metadata cannot contain null names or values");
        // UTF-8 cannot use fewer bytes than the UTF-16 length for a valid string.
        if (value.length() > remaining) throw new IllegalArgumentException("Redis metadata exceeds 64 KiB UTF-8");
        try {
            var encoded = StandardCharsets.UTF_8.newEncoder().onMalformedInput(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(value));
            if (encoded.remaining() > remaining) throw new IllegalArgumentException("Redis metadata exceeds 64 KiB UTF-8");
            byte[] bytes = new byte[encoded.remaining()];
            encoded.get(bytes);
            remaining -= bytes.length;
            return bytes;
        } catch (CharacterCodingException invalid) {
            throw new IllegalArgumentException("Redis metadata contains malformed UTF-16", invalid);
        }
    }
}
