package ai.pipestream.proto.validate.model;

import com.google.protobuf.ByteString;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Neutral constraints for bytes fields. Violation rule ids are the fixed
 * {@code bytes.*} ids ({@code bytes.len}, {@code bytes.prefix}, …). Length rules
 * count bytes.
 */
public record BytesConstraints(
        OptionalLong len,
        OptionalLong minLen,
        OptionalLong maxLen,
        Optional<ByteString> prefix,
        Optional<ByteString> suffix,
        Optional<ByteString> contains) {

    public BytesConstraints {
        Objects.requireNonNull(len, "len");
        Objects.requireNonNull(minLen, "minLen");
        Objects.requireNonNull(maxLen, "maxLen");
        Objects.requireNonNull(prefix, "prefix");
        Objects.requireNonNull(suffix, "suffix");
        Objects.requireNonNull(contains, "contains");
    }

    public boolean isEmpty() {
        return len.isEmpty() && minLen.isEmpty() && maxLen.isEmpty()
                && prefix.isEmpty() && suffix.isEmpty() && contains.isEmpty();
    }
}
