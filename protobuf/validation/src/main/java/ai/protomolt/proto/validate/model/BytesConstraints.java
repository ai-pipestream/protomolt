package ai.protomolt.proto.validate.model;

import com.google.protobuf.ByteString;

import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

/**
 * Neutral constraints for bytes fields. Violation rule ids are the fixed
 * {@code bytes.*} ids ({@code bytes.len}, {@code bytes.prefix}, {@code bytes.pattern}, …). Length
 * rules count bytes; {@code pattern} matches the bytes decoded one-byte-per-char (Latin-1).
 */
public record BytesConstraints(
        Optional<ByteString> constant,
        OptionalLong len,
        OptionalLong minLen,
        OptionalLong maxLen,
        Optional<ByteString> prefix,
        Optional<ByteString> suffix,
        Optional<ByteString> contains,
        Optional<String> pattern,
        List<ByteString> in,
        List<ByteString> notIn,
        Set<BytesFormat> formats) {

    public BytesConstraints {
        Objects.requireNonNull(constant, "constant");
        Objects.requireNonNull(len, "len");
        Objects.requireNonNull(minLen, "minLen");
        Objects.requireNonNull(maxLen, "maxLen");
        Objects.requireNonNull(prefix, "prefix");
        Objects.requireNonNull(suffix, "suffix");
        Objects.requireNonNull(contains, "contains");
        Objects.requireNonNull(pattern, "pattern");
        in = List.copyOf(Objects.requireNonNull(in, "in"));
        notIn = List.copyOf(Objects.requireNonNull(notIn, "notIn"));
        Objects.requireNonNull(formats, "formats");
        formats = formats.isEmpty() ? Set.of() : Set.copyOf(EnumSet.copyOf(formats));
    }

    public boolean isEmpty() {
        return constant.isEmpty() && len.isEmpty() && minLen.isEmpty() && maxLen.isEmpty()
                && prefix.isEmpty() && suffix.isEmpty() && contains.isEmpty()
                && pattern.isEmpty() && in.isEmpty() && notIn.isEmpty() && formats.isEmpty();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private Optional<ByteString> constant = Optional.empty();
        private OptionalLong len = OptionalLong.empty();
        private OptionalLong minLen = OptionalLong.empty();
        private OptionalLong maxLen = OptionalLong.empty();
        private Optional<ByteString> prefix = Optional.empty();
        private Optional<ByteString> suffix = Optional.empty();
        private Optional<ByteString> contains = Optional.empty();
        private Optional<String> pattern = Optional.empty();
        private List<ByteString> in = List.of();
        private List<ByteString> notIn = List.of();
        private final EnumSet<BytesFormat> formats = EnumSet.noneOf(BytesFormat.class);

        public Builder constant(ByteString v) {
            this.constant = Optional.ofNullable(v);
            return this;
        }

        public Builder len(long v) {
            this.len = OptionalLong.of(v);
            return this;
        }

        public Builder minLen(long v) {
            this.minLen = OptionalLong.of(v);
            return this;
        }

        public Builder maxLen(long v) {
            this.maxLen = OptionalLong.of(v);
            return this;
        }

        public Builder prefix(ByteString v) {
            this.prefix = Optional.ofNullable(v);
            return this;
        }

        public Builder suffix(ByteString v) {
            this.suffix = Optional.ofNullable(v);
            return this;
        }

        public Builder contains(ByteString v) {
            this.contains = Optional.ofNullable(v);
            return this;
        }

        public Builder pattern(String v) {
            this.pattern = Optional.ofNullable(v);
            return this;
        }

        public Builder in(List<ByteString> v) {
            this.in = List.copyOf(v);
            return this;
        }

        public Builder notIn(List<ByteString> v) {
            this.notIn = List.copyOf(v);
            return this;
        }

        public Builder format(BytesFormat format) {
            formats.add(Objects.requireNonNull(format, "format"));
            return this;
        }

        public BytesConstraints build() {
            return new BytesConstraints(constant, len, minLen, maxLen, prefix, suffix, contains,
                    pattern, in, notIn, formats);
        }
    }
}
