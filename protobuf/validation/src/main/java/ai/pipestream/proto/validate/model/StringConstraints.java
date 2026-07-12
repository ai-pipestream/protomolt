package ai.pipestream.proto.validate.model;

import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

/**
 * Neutral constraints for string fields. Violation rule ids are the fixed
 * {@code string.*} ids ({@code string.min_len}, {@code string.const},
 * {@code string.email}, …). Length rules count Unicode code points.
 */
public record StringConstraints(
        Optional<String> constant,
        OptionalLong len,
        OptionalLong minLen,
        OptionalLong maxLen,
        Optional<String> pattern,
        Optional<String> prefix,
        Optional<String> suffix,
        Optional<String> contains,
        Optional<String> notContains,
        List<String> in,
        List<String> notIn,
        Set<StringFormat> formats,
        Optional<HttpHeaderRule> httpHeader) {

    public StringConstraints {
        Objects.requireNonNull(constant, "constant");
        Objects.requireNonNull(len, "len");
        Objects.requireNonNull(minLen, "minLen");
        Objects.requireNonNull(maxLen, "maxLen");
        Objects.requireNonNull(pattern, "pattern");
        Objects.requireNonNull(prefix, "prefix");
        Objects.requireNonNull(suffix, "suffix");
        Objects.requireNonNull(contains, "contains");
        Objects.requireNonNull(notContains, "notContains");
        in = List.copyOf(Objects.requireNonNull(in, "in"));
        notIn = List.copyOf(Objects.requireNonNull(notIn, "notIn"));
        Objects.requireNonNull(formats, "formats");
        Objects.requireNonNull(httpHeader, "httpHeader");
        formats = formats.isEmpty() ? Set.of() : Set.copyOf(EnumSet.copyOf(formats));
    }

    /** True when no string constraint is actually set (safe to skip). */
    public boolean isEmpty() {
        return constant.isEmpty() && len.isEmpty() && minLen.isEmpty() && maxLen.isEmpty()
                && pattern.isEmpty() && prefix.isEmpty() && suffix.isEmpty()
                && contains.isEmpty() && notContains.isEmpty()
                && in.isEmpty() && notIn.isEmpty() && formats.isEmpty() && httpHeader.isEmpty();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private Optional<String> constant = Optional.empty();
        private OptionalLong len = OptionalLong.empty();
        private OptionalLong minLen = OptionalLong.empty();
        private OptionalLong maxLen = OptionalLong.empty();
        private Optional<String> pattern = Optional.empty();
        private Optional<String> prefix = Optional.empty();
        private Optional<String> suffix = Optional.empty();
        private Optional<String> contains = Optional.empty();
        private Optional<String> notContains = Optional.empty();
        private List<String> in = List.of();
        private List<String> notIn = List.of();
        private final EnumSet<StringFormat> formats = EnumSet.noneOf(StringFormat.class);
        private Optional<HttpHeaderRule> httpHeader = Optional.empty();

        public Builder constant(String constant) {
            this.constant = Optional.ofNullable(constant);
            return this;
        }

        public Builder len(long len) {
            this.len = OptionalLong.of(len);
            return this;
        }

        public Builder minLen(long minLen) {
            this.minLen = OptionalLong.of(minLen);
            return this;
        }

        public Builder maxLen(long maxLen) {
            this.maxLen = OptionalLong.of(maxLen);
            return this;
        }

        public Builder pattern(String pattern) {
            this.pattern = Optional.ofNullable(pattern);
            return this;
        }

        public Builder prefix(String prefix) {
            this.prefix = Optional.ofNullable(prefix);
            return this;
        }

        public Builder suffix(String suffix) {
            this.suffix = Optional.ofNullable(suffix);
            return this;
        }

        public Builder contains(String contains) {
            this.contains = Optional.ofNullable(contains);
            return this;
        }

        public Builder notContains(String notContains) {
            this.notContains = Optional.ofNullable(notContains);
            return this;
        }

        public Builder in(List<String> in) {
            this.in = List.copyOf(in);
            return this;
        }

        public Builder notIn(List<String> notIn) {
            this.notIn = List.copyOf(notIn);
            return this;
        }

        public Builder format(StringFormat format) {
            formats.add(Objects.requireNonNull(format, "format"));
            return this;
        }

        public Builder httpHeader(HttpHeaderRule httpHeader) {
            this.httpHeader = Optional.ofNullable(httpHeader);
            return this;
        }

        public StringConstraints build() {
            return new StringConstraints(constant, len, minLen, maxLen, pattern,
                    prefix, suffix, contains, notContains, in, notIn, formats, httpHeader);
        }
    }
}
