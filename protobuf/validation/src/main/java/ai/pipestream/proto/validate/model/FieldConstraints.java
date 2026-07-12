package ai.pipestream.proto.validate.model;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Neutral, source-agnostic constraints for a single field. A
 * {@link ai.pipestream.proto.validate.spi.ValidationRuleSource} translates its own
 * annotation dialect (Pipestream {@code validate.v1}, {@code buf.validate}, …) into
 * this model; the validator core evaluates only this model and never reads a specific
 * option dialect directly.
 *
 * <p>The field's type selects which sub-constraints apply — a string field only
 * consults {@link #string()}, a map field only {@link #map()}, and so on; the rest
 * are ignored.
 */
public record FieldConstraints(
        boolean required,
        IgnoreMode ignore,
        Optional<StringConstraints> string,
        Optional<IntegralConstraints> integral,
        Optional<FloatingConstraints> floating,
        Optional<BoolConstraints> bool,
        Optional<BytesConstraints> bytes,
        Optional<EnumConstraints> enumeration,
        Optional<RepeatedConstraints> repeated,
        Optional<MapConstraints> map,
        Optional<TimestampConstraints> timestamp,
        Optional<DurationConstraints> duration,
        List<CelConstraint> cel) {

    public FieldConstraints {
        Objects.requireNonNull(ignore, "ignore");
        Objects.requireNonNull(string, "string");
        Objects.requireNonNull(integral, "integral");
        Objects.requireNonNull(floating, "floating");
        Objects.requireNonNull(bool, "bool");
        Objects.requireNonNull(bytes, "bytes");
        Objects.requireNonNull(enumeration, "enumeration");
        Objects.requireNonNull(repeated, "repeated");
        Objects.requireNonNull(map, "map");
        Objects.requireNonNull(timestamp, "timestamp");
        Objects.requireNonNull(duration, "duration");
        cel = List.copyOf(Objects.requireNonNull(cel, "cel"));
    }

    public static Builder builder() {
        return new Builder();
    }

    /** Mutable builder; empty sub-constraints collapse to {@link Optional#empty()}. */
    public static final class Builder {
        private boolean required;
        private IgnoreMode ignore = IgnoreMode.UNSPECIFIED;
        private StringConstraints string;
        private IntegralConstraints integral;
        private FloatingConstraints floating;
        private BoolConstraints bool;
        private BytesConstraints bytes;
        private EnumConstraints enumeration;
        private RepeatedConstraints repeated;
        private MapConstraints map;
        private TimestampConstraints timestamp;
        private DurationConstraints duration;
        private final List<CelConstraint> cel = new ArrayList<>();

        public Builder required(boolean required) {
            this.required = required;
            return this;
        }

        public Builder ignore(IgnoreMode ignore) {
            this.ignore = Objects.requireNonNull(ignore, "ignore");
            return this;
        }

        public Builder string(StringConstraints string) {
            this.string = (string == null || string.isEmpty()) ? null : string;
            return this;
        }

        public Builder integral(IntegralConstraints integral) {
            this.integral = (integral == null || integral.isEmpty()) ? null : integral;
            return this;
        }

        public Builder floating(FloatingConstraints floating) {
            this.floating = (floating == null || floating.isEmpty()) ? null : floating;
            return this;
        }

        public Builder bool(BoolConstraints bool) {
            this.bool = (bool == null || bool.isEmpty()) ? null : bool;
            return this;
        }

        public Builder bytes(BytesConstraints bytes) {
            this.bytes = (bytes == null || bytes.isEmpty()) ? null : bytes;
            return this;
        }

        public Builder enumeration(EnumConstraints enumeration) {
            this.enumeration = (enumeration == null || enumeration.isEmpty()) ? null : enumeration;
            return this;
        }

        public Builder repeated(RepeatedConstraints repeated) {
            this.repeated = (repeated == null || repeated.isEmpty()) ? null : repeated;
            return this;
        }

        public Builder map(MapConstraints map) {
            this.map = (map == null || map.isEmpty()) ? null : map;
            return this;
        }

        public Builder timestamp(TimestampConstraints timestamp) {
            this.timestamp = (timestamp == null || timestamp.isEmpty()) ? null : timestamp;
            return this;
        }

        public Builder duration(DurationConstraints duration) {
            this.duration = (duration == null || duration.isEmpty()) ? null : duration;
            return this;
        }

        public Builder addCel(CelConstraint constraint) {
            cel.add(Objects.requireNonNull(constraint, "constraint"));
            return this;
        }

        public FieldConstraints build() {
            return new FieldConstraints(
                    required,
                    ignore,
                    Optional.ofNullable(string),
                    Optional.ofNullable(integral),
                    Optional.ofNullable(floating),
                    Optional.ofNullable(bool),
                    Optional.ofNullable(bytes),
                    Optional.ofNullable(enumeration),
                    Optional.ofNullable(repeated),
                    Optional.ofNullable(map),
                    Optional.ofNullable(timestamp),
                    Optional.ofNullable(duration),
                    List.copyOf(cel));
        }
    }
}
