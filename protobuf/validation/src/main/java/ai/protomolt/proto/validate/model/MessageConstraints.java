package ai.protomolt.proto.validate.model;

import java.util.List;
import java.util.Objects;

/**
 * Neutral, source-agnostic message-level constraints: CEL predicates, synthetic message oneof
 * rules, the names of real protobuf oneofs marked {@code required}, and an optional
 * {@code skipWhen} escape channel.
 */
public record MessageConstraints(
        List<CelConstraint> cel, List<Oneof> oneofs, List<String> requiredOneofs, String skipWhen) {

    public MessageConstraints {
        cel = List.copyOf(Objects.requireNonNull(cel, "cel"));
        oneofs = List.copyOf(Objects.requireNonNull(oneofs, "oneofs"));
        requiredOneofs = List.copyOf(Objects.requireNonNull(requiredOneofs, "requiredOneofs"));
        skipWhen = Objects.requireNonNull(skipWhen, "skipWhen");
    }

    /** Backwards-compatible constructor for sources without a skip-when escape channel. */
    public MessageConstraints(List<CelConstraint> cel, List<Oneof> oneofs, List<String> requiredOneofs) {
        this(cel, oneofs, requiredOneofs, "");
    }

    public MessageConstraints(List<CelConstraint> cel, List<Oneof> oneofs) {
        this(cel, oneofs, List.of());
    }

    /** Backwards-compatible constructor for sources without message oneof support. */
    public MessageConstraints(List<CelConstraint> cel) {
        this(cel, List.of(), List.of());
    }

    public boolean isEmpty() {
        return cel.isEmpty() && oneofs.isEmpty() && requiredOneofs.isEmpty() && skipWhen.isEmpty();
    }

    /**
     * A message-level {@code oneof} rule: at most one of {@link #fields} may be populated, and when
     * {@link #required} at least one must be. Field-level rules on an unpopulated member are skipped.
     */
    public record Oneof(List<String> fields, boolean required) {
        public Oneof {
            fields = List.copyOf(Objects.requireNonNull(fields, "fields"));
        }
    }
}
