package ai.pipestream.proto.validate.model;

import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;

/**
 * Neutral bound constraints for integral fields (all int/uint/sint/fixed variants).
 * {@code ruleIdPrefix} preserves the source's category label so violation ids stay
 * stable (for example {@code int32.gt}, {@code uint64.lte}). When {@code unsigned}
 * is true all comparisons treat the raw 64-bit value as unsigned.
 */
public record IntegralConstraints(
        String ruleIdPrefix,
        boolean unsigned,
        OptionalLong constant,
        OptionalLong gt,
        OptionalLong gte,
        OptionalLong lt,
        OptionalLong lte,
        List<Long> in,
        List<Long> notIn) {

    public IntegralConstraints {
        Objects.requireNonNull(ruleIdPrefix, "ruleIdPrefix");
        Objects.requireNonNull(constant, "constant");
        Objects.requireNonNull(gt, "gt");
        Objects.requireNonNull(gte, "gte");
        Objects.requireNonNull(lt, "lt");
        Objects.requireNonNull(lte, "lte");
        in = List.copyOf(Objects.requireNonNull(in, "in"));
        notIn = List.copyOf(Objects.requireNonNull(notIn, "notIn"));
    }

    public boolean isEmpty() {
        return constant.isEmpty() && gt.isEmpty() && gte.isEmpty() && lt.isEmpty()
                && lte.isEmpty() && in.isEmpty() && notIn.isEmpty();
    }

    /** Builder for signed rules with the given rule-id prefix (e.g. {@code "int32"}). */
    public static Builder builder(String ruleIdPrefix) {
        return new Builder(ruleIdPrefix, false);
    }

    /** Builder for unsigned rules with the given rule-id prefix (e.g. {@code "uint64"}). */
    public static Builder unsignedBuilder(String ruleIdPrefix) {
        return new Builder(ruleIdPrefix, true);
    }

    public static final class Builder {
        private final String ruleIdPrefix;
        private final boolean unsigned;
        private OptionalLong constant = OptionalLong.empty();
        private OptionalLong gt = OptionalLong.empty();
        private OptionalLong gte = OptionalLong.empty();
        private OptionalLong lt = OptionalLong.empty();
        private OptionalLong lte = OptionalLong.empty();
        private List<Long> in = List.of();
        private List<Long> notIn = List.of();

        private Builder(String ruleIdPrefix, boolean unsigned) {
            this.ruleIdPrefix = Objects.requireNonNull(ruleIdPrefix, "ruleIdPrefix");
            this.unsigned = unsigned;
        }

        public Builder constant(long constant) {
            this.constant = OptionalLong.of(constant);
            return this;
        }

        public Builder gt(long gt) {
            this.gt = OptionalLong.of(gt);
            return this;
        }

        public Builder gte(long gte) {
            this.gte = OptionalLong.of(gte);
            return this;
        }

        public Builder lt(long lt) {
            this.lt = OptionalLong.of(lt);
            return this;
        }

        public Builder lte(long lte) {
            this.lte = OptionalLong.of(lte);
            return this;
        }

        public Builder in(List<Long> in) {
            this.in = List.copyOf(in);
            return this;
        }

        public Builder notIn(List<Long> notIn) {
            this.notIn = List.copyOf(notIn);
            return this;
        }

        public IntegralConstraints build() {
            return new IntegralConstraints(ruleIdPrefix, unsigned, constant, gt, gte, lt, lte, in, notIn);
        }
    }
}
