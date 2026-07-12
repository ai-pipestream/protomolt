package ai.pipestream.proto.validate.model;

import java.util.List;
import java.util.Objects;
import java.util.OptionalDouble;

/**
 * Neutral constraints for floating-point fields ({@code float}/{@code double}).
 * {@code ruleIdPrefix} preserves the source's category label so violation ids stay
 * stable (for example {@code float.gt}, {@code double.lte}). {@code finite} demands
 * the value is neither NaN nor infinite.
 */
public record FloatingConstraints(
        String ruleIdPrefix,
        OptionalDouble constant,
        OptionalDouble gt,
        OptionalDouble gte,
        OptionalDouble lt,
        OptionalDouble lte,
        List<Double> in,
        List<Double> notIn,
        boolean finite) {

    public FloatingConstraints {
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
                && lte.isEmpty() && in.isEmpty() && notIn.isEmpty() && !finite;
    }

    public static Builder builder(String ruleIdPrefix) {
        return new Builder(ruleIdPrefix);
    }

    public static final class Builder {
        private final String ruleIdPrefix;
        private OptionalDouble constant = OptionalDouble.empty();
        private OptionalDouble gt = OptionalDouble.empty();
        private OptionalDouble gte = OptionalDouble.empty();
        private OptionalDouble lt = OptionalDouble.empty();
        private OptionalDouble lte = OptionalDouble.empty();
        private List<Double> in = List.of();
        private List<Double> notIn = List.of();
        private boolean finite;

        private Builder(String ruleIdPrefix) {
            this.ruleIdPrefix = Objects.requireNonNull(ruleIdPrefix, "ruleIdPrefix");
        }

        public Builder constant(double constant) {
            this.constant = OptionalDouble.of(constant);
            return this;
        }

        public Builder gt(double gt) {
            this.gt = OptionalDouble.of(gt);
            return this;
        }

        public Builder gte(double gte) {
            this.gte = OptionalDouble.of(gte);
            return this;
        }

        public Builder lt(double lt) {
            this.lt = OptionalDouble.of(lt);
            return this;
        }

        public Builder lte(double lte) {
            this.lte = OptionalDouble.of(lte);
            return this;
        }

        public Builder in(List<Double> in) {
            this.in = List.copyOf(in);
            return this;
        }

        public Builder notIn(List<Double> notIn) {
            this.notIn = List.copyOf(notIn);
            return this;
        }

        public Builder finite(boolean finite) {
            this.finite = finite;
            return this;
        }

        public FloatingConstraints build() {
            return new FloatingConstraints(ruleIdPrefix, constant, gt, gte, lt, lte, in, notIn, finite);
        }
    }
}
