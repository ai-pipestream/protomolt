package ai.protomolt.proto.descriptors;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ApiStabilityResolverTest {

    @ApiStability(value = StabilityTier.STABLE, since = "0.1.0")
    static class StableContract {
        public static final String CONSTANT = "v1";

        @ApiStability(StabilityTier.EXPERIMENTAL)
        public String experimentalField;

        public StableContract() {
        }

        @ApiStability(StabilityTier.EXPERIMENTAL)
        public StableContract(int experimentalArg) {
        }

        public String stableOperation() {
            return CONSTANT;
        }

        @ApiStability(StabilityTier.EXPERIMENTAL)
        public String experimentalOperation() {
            return "exp";
        }

        @Deprecated
        public String deprecatedOperation() {
            return "old";
        }
    }

    @ApiStability(StabilityTier.INTERNAL)
    static class InternalImplementation {
        public void helper() {
        }
    }

    @Test
    void resolvesStabilityHierarchyAndMemberOverrides() throws Exception {
        assertThat(ApiStabilityResolver.resolve(StableContract.class))
                .isEqualTo(StabilityTier.STABLE);
        assertThat(ApiStabilityResolver.resolve(InternalImplementation.class))
                .isEqualTo(StabilityTier.INTERNAL);

        Method stableMethod = StableContract.class.getMethod("stableOperation");
        Method expMethod = StableContract.class.getMethod("experimentalOperation");
        Method depMethod = StableContract.class.getMethod("deprecatedOperation");
        assertThat(ApiStabilityResolver.resolve(stableMethod)).isEqualTo(StabilityTier.STABLE);
        assertThat(ApiStabilityResolver.resolve(expMethod)).isEqualTo(StabilityTier.EXPERIMENTAL);
        assertThat(ApiStabilityResolver.resolve(depMethod)).isEqualTo(StabilityTier.DEPRECATED);

        Constructor<?> stableCtor = StableContract.class.getConstructor();
        Constructor<?> expCtor = StableContract.class.getConstructor(int.class);
        assertThat(ApiStabilityResolver.resolve(stableCtor)).isEqualTo(StabilityTier.STABLE);
        assertThat(ApiStabilityResolver.resolve(expCtor)).isEqualTo(StabilityTier.EXPERIMENTAL);

        Field constantField = StableContract.class.getField("CONSTANT");
        Field expField = StableContract.class.getField("experimentalField");
        assertThat(ApiStabilityResolver.resolve(constantField)).isEqualTo(StabilityTier.STABLE);
        assertThat(ApiStabilityResolver.resolve(expField)).isEqualTo(StabilityTier.EXPERIMENTAL);
    }

    @Test
    void detectsInternalPackageConventionsAndTierTransitions() {
        assertThat(ApiStabilityResolver.isInternalPackageName("ai.protomolt.proto.internal"))
                .isTrue();
        assertThat(ApiStabilityResolver.isInternalPackageName("ai.protomolt.proto.internal.wire"))
                .isTrue();
        assertThat(ApiStabilityResolver.isInternalPackageName("ai.protomolt.proto.descriptors"))
                .isFalse();

        assertThat(StabilityTier.STABLE.enforcesCompatibility()).isTrue();
        assertThat(StabilityTier.DEPRECATED.enforcesCompatibility()).isTrue();
        assertThat(StabilityTier.EVOLVING.enforcesCompatibility()).isFalse();
        assertThat(StabilityTier.EXPERIMENTAL.enforcesCompatibility()).isFalse();
        assertThat(StabilityTier.INTERNAL.enforcesCompatibility()).isFalse();

        // Deprecating a STABLE surface preserves gate enforcement and is not a downgrade.
        assertThat(StabilityTier.STABLE.isDowngradeTo(StabilityTier.DEPRECATED)).isFalse();
        // Moving STABLE to EXPERIMENTAL or INTERNAL strips gate enforcement and is a downgrade.
        assertThat(StabilityTier.STABLE.isDowngradeTo(StabilityTier.EXPERIMENTAL)).isTrue();
        assertThat(StabilityTier.STABLE.isDowngradeTo(StabilityTier.INTERNAL)).isTrue();
        assertThat(StabilityTier.DEPRECATED.isDowngradeTo(StabilityTier.EXPERIMENTAL)).isTrue();

        assertThat(StabilityTier.fromLabel("stable", StabilityTier.EVOLVING))
                .isEqualTo(StabilityTier.STABLE);
        assertThat(StabilityTier.fromLabel("experimental", StabilityTier.STABLE))
                .isEqualTo(StabilityTier.EXPERIMENTAL);
        assertThat(StabilityTier.fromLabel(null, StabilityTier.EVOLVING))
                .isEqualTo(StabilityTier.EVOLVING);
        assertThat(StabilityTier.fromLabel("   ", StabilityTier.EVOLVING))
                .isEqualTo(StabilityTier.EVOLVING);
        assertThatThrownBy(() -> StabilityTier.fromLabel("unknown-label", StabilityTier.EVOLVING))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("unknown-label");

        assertThat(StabilityTier.parseExplicit(null)).isEmpty();
        assertThat(StabilityTier.parseExplicit("   ")).isEmpty();
        assertThat(StabilityTier.parseExplicit("evolving")).contains(StabilityTier.EVOLVING);
        assertThatThrownBy(() -> StabilityTier.parseExplicit("bogus"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("bogus");
    }
}
