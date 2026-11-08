package ai.protomolt.proto.emit.renderer;

import ai.protomolt.proto.descriptors.DescriptorFingerprints;
import ai.protomolt.proto.emit.v1.RendererCapability;
import ai.protomolt.proto.validate.ProtoValidator;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class RendererContractTest {
    @Test void pinsRendererClosureIndependentlyOfRepositoryContainer() {
        assertThat(DescriptorFingerprints.fingerprintOf(RendererCapability.getDescriptor()))
                .isEqualTo("19c5525b9ccef5e2d9ae00ba7d366ca8c65116784fbb1fd7b0a0a5bf8aab705c");
    }
    @Test void capabilityConstraintsRejectMissingIdentityAndZeroLimits() {
        var validator = ProtoValidator.forMessageType(RendererCapability.getDescriptor());
        assertThat(validator.validate(RendererCapability.getDefaultInstance()).valid()).isFalse();
        assertThat(validator.validate(new RendererRegistryTest.Echo().capability().toBuilder().setMaxOutputBytes(0).build()).valid()).isFalse();
        assertThat(validator.validate(new RendererRegistryTest.Echo().capability()).valid()).isTrue();
    }
}
