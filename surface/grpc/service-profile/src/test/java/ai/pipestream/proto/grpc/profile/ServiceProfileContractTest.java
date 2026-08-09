package ai.pipestream.proto.grpc.profile;

import ai.pipestream.proto.grpc.profile.v1.DescriptorArtifact;
import ai.pipestream.proto.grpc.profile.v1.ServiceProfile;
import com.google.protobuf.util.JsonFormat;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ServiceProfileContractTest {

    @Test
    void profileRoundTripsAsBinaryAndJsonWithoutInlineDescriptorOrSecretMaterial() throws Exception {
        ServiceProfile profile = TestProfiles.profile("contract");
        ServiceProfile binary = ServiceProfile.parseFrom(profile.toByteArray());
        ServiceProfile.Builder jsonBuilder = ServiceProfile.newBuilder();
        JsonFormat.parser().merge(JsonFormat.printer().print(profile), jsonBuilder);
        ServiceProfile json = jsonBuilder.build();

        assertThat(binary).isEqualTo(profile);
        assertThat(json).isEqualTo(profile);
        assertThat(profile.getSchemaSource().getDescriptorFingerprint()).hasSize(64);
        assertThat(profile.getSchemaSource().getDescriptorArtifactRef()).isNotBlank();
        assertThat(profile.toString()).doesNotContain("descriptor_set");
        assertThat(ServiceProfile.getDescriptor().getFields()).extracting(
                        field -> field.getName())
                .doesNotContain("password", "secret", "private_key", "descriptor_set");
    }

    @Test
    void descriptorArtifactIsASeparateContract() {
        DescriptorArtifact artifact = TestProfiles.artifact();
        assertThat(DescriptorArtifact.getDescriptor().findFieldByName("descriptor_set"))
                .isNotNull();
        assertThat(ServiceProfile.getDescriptor().findFieldByName("descriptor_set"))
                .isNull();
        assertThat(artifact.getDescriptorSet()).isNotEmpty();
    }
}
