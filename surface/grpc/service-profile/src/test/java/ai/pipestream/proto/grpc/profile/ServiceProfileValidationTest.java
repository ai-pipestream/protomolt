package ai.pipestream.proto.grpc.profile;

import ai.pipestream.proto.grpc.profile.v1.DescriptorArtifact;
import ai.pipestream.proto.grpc.profile.v1.HealthProbe;
import ai.pipestream.proto.grpc.profile.v1.MethodPolicy;
import ai.pipestream.proto.grpc.profile.v1.Operation;
import ai.pipestream.proto.grpc.profile.v1.SchemaSource;
import ai.pipestream.proto.grpc.profile.v1.ServiceEndpoint;
import ai.pipestream.proto.grpc.profile.v1.ServiceProfile;
import ai.pipestream.proto.grpc.profile.v1.Transport;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ServiceProfileValidationTest {

    @Test
    void acceptsExplicitTransportSchemaAndOperationalPolicy() {
        ServiceProfileValidation.validate(TestProfiles.profile("example-service"));
        ServiceProfileValidation.validate(TestProfiles.artifact());
    }

    @Test
    void acceptsConnectionProfileBeforeReflectionSuppliesSchema() {
        ServiceProfile draft = TestProfiles.profile("draft").toBuilder().clearSchemaSource().build();

        ServiceProfileValidation.validateConnectionProfile(draft);
        assertThatThrownBy(() -> ServiceProfileValidation.validate(draft))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("schema_source.kind");
    }

    @Test
    void rejectsPathTraversalAndUnsafeNames() {
        assertThatThrownBy(() -> ServiceProfileValidation.validate(TestProfiles.profile("../escape")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("path-safe");
        assertThatThrownBy(() -> ServiceProfileValidation.validateName("nested/name", "name"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> ServiceProfileValidation.validateName("..", "name"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsMissingEndpointAndAmbiguousTransport() {
        ServiceProfile noEndpoints = TestProfiles.profile("empty").toBuilder().clearEndpoints().build();
        assertThatThrownBy(() -> ServiceProfileValidation.validate(noEndpoints))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("endpoints");

        ServiceProfile unspecifiedTransport = TestProfiles.profile("transport").toBuilder()
                .setEndpoints(0, ServiceEndpoint.newBuilder()
                        .setName("local").setHost("localhost").setPort(9090).build())
                .build();
        assertThatThrownBy(() -> ServiceProfileValidation.validate(unspecifiedTransport))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("transport");

        ServiceProfile unknownTransport = TestProfiles.profile("unknown-transport").toBuilder()
                .setEndpoints(0, TestProfiles.profile("other").getEndpoints(0).toBuilder()
                        .setTransportValue(99).build())
                .build();
        assertThatThrownBy(() -> ServiceProfileValidation.validate(unknownTransport))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("plaintext or TLS");
    }

    @Test
    void rejectsDuplicateEndpointsMethodsAndInvalidReferences() {
        ServiceProfile duplicateEndpoints = TestProfiles.profile("duplicates").toBuilder()
                .addEndpoints(TestProfiles.profile("other").getEndpoints(0)).build();
        assertThatThrownBy(() -> ServiceProfileValidation.validate(duplicateEndpoints))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicate endpoint");

        MethodPolicy duplicate = TestProfiles.profile("methods").getMethodPolicies(0);
        ServiceProfile duplicateMethods = TestProfiles.profile("methods").toBuilder()
                .addMethodPolicies(duplicate).build();
        assertThatThrownBy(() -> ServiceProfileValidation.validate(duplicateMethods))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("duplicate method");

        ServiceProfile whitespaceRef = TestProfiles.profile("refs").toBuilder()
                .setEndpoints(0, TestProfiles.profile("other").getEndpoints(0).toBuilder()
                        .setCredentialRef("secret value").build()).build();
        assertThatThrownBy(() -> ServiceProfileValidation.validate(whitespaceRef))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("namespaced opaque reference");

        ServiceProfile rawSecret = TestProfiles.profile("raw-secret").toBuilder()
                .setEndpoints(0, TestProfiles.profile("other").getEndpoints(0).toBuilder()
                        .setCredentialRef("sk_live_not_a_reference").build()).build();
        assertThatThrownBy(() -> ServiceProfileValidation.validate(rawSecret))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("namespaced opaque reference");
    }

    @Test
    void rejectsResolverUrisBeforeTheyCanBecomeGrpcTargets() {
        ServiceProfile resolver = TestProfiles.profile("resolver").toBuilder()
                .setEndpoints(0, TestProfiles.profile("other").getEndpoints(0).toBuilder()
                        .setHost("dns:///other-host").build()).build();

        assertThatThrownBy(() -> ServiceProfileValidation.validateConnectionProfile(resolver))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("not a URI target");
    }

    @Test
    void rejectsInvalidSchemaFingerprintAndArtifactContent() {
        ServiceProfile badFingerprint = TestProfiles.profile("schema").toBuilder()
                .setSchemaSource(SchemaSource.newBuilder()
                        .setKind(ai.pipestream.proto.grpc.profile.v1.SourceKind.SOURCE_KIND_ARTIFACT)
                        .setDescriptorFingerprint("not-a-fingerprint")
                        .setDescriptorArtifactRef("not-a-fingerprint")
                        .build()).build();
        assertThatThrownBy(() -> ServiceProfileValidation.validate(badFingerprint))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("fingerprint");

        DescriptorArtifact wrongHash = TestProfiles.artifact().toBuilder()
                .setFingerprint("ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff")
                .build();
        assertThatThrownBy(() -> ServiceProfileValidation.validate(wrongHash))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not match");

        DescriptorArtifact notDescriptorSet = DescriptorArtifact.newBuilder()
                .setFingerprint(ServiceProfileValidation.sha256(new byte[] {1, 2, 3}))
                .setDescriptorSet(ByteString.copyFrom(new byte[] {1, 2, 3}))
                .build();
        assertThatThrownBy(() -> ServiceProfileValidation.validate(notDescriptorSet))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("FileDescriptorSet");

        DescriptorArtifact oversized = DescriptorArtifact.newBuilder()
                .setFingerprint("0000000000000000000000000000000000000000000000000000000000000000")
                .setDescriptorSet(ByteString.copyFrom(
                        new byte[ServiceProfileValidation.MAX_DESCRIPTOR_ARTIFACT_BYTES + 1]))
                .build();
        assertThatThrownBy(() -> ServiceProfileValidation.validate(oversized))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maximum size");

        ServiceProfile unknownSource = TestProfiles.profile("unknown-source").toBuilder()
                .setSchemaSource(TestProfiles.profile("other").getSchemaSource().toBuilder()
                        .setKindValue(99).build()).build();
        assertThatThrownBy(() -> ServiceProfileValidation.validate(unknownSource))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("kind must be recognized");
    }

    @Test
    void rejectsEnabledHealthProbeWithoutMethodOrTimeout() {
        ServiceProfile missingMethod = TestProfiles.profile("health").toBuilder()
                .setHealthProbe(HealthProbe.newBuilder().setEnabled(true)
                        .setTimeout(Duration.newBuilder().setSeconds(1).build()).build())
                .build();
        assertThatThrownBy(() -> ServiceProfileValidation.validate(missingMethod))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("health_probe.method");

        ServiceProfile missingTimeout = TestProfiles.profile("health-timeout").toBuilder()
                .setHealthProbe(HealthProbe.newBuilder()
                        .setEnabled(true).setMethod("grpc.health.v1.Health/Check").build())
                .build();
        assertThatThrownBy(() -> ServiceProfileValidation.validate(missingTimeout))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("timeout");
    }

    @Test
    void rejectsUnspecifiedMethodOperation() {
        ServiceProfile profile = TestProfiles.profile("operation").toBuilder()
                .setMethodPolicies(0, MethodPolicy.newBuilder()
                        .setMethod("example.v1.ExampleService/Get")
                        .addOperation(Operation.OPERATION_UNSPECIFIED).build())
                .build();
        assertThatThrownBy(() -> ServiceProfileValidation.validate(profile))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("operation must be recognized");

        ServiceProfile unknown = TestProfiles.profile("unknown-operation").toBuilder()
                .setMethodPolicies(0, MethodPolicy.newBuilder()
                        .setMethod("example.v1.ExampleService/Get")
                        .addOperationValue(99).build())
                .build();
        assertThatThrownBy(() -> ServiceProfileValidation.validate(unknown))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("operation must be recognized");
    }
}
